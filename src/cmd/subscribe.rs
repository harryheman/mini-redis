use crate::cmd::{Parse, ParseError, Unknown};
use crate::{Command, Connection, Db, Frame, Shutdown};

use bytes::Bytes;
use std::pin::Pin;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::{Stream, StreamExt, StreamMap};

/// Подписывает клиента на один или несколько каналов.
///
/// После подписки клиент может выполнять только команды
/// SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PING и QUIT
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

/// Отписывает клиента от одного или нескольких каналов.
///
/// Если каналы не указаны, клиент отписывается от всех каналов,
/// на которые он подписан
#[derive(Clone, Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

/// Поток сообщений. Поток получает сообщения из
/// `broadcast::Receiver`. Мы используем `stream!` для создания `Stream`,
/// потребляющего сообщения. Поскольку значения `stream!` не могут быть именованы, мы оборачиваем поток
/// в трейт-объект
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    /// Создает новую команду `Subscribe` для прослушивания определенных каналов
    pub(crate) fn new(channels: Vec<String>) -> Subscribe {
        Subscribe { channels }
    }

    /// Разбирает экземпляр `Subscribe` из полученного кадра.
    ///
    /// Аргумент `Parse` предоставляет подобное курсору (cursor-like) API для чтения полей из
    /// `Frame`. На этом этапе из сокета получен весь кадр.
    ///
    /// Строка `SUBSCRIBE` уже потреблена.
    ///
    /// # Возвращаемые значения
    ///
    /// При успехе возвращается значение `Subscribe`. Если кадр испорчен,
    /// возвращается `Err`.
    ///
    /// # Формат
    ///
    /// Ожидается массив кадров, содержащий минимум 2 сущности:
    ///
    /// ```text
    /// SUBSCRIBE channel [channel ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        // В `parse` остались одна или несколько строк,
        // представляющие каналы для подписки.
        //
        // Извлекаем первую строку. Если строка отсутствует,
        // кадр испорчен, возвращается ошибка
        let mut channels = vec![parse.next_string()?];

        // Потребляется остаток кадра. Каждое значение должно быть
        // строкой или кадр считается испорченным. После потребления всех
        // значений кадра, команда считается полностью разобранной
        loop {
            match parse.next_string() {
                // Помещаем извлеченную из `parse` строку в
                // список каналов для подписки
                Ok(s) => channels.push(s),
                // Ошибка `EndOfStream` означает отсутствие данных для разбора
                Err(EndOfStream) => break,
                // Другие ошибки передаются вызывающей стороне, что приводит к закрытию соединения
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /// Применяет команду `Subscribe` к определенному экземпляру `Db`.
    ///
    /// Эта функция является входной точкой и содержит начальный список
    /// каналов для подписки. Дополнительные команды `subscribe` и `unsubscribe`
    /// могут быть получены от клиента, и список подписок обновляется соответствующим образом.
    ///
    /// См. https://redis.io/topics/pubsub
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        // Подписка на конкретный канал `sync::broadcast`. Сообщения передаются
        // всем клиентам, подписанным на канал.
        //
        // Один клиент может подписаться на несколько каналов и
        // динамически добавлять и удалять каналы из списка подписок.
        // `StreamMap` используется для отслеживания активных подписок.
        // `StreamMap` объединяет сообщения из отдельных широковещательных каналов
        // по мере их поступления.
        let mut subscriptions = StreamMap::new();

        loop {
            // `self.channels` используется для отслеживания дополнительных каналов для подписки.
            // При получении новых команд `SUBSCRIBE` в процессе
            // выполнения `apply`, новые каналы помещаются в этот `vec`
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            // Ждем наступления одного из следующих событий:
            //
            // - получение сообщения из одного из подписанных каналов
            // - получение команды подписки или отписки от клиента
            // - получение сигнала о закрытии
            select! {
                // Получаем сообщения из подписанного канала
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        // Клиент отключился
                        None => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };
        }
    }

    /// Преобразует команду в соответствующий `Frame`.
    ///
    /// Это вызывается клиентом при кодировке команды `Subscribe`
    /// для отправки на сервер
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    // Подписываемся на канал
    let rx = Box::pin(async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(msg) => yield msg,
                // Если мы зависли (lagged) при потреблении сообщений, просто продолжаем
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });

    // Помещаем подписку в список подписок клиента для отслеживания
    subscriptions.insert(channel_name.clone(), rx);

    // Отвечаем успешной подпиской
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;

    Ok(())
}

/// Обрабатывает команду, полученную во время выполнения `Subscribe::apply`.
/// В этом контексте разрешены только команды подписки и отписки.
///
/// Любые новые подписки добавляются в `subscribe_to` вместо модификации
/// `subscriptions`
async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, Messages>,
    dst: &mut Connection,
) -> crate::Result<()> {
    // От клиента была получена команда.
    //
    // В этом контексте разрешены только команды `SUBSCRIBE` и `UNSUBSCRIBE`
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            // Метод `apply` выполнит подписку на каналы,
            // добавленные в этот вектор
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            // Если каналы не указаны, выполняется отписка от всех каналов.
            // Для этого вектор `unsubscribe.channels` заполняется каналами,
            // на которые подписан клиент
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }

            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);

                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

/// Создает ответ на запрос подписки.
///
/// Все эти функции принимают `channel_name` как `String`, а не
/// `&str`, поскольку `Bytes::from` может повторно использовать выделение (allocation) в `String`, а принятие
/// `&str` потребует копирования данных. Это позволяет вызывающей стороне решать,
/// клонировать название канала или нет
fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Создает ответ на запрос отписки
fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

/// Создает сообщение, информирующее клиента о новом сообщении в канале,
/// на который он подписан
fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    /// Создает новую команду `Unsubscribe` с указанными `channels`.
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    /// Разбирает экземпляр `Unsubscribe` из полученного кадра.
    ///
    /// Аргумент `Parse` предоставляет подобное курсору (cursor-like) API для чтения полей из
    /// `Frame`. На этом этапе из сокета получен весь кадр.
    ///
    /// Строка `UNSUBSCRIBE` уже потреблена.
    ///
    /// # Возвращаемые значения
    ///
    /// При успехе возвращается значение `Unsubscribe`. Если кадр испорчен,
    /// возвращается `Err`.
    ///
    /// # Формат
    ///
    /// Ожидается массив кадров, содержащий минимум 1 сущность:
    ///
    /// ```text
    /// UNSUBSCRIBE [channel [channel ...]]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        // Каналы могут отсутствовать, так что начинаем с пустого вектора
        let mut channels = vec![];

        // Каждая сущность кадра должна быть строкой, иначе
        // кадр считается испорченным. После потребления всех значений
        // кадра, команда считается полностью разобранной
        loop {
            match parse.next_string() {
                // Помещаем извлеченную из `parse()` строку в
                // список каналов для отписки
                Ok(s) => channels.push(s),
                // Ошибка `EndOfStream` означает, что данных для разбора больше нет
                Err(EndOfStream) => break,
                // Другие ошибки передаются вызывающей стороне, что приводит к
                // закрытию соединения
                Err(err) => return Err(err),
            }
        }

        Ok(Unsubscribe { channels })
    }

    /// Преобразует команду в соответствующий `Frame`.
    ///
    /// Это вызывается клиентом при кодировке команды `Unsubscribe`
    /// для отправки на сервер
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));

        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }

        frame
    }
}
