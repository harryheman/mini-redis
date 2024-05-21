//! Минимальная реализация клиента `Redis`.
//!
//! Предоставляет асинхронное подключение и методы для обработки поддерживаемых команд.

use crate::cmd::{Get, Ping, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

/// Соединение, установленное с сервером `Redis`.
///
/// Поддерживаемый одним `TcpStream`, `Client` предоставляет базовую функциональность
/// сетевого клиента (нет длинного опроса (polling), повторов и др.). Соединения устанавливаются
/// с помощью функции `connect`.
///
/// Запросы обрабатываются с помощью разных методов `Client`.
pub struct Client {
    /// Соединение TCP, декорированное кодировщиком/декодером протокола `Redis`,
    /// реализованного с помощью буферного `TcpStream`.
    ///
    /// Когда `Listener` получает входящее соединение, `TcpStream`
    /// передается в `Connection::new()`, инициализирующий соответствующие буферы.
    /// `Connection` позволяет обработчику оперировать на уровне "кадра",
    /// инкапсулируя детали разбора протокола на уровне байтов.
    connection: Connection,
}

/// Клиент в режиме pub/sub (издатель/подписчик).
///
/// После подписки на канал, клиенты могут выполнять только команды, связанные с pub/sub.
/// Тип `Client` становится типом `Subscriber` для предотвращения вызова команд,
/// не связанных с pub/sub.
pub struct Subscriber {
    /// Подписанный клиент.
    client: Client,

    /// Набор каналов, на которые подписан `Subscriber`.
    subscribed_channels: Vec<String>,
}

/// Сообщение, полученное в подписанном канале.
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// Устанавливает соединение с сервером `Redis`, находящимся по `addr`.
    ///
    /// `addr` - любой тип, который может быть асинхронно преобразован в
    /// `SocketAddr`. Это включает `SocketAddr` и строки. Трейт `ToSocketAddrs`
    /// предоставляется `Tokio`, а `std`.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = match Client::connect("localhost:6379").await {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("Невозможно установить соединение!"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    ///
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        // Аргумент `addr` передается прямо в `TcpStream::connect()`. Выполняется
        // асинхронный поиск DNS и попытка установить соединение TCP.
        // Ошибка, возникшая на этом этапе, поднимается (bubble up) к вызывающей стороне.
        let socket = TcpStream::connect(addr).await?;

        // Инициализируем состояние подключения. Это выделяет буферы чтения/записи для
        // разбора кадра протокола `Redis`.
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    /// "Пингует" сервер.
    ///
    /// При отсутствии аргументов, возвращается "PONG",
    /// иначе, возвращается копия аргументов в виде группы (bulk).
    ///
    /// Эта команда часто используется для тестирования того, что
    /// соединение открыто, а также для измерения задержки.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        self.connection.write_frame(&frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// Извлекает значение по ключу.
    ///
    /// При отсутствии значения, возвращается `None`.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("{:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // Создаем команду `Get` для `key` и преобразуем ее в кадр.
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        // Это записывает полный кадр в
        // сокет, ожидая при необходимости
        self.connection.write_frame(&frame).await?;

        // Ждем ответа сервера.
        //
        // Принимаются кадры `Simple` и `Bulk`. `Null` представляет
        // отсутствующий ключ - возвращается `None`
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// Устанавливает переданное `value` для `key`.
    ///
    /// `value` ассоциируется с `key`, пока не будет перезаписано следующим
    /// вызовом `set` или не будет удалено.
    ///
    /// Предыдущее значение перезаписывается (при наличии). Предыдущее время жизни
    /// ключа отбрасывается (discard) при успехе операции `SET`.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set("foo", "bar".into()).await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        // Создаем команду `Set` и передаем ее в `set_cmd()`. Для установки значения
        // с временем жизни (expiration) используется отдельный метод. Общая часть обеих
        // функций реализуется `set_cmd`.
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// Устанавливает переданное `value` для `key`. Значение истекает после `expiration`.
    ///
    /// `value` ассоциируется с `key`, пока оно не:
    /// - истечет
    /// - будет перезаписано следующим вызовом `set`
    /// - будет удалено
    ///
    /// Предыдущее значение перезаписывается (при наличии). Предыдущее время жизни
    /// ключа отбрасывается (discard) при успехе операции `SET`.
    ///
    /// # Примеры
    ///
    /// Пример может работать не всегда, поскольку он полагается на
    /// относительную синхронизацию клиента и сервера по времени.
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    /// use tokio::time;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set_expires("foo", "bar".into(), ttl).await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // Ждем окончания времени жизни
    ///     time::sleep(ttl).await;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    /// Основная логика `SET`, используемая методами `set` и `set_expires.
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // Преобразуем команду `Set` в кадр
        let frame = cmd.into_frame();

        debug!(request = ?frame);

        // Это записывает полный кадр в
        // сокет, ожидая при необходимости
        self.connection.write_frame(&frame).await?;

        // Ждем ответа сервера. При успехе сервер отвечает
        // простым `OK`. Любой другой ответ означает ошибку
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// Отправляет  `message` в определенный `channel`.
    ///
    /// Возвращает количество подписчиков канала.
    /// Не гарантируется, что все эти подписчики получат сообщение, поскольку
    /// они могут отключиться в любой момент.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("Получено = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // Преобразуем команду `Publish` в кадр
        let frame = Publish::new(channel, message).into_frame();

        debug!(request = ?frame);

        // Записываем кадр в сокет
        self.connection.write_frame(&frame).await?;

        // Читаем ответ
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// Подписывает клиента на определенные каналы.
    ///
    /// После подписки на канал, клиент не может выполнять команды,
    /// не связанные с pub/sub. Функция потребляет `self` и возвращает `Subscriber`.
    ///
    /// Значение `Subscriber` используется для получения сообщений, а также
    /// для управления списком каналов, на которые подписан клиент.
    #[instrument(skip(self))]
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        // Отправляем команду подписки серверу и ждем подтверждения.
        // Клиент переходит в состояние "подписчика" и с этого момента
        // может выполняться только команды, связанные с pub/sub
        self.subscribe_cmd(&channels).await?;

        // Возвращаем тип `Subscriber`
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    /// Основная логика `SUBSCRIBE`, используемая функциями подписки.
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // Преобразуем команду `Subscribe` в кадр
        let frame = Subscribe::new(channels.to_vec()).into_frame();

        debug!(request = ?frame);

        // Записываем кадр в сокет
        self.connection.write_frame(&frame).await?;

        // Дл каждого канала, на который выполняется подписка, сервер отвечает
        // подтверждением подписки на этот канал.
        for channel in channels {
            // Читаем ответ
            let response = self.read_response().await?;

            // Проверяем, что получили подтверждение подписки
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // Сервер отвечает массивом кадров в форме:
                    //
                    // ```
                    // [ "subscribe", channel, num-subscribed ]
                    // ```
                    //
                    // где `channel` - это название канала, а
                    // `num-subscribed` - количество подписчиков этого канала
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }

    /// Читает кадр ответа из сокета.
    ///
    /// Кадр `Error` преобразуется в `Err`.
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            // Кадры `Error` преобразуются в `Err`
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // `None` - индикатор того, что сервер закрыл
                // соединение без отправки кадра
                let err = Error::new(ErrorKind::ConnectionReset, "Соединение сброшено сервером.");

                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    /// Возвращает набор каналов, на которые выполнена подписка.
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// Получает следующее сообщение, опубликованное в подписанном канале,
    /// ожидая при необходимости.
    ///
    /// `None` - индикатор прекращения подписки.
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(mframe) => {
                debug!(?mframe);

                match mframe {
                    Frame::Array(ref frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => Err(mframe.to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None),
        }
    }

    /// Преобразует подписчика в `Stream`, возвращающего (yielding) новые сообщения,
    /// опубликованные в подписанных каналах.
    ///
    /// `Subscriber` не реализует поток самостоятельно, поскольку выполнение этой задачи с помощью
    /// безопасного кода является нетривиальным. Поэтому функция преобразования
    /// предоставляется и возвращаемый поток реализуется с помощью крейта `async-stream`.
    pub fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        // Используем макрос `try_stream` из крейта `async-stream`.
        // Генераторы в `Rust` являются нестабильными. Крейт использует макрос
        // для симуляции генераторов поверх `async/await`. Существуют
        // некоторые ограничения, так что ознакомьтесь с документацией
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// Выполняет подписку на указанные каналы
    #[instrument(skip(self))]
    pub async fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        // Выполняем команду подписки
        self.client.subscribe_cmd(channels).await?;

        // Обновляем набор подписанных каналов
        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    /// Выполняет отписку от указанных каналов
    #[instrument(skip(self))]
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Unsubscribe::new(channels).into_frame();

        debug!(request = ?frame);

        // Записываем кадр в сокет
        self.client.connection.write_frame(&frame).await?;

        // Пустой список каналов означает отписку от всех каналов
        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        // Читаем ответ
        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscribed_channels.len();

                        if len == 0 {
                            // Должен быть как минимум один канал
                            return Err(response.to_error());
                        }

                        // Отписанный канал должен существовать в списке подписанных каналов на этом этапе
                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        // Только один канал должен удаляться из
                        // списка
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }
}
