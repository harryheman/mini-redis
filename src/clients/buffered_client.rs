use crate::clients::Client;
use crate::Result;

use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;

// Перечисление, используемое для передачи команды из обработчика `BufferedClient`
#[derive(Debug)]
enum Command {
    Get(String),
    Set(String, Bytes),
}

// Тип сообщения, передаваемый через канал в задачу соединения.
//
// `Command` - это команда для передачи в соединение.
//
// `oneshot::Sender` - тип канала, отправляющий единичное значение. Используется
// здесь для отправки ответа, полученного из соединения, вызывающей стороне
type Message = (Command, oneshot::Sender<Result<Option<Bytes>>>);

/// Получает команды через канал и передает их клиенту.
/// Ответ возвращается вызывающей стороне через `oneshot`
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    // Извлекаем сообщения из канала в цикле. `None`
    // является индикатором того, что все обработчики `BufferedClient` уничтожены и
    // сообщений в канале больше не будет
    while let Some((cmd, tx)) = rx.recv().await {
        // Команда передается в соединение
        let response = match cmd {
            Command::Get(key) => client.get(&key).await,
            Command::Set(key, value) => client.set(&key, value).await.map(|_| None),
        };

        // Возвращаем ответ вызывающей стороне.
        //
        // Провал отправки сообщения свидетельствует о том, что половина `rx` уничтожена.
        // Это нормальное событие среды выполнения
        let _ = tx.send(response);
    }
}

#[derive(Clone)]
pub struct BufferedClient {
    tx: Sender<Message>,
}

impl BufferedClient {
    /// Создает новый буфер запросов клиента.
    ///
    /// `Client` выполняет команды `Redis` прямо на соединении TCP.
    /// Только один запрос может одновременно находиться в процессе выполнения.
    /// Операциям требуется мутабельный доступ к обработчику `Client`. Это предотвращает использование
    /// одного соединения `Redis` несколькими задачами `Tokio`.
    ///
    /// Стратегией решения этого класса проблем является выделение отдельной
    /// задачи `Tokio` для управления соединением `Redis` и использование
    /// "передачи сообщения" в соединение. Команды помещаются в канал.
    /// Задача соединения извлекает команды из канала и применяет их к
    /// соединению `Redis`. При получении ответа, он возвращается
    /// запрашивающей стороне.
    ///
    /// Возвращаемый обработчик `BufferedClient` может быть клонирован перед передачей
    /// нового обработчика в отдельные задачи.
    pub fn buffer(client: Client) -> BufferedClient {
        // Устанавливаем лимит сообщений в 32. В реальном приложении
        // размер буфера должен быть настраиваемым
        let (tx, rx) = channel(32);

        // Выделяем задачу для обработки запросов соединения
        tokio::spawn(async move { run(client, rx).await });

        // Возвращаем обработчик `BufferedClient`
        BufferedClient { tx }
    }

    /// Извлекает значение по ключу.
    ///
    /// Аналогично `Client::get`, но запросы помещаются в буфер,
    /// пока соответствующее соединение не сможет отправить запрос
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        // Инициализируем новую команду `Get` для отправки через канал
        let get = Command::Get(key.into());

        // Инициализируем новый `oneshot` для получения ответа из соединения
        let (tx, rx) = oneshot::channel();

        // Отправляем запрос
        self.tx.send((get, tx)).await?;

        // Ждем ответ
        match rx.await {
            Ok(res) => res,
            Err(err) => Err(err.into()),
        }
    }

    /// Устанавливает `value` для `key`.
    ///
    /// Аналогично `Client::set`, но запросы помещаются в буфер,
    /// пока соответствующее соединение не сможет отправить запрос
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<()> {
        // Инициализируем новую команду `Set` для отправки через канал
        let set = Command::Set(key.into(), value);

        // Инициализируем новый `oneshot` для получения ответа из соединения
        let (tx, rx) = oneshot::channel();

        // Отправляем запрос
        self.tx.send((set, tx)).await?;

        // Ждем ответ
        match rx.await {
            Ok(res) => res.map(|_| ()),
            Err(err) => Err(err.into()),
        }
    }
}
