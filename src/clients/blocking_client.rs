//! Минимальная реализация блокирующего клиента `Redis`.
//!
//! Предоставляет блокирующее подключение и методы для обработки поддерживаемых команд.

use bytes::Bytes;
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::runtime::Runtime;

pub use crate::clients::Message;

/// Соединение, установленное с сервером `Redis`.
///
/// Поддерживаемый одним `TcpStream`, `BlockingClient` предоставляет базовую функциональность
/// сетевого клиента (нет длинного опроса (polling), повторов и др.). Соединения устанавливаются
/// с помощью функции `connect`.
///
/// Запросы обрабатываются с помощью разных методов `BlockingClient`.
pub struct BlockingClient {
    /// Асинхронный `Client`.
    inner: crate::clients::Client,

    /// Среда `current_thread` для выполнения операций с помощью
    /// асинхронного `Client` блокирующим способом.
    rt: Runtime,
}

/// Клиент в режиме pub/sub (издатель/подписчик).
///
/// После подписки на канал, клиенты могут выполнять только команды, связанные с pub/sub.
/// Тип `BlockingClient` становится типом `BlockingSubscriber` для предотвращения вызова команд,
/// не связанных с pub/sub.
pub struct BlockingSubscriber {
    /// Асинхронный `Subscriber`.
    inner: crate::clients::Subscriber,

    /// Среда `current_thread` для выполнения операций с помощью
    /// асинхронного `Subscriber` блокирующим способом.
    rt: Runtime,
}

/// Итератор, возвращаемый `Subscriber::into_iter()`.
struct SubscriberIterator {
    /// Асинхронный `Subscriber`.
    inner: crate::clients::Subscriber,

    /// Среда `current_thread` для выполнения операций с помощью
    /// асинхронного `Subscriber` блокирующим способом.
    rt: Runtime,
}

impl BlockingClient {
    /// Устанавливает соединение с сервером `Redis`, находящимся по `addr`.
    ///
    /// `addr` - любой тип, который может быть асинхронно преобразован в
    /// `SocketAddr`. Это включает `SocketAddr` и строки. Трейт `ToSocketAddrs`
    /// предоставляется `Tokio`, а не `std`.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let client = match BlockingClient::connect("localhost:6379") {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("Провал установки соединения!"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    pub fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<BlockingClient> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let inner = rt.block_on(crate::clients::Client::connect(addr))?;

        Ok(BlockingClient { inner, rt })
    }

    /// Извлекает значение по ключу.
    ///
    /// При отсутствии значения, возвращается `None`.
    ///
    /// # Примеры
    ///
    /// ```no_run
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     let val = client.get("foo").unwrap();
    ///     println!("Получено = {:?}", val);
    /// }
    /// ```
    pub fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        self.rt.block_on(self.inner.get(key))
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
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     client.set("foo", "bar".into()).unwrap();
    ///
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    pub fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        self.rt.block_on(self.inner.set(key, value))
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
    /// use mini_redis::clients::BlockingClient;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     client.set_expires("foo", "bar".into(), ttl).unwrap();
    ///
    ///     let val = client.get("foo").unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // Ждем окончания времени жизни
    ///     thread::sleep(ttl);
    ///
    ///     let val = client.get("foo").unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    pub fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        self.rt
            .block_on(self.inner.set_expires(key, value, expiration))
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
    /// use mini_redis::clients::BlockingClient;
    ///
    /// fn main() {
    ///     let mut client = BlockingClient::connect("localhost:6379").unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).unwrap();
    ///     println!("Получено = {:?}", val);
    /// }
    /// ```
    pub fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        self.rt.block_on(self.inner.publish(channel, message))
    }

    /// Подписывает клиента на определенные каналы.
    ///
    /// После подписки на канал, клиент не может выполнять команды,
    /// не связанные с pub/sub. Функция потребляет `self` и возвращает `BlockingSubscriber`.
    ///
    /// Значение `BlockingSubscriber` используется для получения сообщений, а также
    /// для управления списком каналов, на которые подписан клиент.
    pub fn subscribe(self, channels: Vec<String>) -> crate::Result<BlockingSubscriber> {
        let subscriber = self.rt.block_on(self.inner.subscribe(channels))?;
        Ok(BlockingSubscriber {
            inner: subscriber,
            rt: self.rt,
        })
    }
}

impl BlockingSubscriber {
    /// Возвращает набор каналов, на которые выполнена подписка.
    pub fn get_subscribed(&self) -> &[String] {
        self.inner.get_subscribed()
    }

    /// Получает следующее сообщение, опубликованное в подписанном канале,
    /// ожидая при необходимости.
    ///
    /// `None` - индикатор прекращения подписки.
    pub fn next_message(&mut self) -> crate::Result<Option<Message>> {
        self.rt.block_on(self.inner.next_message())
    }

    /// Преобразует подписчика в `Iterator`, возвращающий (yielding) новые сообщения,
    /// опубликованные в подписанных каналах.
    pub fn into_iter(self) -> impl Iterator<Item = crate::Result<Message>> {
        SubscriberIterator {
            inner: self.inner,
            rt: self.rt,
        }
    }

    /// Выполняет подписку на указанные каналы.
    pub fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.subscribe(channels))
    }

    /// Выполняет отписку от указанных каналов.
    pub fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        self.rt.block_on(self.inner.unsubscribe(channels))
    }
}

impl Iterator for SubscriberIterator {
    type Item = crate::Result<Message>;

    fn next(&mut self) -> Option<crate::Result<Message>> {
        self.rt.block_on(self.inner.next_message()).transpose()
    }
}
