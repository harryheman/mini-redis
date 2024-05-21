//! Пример публикации сообщения в канале `Redis`.
//!
//! Простой клиент, который подключается к серверу `mini-redis`,
//! и публикует сообщение в канале `foo`.
//!
//! Команда для запуска сервера:
//!
//!     cargo run --bin mini-redis-server
//!
//! Команда для подписки на канал (выполняется в другом терминале):
//!
//!     cargo run --example sub
//!
//! Команда для запуска примера (выполняется в другом терминале):
//!
//!     cargo run --example pub

#![warn(rust_2018_idioms)]

use mini_redis::{clients::Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Открываем соединение с сервером
    let mut client = Client::connect("127.0.0.1:6379").await?;

    // Публикуем сообщение `bar` в канале `foo`
    client.publish("foo", "bar".into()).await?;

    Ok(())
}
