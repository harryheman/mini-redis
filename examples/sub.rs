//! Пример подписки на канал `Redis`.
//!
//! Простой клиент, который подключается к серверу `mini-redis`,
//! подписывается на канал `foo`
//! и ждет публикации сообщений в этом канале.
//!
//! Команда для запуска сервера:
//!
//!     cargo run --bin mini-redis-server
//!
//! Команда для запуска примера (выполняется в другом терминале):
//!
//!     cargo run --example sub
//!
//! Команда для публикации сообщения (выполняется в другом терминале):
//!
//!     cargo run --example pub

#![warn(rust_2018_idioms)]

use mini_redis::{clients::Client, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    // Открываем соединение с сервером
    let client = Client::connect("127.0.0.1:6379").await?;

    // Подписываемся на канал `foo`
    let mut subscriber = client.subscribe(vec!["foo".into()]).await?;

    // Ждем публикации сообщений в канале `foo`
    if let Some(msg) = subscriber.next_message().await? {
        println!(
            "Получено сообщение `{:?}` из канала `{}`",
            msg.content, msg.channel,
        );
    }

    Ok(())
}
