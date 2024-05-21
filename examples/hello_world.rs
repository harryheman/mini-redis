//! Простой клиент, который подключается к серверу `mini-redis`,
//! устанавливает значение `world` для ключа `hello`,
//! и получает это значение от сервера.
//!
//! Команда для запуска сервера:
//!
//!     cargo run --bin mini-redis-server
//!
//! Команда для запуска примера (выполняется в другом терминале):
//!
//!     cargo run --example hello_world

#![warn(rust_2018_idioms)]

use mini_redis::{clients::Client, Result};

#[tokio::main]
pub async fn main() -> Result<()> {
    // Открываем соединение с сервером
    let mut client = Client::connect("127.0.0.1:6379").await?;

    // Устанавливаем значение `world` для ключа `hello`
    client.set("hello", "world".into()).await?;

    // Получаем значение по ключу `hello`
    let result = client.get("hello").await?;

    println!("{:?}", result.unwrap());

    Ok(())
}
