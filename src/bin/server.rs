//! Сервер `mini-redis`.
//!
//! Этот файл представляет собой входную точку сервера.
//! Здесь выполняется разбор командной строки и передача аргументов в
//! `mini_redis::server`.
//!
//! Для разбора командной строки используется крейт `clap`.

use mini_redis::{server, DEFAULT_PORT};

use clap::Parser;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    set_up_logging()?;

    let cli = Cli::parse();
    let port = cli.port.unwrap_or(DEFAULT_PORT);

    // Привязываем обработчик TCP
    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await;

    Ok(())
}

#[derive(Parser, Debug)]
#[clap(name = "mini-redis-server", version, author, about = "A Redis server")]
struct Cli {
    #[clap(long)]
    port: Option<u16>,
}

#[cfg(not(feature = "otel"))]
fn set_up_logging() -> mini_redis::Result<()> {
    // См. https://docs.rs/tracing
    tracing_subscriber::fmt::try_init()
}
