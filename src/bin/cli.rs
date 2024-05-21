use mini_redis::{clients::Client, DEFAULT_PORT};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::convert::Infallible;
use std::num::ParseIntError;
use std::str;
use std::time::Duration;

#[derive(Parser, Debug)]
#[clap(
    name = "mini-redis-cli",
    version,
    author,
    about = "Выполнение команд Redis"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[clap(name = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[clap(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    Ping {
        /// Сообщение для пинга.
        #[clap(value_parser = bytes_from_str)]
        msg: Option<Bytes>,
    },
    /// Извлекает значение по ключу.
    Get {
        /// Название ключа.
        key: String,
    },
    /// Устанавливает значение по ключу.
    Set {
        /// Название ключа.
        key: String,

        /// Значение для установки.
        #[clap(value_parser = bytes_from_str)]
        value: Bytes,

        /// Значение истекает после определенного времени.
        #[clap(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    /// Издатель для отправки сообщения в определенный канал.
    Publish {
        /// Название канала.
        channel: String,

        #[clap(value_parser = bytes_from_str)]
        /// Сообщение для отправки.
        message: Bytes,
    },
    /// Подписывает клиента на определенный канал или каналы.
    Subscribe {
        /// Канал или каналы для подписки.
        channels: Vec<String>,
    },
}

/// Входная точка CLI.
///
/// Аннотация `[tokio::main]` означает, что при вызове функции должна быть
/// запущена среда выполнения `Tokio`. Тело функции выполняется в выделенной
/// среде выполнения.
///
/// `flavor = "current_thread"` используется для предотвращения выделения фоновых потоков.
#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {
    // Включаем логирование
    tracing_subscriber::fmt::try_init()?;

    // Разбираем аргументы командной строки
    let cli = Cli::parse();

    // Получаем адрес для подключения
    let addr = format!("{}:{}", cli.host, cli.port);

    // Устанавливаем соединение
    let mut client = Client::connect(&addr).await?;

    // Обрабатываем команду
    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("Должны быть предоставлены каналы".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            // Ждем сообщения в канале
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "Из канала {} получено сообщение {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(src: &str) -> Result<Bytes, Infallible> {
    Ok(Bytes::from(src.to_string()))
}
