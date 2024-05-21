mod get;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

/// Перечисление поддерживаемых команд.
///
/// Методы, вызываемые на `Command`, делегируются реализации команды
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Unknown(Unknown),
}

impl Command {
    /// Разбирает команду из полученного кадра.
    ///
    /// `Frame` должен представлять команду `Redis`, поддерживаемую `mini-redis`, и
    /// являться массивом.
    ///
    /// # Возвращаемые значения
    ///
    /// При успехе возвращается команда, иначе, возвращается `Err`
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // Значение кадра декорируется с помощью `Parse`. `Parse` предоставляет
        // подобное курсору (cursor-like) API, облегчающее разбор команды.
        //
        // Значение кадра должно быть массивом. Другие варианты
        // приводят к возврату ошибки
        let mut parse = Parse::new(frame)?;

        // Все команды начинаются с названия команды в виде строки. Название
        // читается и приводится к нижнему регистру для выполнения чувствительного к регистру сопоставления
        let command_name = parse.next_string()?.to_lowercase();

        // Сопоставляем название команды, делегируя ее дальнейший разбор реализации
        // соответствующей команды
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            _ => {
                // Команда не распознана, возвращается `Unknown`.
                //
                // `return` вызывается здесь для предотвращения вызова `finish` ниже. Поскольку
                // команда не была распознана, с высокой долей вероятности
                // в экземпляре `Parse` остались непотребленные поля
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // Проверяем наличие непотребленных полей в значении `Parse`.
        // Наличие таких полей указывает на неожиданный формат кадра,
        // возвращается ошибка
        parse.finish()?;

        // Команда была успешно разобрана
        Ok(command)
    }

    /// Применяет команду к определенному экземпляру `Db`.
    ///
    /// Ответ записывается в `dst`. Это вызывается сервером для
    /// выполнения полученной команды
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Ping(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
            // `Unsubscribe` не может применяться здесь. Она может приходить только
            // из контекста команды `Subscribe`
            Unsubscribe(_) => Err("`Unsubscribe` не поддерживается в этом контексте".into()),
        }
    }

    /// Возвращает название команды
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Ping(_) => "ping",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
