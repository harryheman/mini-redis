use crate::{Connection, Frame, Parse, ParseError};
use bytes::Bytes;
use tracing::{debug, instrument};

/// Возвращает `PONG` при отсутствии аргументов,
/// иначе, возвращает копию аргументов в виде группы (bulk).
///
/// Эта команда часто используется для тестирования того,
/// что соединение открыто, а также для измерения задержки
#[derive(Debug, Default)]
pub struct Ping {
    /// Опциональное сообщение для возврата
    msg: Option<Bytes>,
}

impl Ping {
    /// Создает новую команду `Ping` с опциональным `msg`
    pub fn new(msg: Option<Bytes>) -> Ping {
        Ping { msg }
    }

    /// Разбирает экземпляр `Ping` из полученного кадра.
    ///
    /// Аргумент `Parse` предоставляет подобное курсору (cursor-like) API для чтения полей из
    /// `Frame`. На этом этапе из сокета получен весь кадр.
    ///
    /// Строка `PING` уже потреблена.
    ///
    /// # Возвращаемые значения
    ///
    /// Возвращает значение `Ping` при успехе. Если кадр испорчен,
    /// возвращается `Err`.
    ///
    /// # Формат
    ///
    /// Ожидается массив кадров, содержащий `PING` и опциональное сообщение:
    ///
    /// ```text
    /// PING [message]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_bytes() {
            Ok(msg) => Ok(Ping::new(Some(msg))),
            Err(ParseError::EndOfStream) => Ok(Ping::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Применяет команду `Ping` и возвращает сообщение.
    ///
    /// Ответ записывается в `dst`. Это вызывается сервером для
    /// выполнения полученной команды
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(msg),
        };

        debug!(?response);

        // Возвращаем ответ клиенту
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Преобразует команду в соответствующий `Frame`.
    ///
    /// Это вызывается клиентом при кодировке команды `Ping`
    /// для отправки на сервер
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("ping".as_bytes()));
        if let Some(msg) = self.msg {
            frame.push_bulk(msg);
        }
        frame
    }
}
