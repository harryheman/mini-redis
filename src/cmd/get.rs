use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Извлекает значение по ключу.
///
/// При отсутствии значения, возвращается специальное значение `nil`. Ошибка
/// возвращается, если значение не является строкой, поскольку `GET`
/// работает только со строками
#[derive(Debug)]
pub struct Get {
    /// Названия ключа для получения
    key: String,
}

impl Get {
    /// Создает новую команду `Get`, которая запрашивает `key`
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// Возвращает ключ
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Разбирает экземпляр `Get` из полученного кадра.
    ///
    /// Аргумент `Parse` предоставляет подобное курсору (cursor-like) API для чтения полей из
    /// `Frame`. На этом этапе из сокета получен весь кадр.
    ///
    /// Строка `GET` уж потреблена.
    ///
    /// # Возвращаемые значения
    ///
    /// Возвращает значение `Get` при успехе. Если кадр испорчен,
    /// возвращается `Err`.
    ///
    /// # Формат
    ///
    /// Ожидается массив кадров, содержащий 2 сущности:
    ///
    /// ```text
    /// GET key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // Строка `GET` уже потреблена. Следующим значением является название ключа.
        // Если следующее значение не является строкой или входные данные
        // полностью потреблены, возвращается ошибка
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    /// Применяет команду `Get` к определенному экземпляру `Db`.
    ///
    /// Ответ записывается в `dst`. Это вызывается сервером для
    /// выполнения полученной команды
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Извлекаем значение из общего состояния БД
        let response = if let Some(value) = db.get(&self.key) {
            // Если значение имеется, оно возвращается клиенту в "групповом" формате
            Frame::Bulk(value)
        } else {
            // При отсутствии значения возвращается `Null`
            Frame::Null
        };

        debug!(?response);

        // Возвращаем ответ клиенту
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Преобразует команду в соответствующий `Frame`.
    ///
    /// Это вызывается клиентом при кодировке команды `Get`
    /// для отправки на сервер
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
