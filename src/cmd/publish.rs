use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;

/// Публикует сообщение в определенном канале.
///
/// Отправляет сообщение в канал без знания об индивидуальных потребителях.
/// Потребители могут подписываться на каналы для получения сообщений.
///
/// Названия каналов не имеют отношения к пространству "ключ-значение".
/// Публикация в канале `foo` не связана с установкой значения для ключа `foo`
#[derive(Debug)]
pub struct Publish {
    /// Название канала
    channel: String,

    /// Сообщение для публикации
    message: Bytes,
}

impl Publish {
    /// Создает новую команду `Publish`, отправляющую `message` в `channel`
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }

    /// Разбирает экземпляр `Publish` из полученного кадра.
    ///
    /// Аргумент `Parse` предоставляет подобное курсору (cursor-like) API для чтения полей из
    /// `Frame`. На этом этапе из сокета получен весь кадр.
    ///
    /// Строка `PUBLISH` уже потреблена.
    ///
    /// # Возвращаемые значения
    ///
    /// Возвращает значение `Publish` при успехе. Если кадр испорчен,
    /// возвращается `Err`.
    ///
    /// # Формат
    ///
    /// Ожидается массив кадров, содержащий 3 сущности:
    ///
    /// ```text
    /// PUBLISH channel message
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        // Извлекаем из кадра значения `channel` и `message`.
        //
        // `channel` должен быть валидной строкой
        let channel = parse.next_string()?;

        // `message` - это байты
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    /// Применяет команду `Publish` к определенному экземпляру `Db`.
    ///
    /// Ответ записывается в `dst`. Это вызывается сервером для
    /// выполнения полученной команды
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Общее состояние содержит `tokio::sync::broadcast::Sender` для
        // всех активных каналов. Вызов `db.publish` отправляет сообщение в
        // соответствующий канал.
        //
        // Возвращается количество подписчиков на канал.
        // Это не означает, что `num_subscriber` получат сообщение.
        // Подписчики могут отписаться от канала в любое время.
        // Поэтому `num_subscribers` должна использоваться только в качестве
        // "подсказки"
        let num_subscribers = db.publish(&self.channel, self.message);

        // В ответ на запрос публикации возвращается количество подписчиков на канал
        let response = Frame::Integer(num_subscribers as u64);

        // Возвращаем ответ клиенту
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Преобразует команду в соответствующий `Frame`.
    ///
    /// Это вызывается клиентом при кодировке команды `Publish`
    /// для отправки на сервер
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}
