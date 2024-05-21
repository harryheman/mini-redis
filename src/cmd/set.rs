use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

/// Устанавливает строковое `value` для `key`.
///
/// Предыдущее значение перезаписывается, независимо от типа (при наличии).
/// Предыдущее время жизни отбрасывается (discard) при успешной операции `SET`.
///
/// # Настройки
///
/// Поддерживаются следующие настройки:
///
/// * EX `seconds` - время жизни в секундах.
/// * PX `milliseconds` - время жизни в миллисекундах.
#[derive(Debug)]
pub struct Set {
    /// Ключ для поиска
    key: String,

    /// Значение для хранения
    value: Bytes,

    /// Время жизни ключа
    expire: Option<Duration>,
}

impl Set {
    /// Создает новую команду `Set`, устанавливающую `value` для `key`.
    ///
    /// Если `expire` является `Some`, значение должно быть удалено по истечение определенного времени.
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    /// Возвращает ключ
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Возвращает значение
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Возвращает время жизни
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    /// Разбирает экземпляр `Set` из полученного кадра.
    ///
    /// Аргумент `Parse` предоставляет подобное курсору (cursor-like) API для чтения полей из
    /// `Frame`. На этом этапе из сокета получен весь кадр.
    ///
    /// Строка `SET` уже потреблена.
    ///
    /// # Возвращаемые значения
    ///
    /// Возвращает значение `Get` при успехе. Если кадр испорчен,
    /// возвращается `Err`.
    ///
    /// # Формат
    ///
    /// Ожидается массив, состоящий минимум из 3 сущностей:
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // Читаем ключ для установки. Это обязательное поле
        let key = parse.next_string()?;

        // Читаем значение для установки. Это обязательное поле
        let value = parse.next_bytes()?;

        // Время жизни является опциональным. Если отсутствует, то имеет значение
        // `None`.
        let mut expire = None;

        // Пытаемся разобрать следующую строку
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // Время жизни определено в секундах. Следующее значение -
                // целое число
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // Время жизни определено в миллисекундах. Следующее значение -
                // целое число
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            // `mini-redis` не поддерживает другие настройки `SET`
            // Ошибка, возникающая здесь, приводит к закрытию соединения.
            // Другие соединения продолжают нормально функционировать
            Ok(_) => return Err("`SET` поддерживает только настройку `expiration`.".into()),
            // Ошибка `EndOfStream` является индикатором того, что для разбора не осталось данных.
            // Это нормальная ситуация времени выполнения, означающая, что
            // настройки `SET` отсутствуют
            Err(EndOfStream) => {}
            // Другие ошибки всплывают наверх, что приводит к прерыванию соединения
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    /// Применяет команду `Set` к определенному
    /// экземпляру `Db`.
    ///
    /// Ответ записывается в `dst`. Это вызывается сервером для
    /// выполнения полученной команды
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Установка значения в общее состояние БД
        db.set(self.key, self.value, self.expire);

        // Создание успешного ответа и его запись в `dst`
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Преобразует команду в соответствующий `Frame`.
    ///
    /// Это вызывается клиентом при кодировке команды `Set`
    /// для отправки на сервер
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(ms) = self.expire {
            // Время жизни в протоколе `Redis` может быть определено двумя способами:
            // 1. SET key value EX seconds
            // 2. SET key value PX milliseconds
            // Мы выбираем второй вариант, поскольку он предоставляет большую точность и парсер в
            // `src/bin/cli.rs` разбирает аргумент `expiration` как миллисекунды
            // в `duration_from_ms_str()`
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }
}
