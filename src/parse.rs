use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// Утилита для разбора команды.
///
/// Команды представлены как массивы кадров. Каждая сущность кадра - это
/// "токен". `Parse` инициализируется массивом кадров и предоставляет
/// подобный курсору интерфейс (cursor-like API). Каждая структура команды включает метод `parse_frame`, который
/// использует `Parse` для извлечения значений.
#[derive(Debug)]
pub(crate) struct Parse {
    /// Итератор массива кадров.
    parts: vec::IntoIter<Frame>,
}

/// Ошибка, возникающая при разборе кадра.
///
/// Только ошибки `EndOfStream` обрабатываются во время выполнения. Другие ошибки приводят к
/// закрытию соединения.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// Попытка извлечь значение проваливается из-за полного потребления кадра.
    EndOfStream,

    /// Другие ошибки.
    Other(crate::Error),
}

impl Parse {
    /// Создает новый `Parse` для разбора содержимого `frame`.
    ///
    /// Возвращает `Err`, если `frame` не является массивом кадров.
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => {
                return Err(
                    format!("Ошибка протокола; ожидается массив, получено {:?}", frame).into(),
                )
            }
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// Возвращает следующую сущность/кадр.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// Возвращает следующую сущность в виде строки.
    ///
    /// Если следующая сущность не может быть представлена как `String`, возвращается ошибка.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            // Представления `Simple` и `Bulk` могут быть строками. Строки
            // разбираются в UTF-8.
            //
            // Хотя ошибки хранятся как строки, они считаются отдельным типом.
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "Ошибка протокола; невалидная строка".into()),
            frame => Err(format!(
                "Ошибка протокола; ожидается кадр или группа кадров, получено {:?}",
                frame
            )
            .into()),
        }
    }

    /// Возвращает следующую сущность в виде "сырых" (raw) байтов.
    ///
    /// Если следующая сущность не может быть представлена в виде сырых байтов, возвращается ошибка.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        match self.next()? {
            // Типы кадров `Simple` и `Bulk` могут быть представлены в виде сырых байтов.
            //
            // Хотя ошибки хранятся как строки и могут быть представлены
            // в виде сырых байтов, они считаются отдельным типом.
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "Ошибка протокола; ожидается кадр или группа кадров, получено {:?}",
                frame
            )
            .into()),
        }
    }

    /// Возвращает следующий кадр как целое число.
    ///
    /// Это включает типы кадров `Simple`, `Bulk` и `Integer`. Типы `Simple` и
    /// `Bulk` разбираются.
    ///
    /// Если следующая сущность не может быть представлена в виде целого числа, возвращается ошибка.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "Ошибка протокола; невалидное число";

        match self.next()? {
            // Кадр `Integer` хранится в виде целого числа.
            Frame::Integer(v) => Ok(v),
            // Кадры `Simple` и `Bulk` должны быть разобраны как целые числа. Если разбор
            // проваливается, возвращается ошибка.
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!(
                "Ошибка протокола; ожидается кадр `int`, получено {:?}",
                frame
            )
            .into()),
        }
    }

    /// Проверяет отсутствие сущностей в массиве.
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("Ошибка протокола; ожидается конец кадра, но обнаружены новые кадры.".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "Ошибка протокола; неожиданный конец потока.".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}
