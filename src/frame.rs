//! Предоставляет тип, представляющий кадр протокола `Redis`, а также
//! утилиты для разбора кадров из массива байтов.

use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

/// Кадр протокола `Redis`.
#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    /// Недостаточно данных для разбора сообщения.
    Incomplete,

    /// Невалидная кодировка сообщения.
    Other(crate::Error),
}

impl Frame {
    /// Возвращает пустой массив.
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    /// Добавляет кадр `Bulk` в массив. `self` должен быть кадром `Array`.
    ///
    /// # Паника
    ///
    /// Паникует, если `self` не является массивом.
    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("Кадр не является массивом!"),
        }
    }

    /// Добавляет кадр `Integer` в массив. `self` должен быть кадром `Array`.
    ///
    /// # Паника
    ///
    /// Паникует, если `self` не является массивом.
    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("Кадр не является массивом!"),
        }
    }

    /// Проверяет, что из `src` может быть декодировано целое сообщение
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        match get_u8(src)? {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    // Пропускаем '-1\r\n'.
                    skip(src, 4)
                } else {
                    // Читаем объемную (bulk) строку.
                    let len: usize = get_decimal(src)?.try_into()?;

                    // Пропускаем это число + 2 (\r\n) байта.
                    skip(src, len + 2)
                }
            }
            b'*' => {
                let len = get_decimal(src)?;

                for _ in 0..len {
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("Ошибка протокола; невалидный тип кадра `{}`.", actual).into()),
        }
    }

    /// Сообщение было проверено с помощью `check`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        match get_u8(src)? {
            b'+' => {
                // Читаем линию и преобразуем ее в `Vec<u8>`.
                let line = get_line(src)?.to_vec();

                // Преобразуем `Vec<u8>` в `String`.
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' => {
                // Читаем линию и преобразуем ее в `Vec<u8>`.
                let line = get_line(src)?.to_vec();

                // Преобразуем `Vec<u8>` в `String`.
                let string = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' => {
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;

                    if line != b"-1" {
                        return Err("Ошибка протокола; невалидный формат кадра.".into());
                    }

                    Ok(Frame::Null)
                } else {
                    // Читаем объемную строку.
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;

                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                    // Пропускаем это число + 2 (\r\n) байта.
                    skip(src, n)?;

                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }

    /// Преобразует кадр в ошибку "Неожиданный кадр"
    pub(crate) fn to_error(&self) -> crate::Error {
        format!("Неожиданный кадр: {}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "Ошибка: {}.", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // Используем пробел в качестве разделителя элементов массива.
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}

fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.chunk()[0])
}

fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    if !src.has_remaining() {
        return Err(Error::Incomplete);
    }

    Ok(src.get_u8())
}

fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }

    src.advance(n);
    Ok(())
}

fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;

    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "Ошибка протокола; невалидный формат кадра.".into())
}

/// Ищет линию.
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // Сканируем байты.
    let start = src.position() as usize;
    // Сканируем до предпоследнего байта.
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // Мы нашли линию, обновляем позицию, чтобы она шла после `\n`.
            src.set_position((i + 2) as u64);

            // Возвращаем линию.
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "Ошибка протокола; невалидный формат кадра.".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "Ошибка протокола; невалидный формат кадра.".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "Поток кончился слишком рано.".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
