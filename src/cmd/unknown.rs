use crate::{Connection, Frame};

use tracing::{debug, instrument};

/// Представляет "неизвестную" команду. Это не настоящая команда `Redis`
#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// Создает новую команду `Unknown` для ответа на неизвестные команды клиента
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    /// Возвращает название команды
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    /// Отвечает клиенту о том, что команда не распознана.
    ///
    /// Обычно это означает, что команда еще не реализована `mini-redis`
    #[instrument(skip(self, dst))]
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
