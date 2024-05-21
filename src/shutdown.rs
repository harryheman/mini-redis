use tokio::sync::broadcast;

/// Регистрирует сигнал о закрытии сервера.
///
/// Такой сигнал принимается с помощью `broadcast::Receiver`. Отправляется только
/// одно значение. Как только значение отправляется через широковещательный канал,
/// сервер должен быть закрыт.
///
/// Структура `Shutdown` регистрирует сигнал и отслеживает, что он
/// был получен. Вызывающая сторона может запрашивать информацию о получении сигнала.
#[derive(Debug)]
pub(crate) struct Shutdown {
    /// `true`, если сигнал о закрытии получен.
    is_shutdown: bool,

    /// Принимающая половина канала используется для регистрации сигнала о закрытии сервера.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// Создает новый `Shutdown`, поддерживаемый переданным `broadcast::Receiver`.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            is_shutdown: false,
            notify,
        }
    }

    /// Возвращает `true`, если сигнал о закрытии получен.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Получает уведомление о закрытии, ждет при необходимости.
    pub(crate) async fn recv(&mut self) {
        // Если сигнал уже получен, сразу возвращаемся.
        if self.is_shutdown {
            return;
        }

        // Может быть отправлено только одно сообщение, поэтому ошибка невозможна.
        let _ = self.notify.recv().await;

        // Обновляем индикатор получения сигнала.
        self.is_shutdown = true;
    }
}
