//! Минимальная реализация сервера `Redis`.
//!
//! Предоставляет асинхронную функцию `run`, регистрирующую входящие соединения и
//! выделяющую (spawn) задачу на каждое из них.

use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// Состояние обработчика сервера. Создается в вызове `run`. Включает метод `run`,
/// прослушивающий TCP и инициализирующий состояние каждого соединения.
#[derive(Debug)]
struct Listener {
    /// Общий обработчик БД.
    ///
    /// Содержит хранилище в форме "ключ-значение", а также широковещательные каналы для
    /// pub/sub (издатель/подписчик).
    ///
    /// Обертка вокруг `Arc`. Внутренняя `Db` может быть
    /// извлечена и передана в состояние каждого соединения (`Handler`).
    db_holder: DbDropGuard,

    /// Обработчик TCP, передаваемый стороне, вызывающей `run`.
    listener: TcpListener,

    /// Максимальное количество подключений.
    ///
    /// `Semaphore` используется для ограничения максимального количества соединений.
    /// Перед попыткой принять новое соединение, проверяется разрешение (permit) из
    /// семафора. Если разрешения отсутствуют, обработчик переходит в состояние ожидания.
    ///
    /// После завершения обработки соединения, в семафор возвращается
    /// разрешение.
    limit_connections: Arc<Semaphore>,

    /// Передает сигнал о закрытии всем активным подключениям.
    ///
    /// Начальный триггер `shutdown` предоставляется стороной, вызывающей `run`.
    /// Сервер отвечает за плавное закрытие активных соединений.
    /// При выделении задачи для соединения, ей передается обработчик приемника
    /// вещания (broadcast receiver handle). При инициализации закрытия, значение `()` передается через
    /// `broadcast::Sender`. Каждое активное соединение получает его, достигает
    /// безопасного состояния и завершает задачу.
    notify_shutdown: broadcast::Sender<()>,

    /// Используется как часть плавного закрытия процесса для ожидания полной
    /// обработки соединений клиента.
    ///
    /// Каналы `Tokio` закрываются как только все обработчики `Sender` выходят за пределы области видимости.
    /// После закрытия канала, приемник получает `None`. Это
    /// позволяет определить завершение всех обработчиков соединения. При
    /// инициализации обработчика соединения, ему присваивается клон
    /// `shutdown_complete_tx`. При закрытии обработчика, он уничтожает
    /// передатчик, удерживаемый этим полем `shutdown_complete_tx`. После
    /// завершения всех задач обработчика, все клоны `Sender` также уничтожаются. Это приводит
    /// к завершению `shutdown_complete_rx.recv()` с `None`. После этого
    /// выход из серверного процесса становится безопасным.
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// Обработчик соединения. Читает запросы из `connection` и применяет
/// команды к `db`.
#[derive(Debug)]
struct Handler {
    /// Общий обработчик БД.
    ///
    /// При получении команды из `connection`, она применяется с `db`.
    /// Реализация команды находится в модуле `cmd`. Каждая команда
    /// взаимодействует с `db` для завершения работы.
    db: Db,

    /// Соединение TCP декорируется кодировщиком/декодером протокола `Redis`,
    /// реализованным с помощью буферного `TcpStream`.
    ///
    /// При получении входящего соединения `Listener`, `TcpStream`
    /// передается в `Connection::new`, который инициализирует соответствующие буферы.
    /// `Connection` позволяет обработчику оперировать на уровне "кадра" и
    /// инкапсулировать детали разбора байтов.
    connection: Connection,

    /// Регистрирует уведомления о закрытии.
    ///
    /// Обертка над `broadcast::Receiver` в сочетании с передатчиком в
    /// `Listener`. Обработчик соединения обрабатывает запросы из
    /// соединения до тех пор, пока клиент не отключится или пока не будет получено
    /// уведомление о закрытии из `shutdown`. В последнем случае любая выполняемая работа продолжается
    /// до достижения безопасного состояния, после чего соединение закрывается.
    shutdown: Shutdown,

    /// Предназначено для внутреннего использования.
    _shutdown_complete: mpsc::Sender<()>,
}

/// Максимальное количество соединений, которые будет принимать сервер.
///
/// При достижении этого лимита, сервер перестает принимать соединения,
/// пока активное соединение не будет прервано.
///
/// В реальном приложении это значение будет настраиваемым.
const MAX_CONNECTIONS: usize = 250;

/// Запускает сервер `mini-redis`.
///
/// Принимает соединения из переданного обработчика. Для каждого входящего
/// соединения
/// выделяется задача для его обработки. Сервер работает до завершения
/// `shutdown`, после чего плавно закрывается.
///
/// `tokio::signal::ctrl_c()` может быть использован в качестве аргумента `shutdown`. Регистрируется сигнал `SIGINT`.
pub async fn run(listener: TcpListener, shutdown: impl Future) {
    // После завершения переданного `shutdown`, мы должны отправить сообщение о
    // закрытии всем активным соединениям. Для этой цели используется широковещательный
    // канал. В приведенном ниже коде игнорируется приемник широковещательной пары.
    // Для создания приемника может использоваться метод передатчика `subscribe`.
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    // Инициализируем состояние обработчика.
    let mut server = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    // Конкурентно запускает сервер и регистрирует сигнал `shutdown`.
    // Задача сервера выполняется до получения ошибки, поэтому при нормальных
    // обстоятельствах эта инструкция `select!` выполняется до получения сигнала
    // `shutdown`.
    //
    // Инструкция `select!` написана в форме:
    //
    // ```
    // <результат асинхронной операции> = <асинхронная операция> => <операция обработки результата>
    // ```
    //
    // Все инструкции `<асинхронная операция>` выполняются параллельно. После завершения первой
    // операции, выполняется ее `<операция обработки результата>`.
    //
    // Макрос `select!` - основной строительный блок асинхронного
    // `Rust`. См.: https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // Если здесь получена ошибка, значит установка соединения обработчиком TCP
            // провалилась несколько раз, сервер сдался и закрылся.
            //
            // Ошибки, возникающие при обработке отдельных соединений, не
            // достигают этой точки.
            if let Err(err) = res {
                error!(cause = %err, "Провал установки соединения.");
            }
        }
        _ = shutdown => {
            // Был получен сигнал о закрытии.
            info!("Закрытие...");
        }
    }

    // Извлекаем приемник `shutdown_complete` и явно уничтожаем
    // передатчик `shutdown_transmitter`. Это важно, поскольку в противном случае
    // `.await` ниже никогда не завершится.
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // При уничтожении `notify_shutdown`, все подписанные задачи
    // получают сигнал о закрытии.
    drop(notify_shutdown);
    // Уничтожаем финального `Sender`, чтобы `Receiver` ниже мог завершиться.
    drop(shutdown_complete_tx);

    // Ждем завершения обработки все активных соединений. Поскольку
    // `Sender`, удерживаемый обработчиком, был уничтожен выше, оставшиеся
    // экземпляры `Sender` удерживаются задачами обработчика соединения. При их уничтожении,
    // канал `mpsc` закрывается и `recv()` возвращает `None`.
    let _ = shutdown_complete_rx.recv().await;
}

impl Listener {
    /// Запускает сервер.
    ///
    /// Регистрирует входящие соединения. Для каждого соединения выделяется
    /// задача для его обработки.
    ///
    /// # Ошибки
    ///
    /// Возвращает `Err`, если установка соединения возвращает ошибку. Это может произойти
    /// по нескольким причинам. Например, если операционная система
    /// достигнет внутреннего лимита сокетов, установка соединения провалится.
    ///
    /// Процесс не может определить, когда переходная (transient) ошибка разрешается сама
    /// собой. Мы решаем эту проблему за счет нескольких попыток установить соединение
    /// с увеличивающейся задержкой между попытками.
    async fn run(&mut self) -> crate::Result<()> {
        info!("Установка соединения...");

        loop {
            // Ждем доступности разрешения (permit).
            //
            // `acquire_owned()` возвращает разрешение, привязанное к семафору.
            // Когда разрешение уничтожается, оно автоматически возвращается
            // в семафор.
            //
            // `acquire_owned()` возвращает `Err`, когда семафор закрывается.
            // Мы никогда этого не делаем, так что `unwrap()` является безопасным.
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            // Принимаем новый сокет. Это включает обработку ошибок.
            // Метод `accept` обрабатывает ошибки самостоятельно, так что
            // возникшая здесь ошибка является невосстановимой (non-recoverable).
            let socket = self.accept().await?;

            // Создаем необходимое состояние обработчика соединения.
            let mut handler = Handler {
                // Получаем общий обработчик БД.
                db: self.db_holder.db(),

                // Инициализируем состояние соединения. Это выделяет буферы
                // чтения/записи для разбора кадров протокола `Redis`.
                connection: Connection::new(socket),

                // Подписываемся на уведомления о закрытии.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                // Уведомляем приемник об уничтожении всех клонов.
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            // Выделяем новую задачу для обработки соединения. Задачи `Tokio` похожи на
            // асинхронные зеленые потоки (green threads) и выполняются параллельно.
            tokio::spawn(async move {
                // Обрабатываем соединение. Если возникает ошибка, печатаем ее.
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "Ошибка соединения.");
                }
                // Перемещаем разрешение в задачу и уничтожаем ее после завершения.
                // Это возвращает разрешение семафору.
                drop(permit);
            });
        }
    }

    /// Принимает входящее соединение.
    ///
    /// Ошибки обрабатываются путем новых попыток установить соединение. Используется
    /// стратегия экспоненциальной задержки. После первого провала задача ждет 1 секунду.
    /// После второго провала задача ждет 2 секунды. Каждый последующий провал удваивает
    /// задержку. Если попытка проваливается в шестой раз после 64 секунд ожидания,
    /// функция возвращает ошибку.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Пытаемся установить соединение несколько раз.
        loop {
            // Выполняем операцию установки соединения. Если сокет принят,
            // возвращаем его. Иначе, сохраняем ошибку.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Возвращаем ошибку.
                        return Err(err.into());
                    }
                }
            }

            // Ставим выполнение на паузу в течение задержки.
            time::sleep(Duration::from_secs(backoff)).await;

            // Удваиваем задержку.
            backoff *= 2;
        }
    }
}

impl Handler {
    /// Обрабатывает соединение.
    ///
    /// Кадры запроса читаются из сокета и обрабатываются. Ответы
    /// записываются обратно в сокет.
    ///
    /// Конвейер не поддерживается. Конвейер позволяет обрабатывать
    /// несколько запросов параллельно. См:
    /// https://redis.io/topics/pipelining
    ///
    /// При получении сигнала о закрытии, соединение обрабатывается до
    /// безопасного состояния, после чего прерывается.
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        // Пока не получен сигнал о закрытии, пытаемся читать
        // новый кадр из запроса.
        while !self.shutdown.is_shutdown() {
            // Во время чтения кадра запроса регистрируем сигнал о закрытии.
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    // Если получен сигнал о закрытии, возвращаемся из `run()`.
                    // Это приводит к закрытию задачи.
                    return Ok(());
                }
            };

            // Если из `read_frame()` вернулось `None`, значит клиент закрыл
            // сокет. Работы больше нет и задача может быть закрыта.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            // Преобразуем кадр `Redis` в структуру команды. Если кадр
            // не является валидной командой `Redis` или является
            // неподдерживаемой командой, возвращается ошибка.
            let cmd = Command::from_frame(frame)?;

            // Печатаем объект `cmd`. Используемый здесь синтаксис - это сокращение,
            // предоставляемое крейтом `tracing`. Полная запись выглядит так:
            //
            // ```
            // debug!(cmd = format!("{:?}", cmd));
            // ```
            //
            // `tracing` предоставляет структурированное логирование, поэтому информация печатается
            // в виде пар "ключ-значение".
            debug!(?cmd);

            // Выполняем работу, необходимую для применения команды. Это может приводить к
            // мутированию состояния БД.
            //
            // Соединение передается в функцию `apply`, что позволяет
            // команде писать ответ прямо в соединение. В случае
            // pub/sub клиенту может быть отправлено несколько кадров.
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
                .await?;
        }

        Ok(())
    }
}