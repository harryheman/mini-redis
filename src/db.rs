use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// Обертка над экземпляром `Db`. Это необходимо для упорядоченной очистки
/// `Db` путем указания фоновой задаче очистки (purge task) закрыться при
/// уничтожении (drop) структуры.
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// Экземпляр `Db`, который будет закрыт, когда эта структура будет уничтожена.
    db: Db,
}

/// Состояние сервера, распределяемое между всеми соединениями.
///
/// `Db` содержит `HashMap`, хранящую данные в форме "ключ-значение" и все
/// значения `broadcast::Sender` для активных каналов pub/sub (издатель/подписчик)
///
/// Экземпляр `Db` - это обработчик общего состояния. Клонирование `Db` является поверхностным и
/// приводит лишь к атомарному увеличению счетчика.
///
/// При создании значения `Db` порождается (spawn) фоновая задача. Эта задача
/// используется для уничтожения (expire) значений после истечения определенного времени. Задача
/// запускается до тех пор, пока все экземпляры `Db` не будут уничтожены, после чего задача
/// прерывается (terminates).
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Обработчик общего состояния. Фоновая задача также будет иметь
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// Общее состояние защищено мьютексом (mutex). Это `std::sync::Mutex`, а не мьютекс Tokio.
    /// Это связано с тем, что во время удержания (holding) мьютекса не выполняется асинхронных операций. Кроме того, критические
    /// разделы являются очень маленькими.
    ///
    /// Мьютекс Tokio нужен, когда блокировки (locks) должны удерживаться
    /// между разными вызовами `.await`. Другие случаи обычно лучше обрабатываются
    ///  стандартным мьютексом. Если критический раздел не содержит
    /// асинхронных операций, но является длинным (нагружает ЦП или выполняет блокирующие
    /// операции), тогда вся операция, включая ожидание мьютекса,
    /// считается "блокирующей". В этом случае должен использоваться
    /// `tokio::task::spawn_blocking`.
    state: Mutex<State>,

    /// Уведомляет фоновую задачу, обрабатывающую истечение времени жизни сущности.
    /// Фоновая задача ждет уведомления, затем проверяет время жизни значений или наличие сигнала о закрытии.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// Данные ключ-значение. Мы не пытаемся делать ничего сложного, поэтому
    /// для хранения значений нам подойдет `std::collections::HashMap`.
    entries: HashMap<String, Entry>,

    /// Пространство ключей (key space) pub/sub. `Redis` использует отдельное пространство ключей для данных
    /// и pub/sub. `mini-redis` использует отдельную `HashMap` для pub/sub.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Времена жизни.
    ///
    /// `BTreeSet` используется для хранения времен жизни, отсортированных по времени их истечения.
    /// Это позволяет фоновой задаче перебирать эту карту для определения
    /// следующего истекающего значения.
    ///
    /// Маловероятно, но возможно, что для одного экземпляра будет создано
    /// несколько времен жизни. Поэтому в качестве уникального ключа
    /// используется `String`, а не `Instant`.
    expirations: BTreeSet<(Instant, String)>,

    /// `true`, когда экземпляр `Db` закрыт. Это происходит, когда все
    /// значения `Db` уничтожены. Установка этого поля в значение `true`
    /// указывает фоновым задачам закрыться.
    shutdown: bool,
}

/// Сущность хранилища ключ-значение.
#[derive(Debug)]
struct Entry {
    /// Хранящиеся данные.
    data: Bytes,

    /// Момент (instant) истечения времени жизни сущности, после которого
    /// она удаляется из БД.
    expires_at: Option<Instant>,
}

impl DbDropGuard {
    /// Создает новый `DbDropGuard`, оборачивающий экземпляр `Db`.
    /// Когда он уничтожается, задача очистки `Db` закрывается.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Возвращает общую БД. Внутри это `Arc`,
    /// поэтому его клонирование лишь увеличивает количество ссылок.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Указывает экземпляру `Db` закрыть задачу, очищающую истекшие ключи.
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Создает новый пустой экземпляр `Db`. Выделяет (allocate) общее состояние и создает (spawn)
    /// фоновую задачу для управления истечением ключей.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Запускает фоновую задачу.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db { shared }
    }

    /// Возвращает значение по ключу.
    ///
    /// При отсутствии значения возвращается `None`. Это может произойти,
    /// если значение не присваивалось или истекло.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Выполняем блокировку (acquire the lock), получаем сущность и клонируем значение.
        //
        // Поскольку данные хранятся с помощью `Bytes`, клонирование является
        // поверхностным. Данные не копируются.
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Устанавливает значение по ключу и, опционально, время его жизни.
    ///
    /// Если значение уже установлено, оно удаляется.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // Если этот `set` становится следующим истекающим ключом, фоновая задача
        // должна узнать об этом для обновления своего состояния.
        //
        // Должна ли задача быть уведомлена, вычисляется в теле этого метода.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant`, когда истекает время жизни ключа.
            let when = Instant::now() + duration;

            // "Воркер" задачи уведомляется, только если добавленное время жизни
            // является следующим истекающим ключом. В этом случае воркер
            // должен быть "разбужен" для обновления своего состояния.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // Добавляем новую сущность в `HashMap`.
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // Если по ключу имеется значение и у него есть время жизни. Соответствующая сущность в карте
        // `expirations` также должна быть удалена. Это предотвращает утечку данных.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // Удаляем время жизни.
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // Отслеживаем время жизни. Добавление сущности перед удалением может привести к багу,
        // когда текущий `(when, key)` будет равен предыдущему `(when, key)`.
        // Удаление перед добавлением решает эту проблему.
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // Освобождаем (release) мьютекс перед уведомлением фоновой задачи. Это позволяет
        // предотвратить ситуацию, когда фоновая задача не может блокировать мьютекс, поскольку он удерживается этой функцией.
        drop(state);

        if notify {
            // Уведомляем фоновую задачу, только если ей необходимо обновить
            // свое состояние для отражения нового времени жизни.
            self.shared.background_task.notify_one();
        }
    }

    /// Возвращает `Receiver` для запрошенного канала.
    ///
    /// Этот `Receiver` используется для получения значений, отправленных с помощью команды `PUBLISH`.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Блокируем мьютекс.
        let mut state = self.shared.state.lock().unwrap();

        // Если для запрошенного канала нет сущности, создаем новый
        // широковещательный (broadcast) канал и связываем его с ключом. Если канал существует,
        // возвращаем соответствующего получателя.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // Широковещательный канал отсутствует, создаем его.
                //
                // Канал создается с емкостью `1024` сообщений.
                // Сообщение хранится в канале до тех пор, пока все подписчики
                // его не увидят. Это означает, что наличие "медленного" подписчика может привести к
                // бесконечно долгому хранению сообщения.
                //
                // При заполнении емкости канала, публикация будет приводить к
                // уничтожению старых сообщений. Это решает проблему блокировки
                // всей системы медленными потребителями.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Публикует сообщение в канале. Возвращает количество подписчиков,
    /// "слушающих" канал.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            // При успешной отправке сообщения в широковещательный канал, возвращается
            // количество подписчиков. Ошибка указывает на отсутствие
            // получателей. В этом случае должен возвращаться `0`.
            .map(|tx| tx.send(value).unwrap_or(0))
            // Если по ключу канала нет сущности, значит нет и
            // подписчиков. В этом случае возвращается `0`.
            .unwrap_or(0)
    }

    /// Указывает фоновой задаче очистки закрыться. Это вызывается
    /// реализацией `Drop` `DbShutdown`
    fn shutdown_purge_task(&self) {
        // Фоновая задача должна получить сигнал о закрытии. Это делается путем
        // установки `State::shutdown` в значение `true`.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // Снимаем блокировку перед уведомлением фоновой задачи. Это позволяет
        // предотвратить ситуацию, когда фоновая задача не может блокировать мьютекс.
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Очищает все истекшие ключи и возвращает `Instant`, когда истечет
    /// следующий ключ. Фоновая задача "спит" до этого момента.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // БД закрывается. Все обработчики общего состояния
            // уничтожены. Фоновая задача должна завершиться.
            return None;
        }

        // Это нужно для того, чтобы сделать "счастливым" контроллера заимствований (borrow checker). Если коротко,
        // `lock()` возвращает `MutexGuard`, а не `&mut State`. Контроллер заимствований
        // не может видеть "сквозь" защитника мьютекса и определить, что
        // мутабельный доступ к `state.expirations` и `state.entries` является безопасным,
        // поэтому мы получаем "настоящую" мутабельную ссылку на `State` за пределами цикла.
        let state = &mut *state;

        // Находим все ключи, истекшие до настоящего времени.
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                // Выполняем очистку. `when` - это момент, когда истекает
                // следующий ключ. Воркер задачи ждет этого момента.
                return Some(when);
            }

            // Ключ истек, удаляем его.
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// Возвращает `true`, если БД закрыта.
    ///
    /// Флаг `shutdown` устанавливается в значение `true`, когда все значения `Db` уничтожаются,
    /// что означает недоступность общего состояния.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Работа, выполняемая фоновой задачей.
///
/// Ждет уведомления. При получении уведомления, очищает все истекшие ключи
/// из обработчика общего состояния. Если установлен `shutdown`, задача прерывается.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // Если флаг `shutdown` имеет значение `true`, задача должна быть закрыта.
    while !shared.is_shutdown() {
        // Очищаем все истекшие ключи. Функция возвращает момент, когда
        // истечет следующий ключ. Воркер ждет этого момента, затем снова выполняет очистку.
        if let Some(when) = shared.purge_expired_keys() {
            // Ждем, когда истечет следующий ключ или когда фоновая задача получит
            // уведомление. При получении уведомления, фоновая задача должна перезагрузить свое состояние. Это делается в цикле.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // Истекших ключей больше не будет. Ждем уведомления задачи.
            shared.background_task.notified().await;
        }
    }

    debug!("Фоновая задача очистки закрыта.")
}
