//! The SQLite broker: a Cabby addition (deviation D-065 in `compat/deviations.md`
//! describes its observable behavior).
//!
//! The memory broker stays the live path: it serves every read, assigns
//! offsets and fans out. Beside it, a bounded write queue (`queue`) feeds one
//! persister thread (`persister`) that writes batches to a WAL-mode SQLite
//! file (`store`); at startup the loader (`loader`) fills the memory broker
//! from the file before Cabby accepts connections. A graceful shutdown writes
//! the last batch and marks the file clean, so the next start keeps the
//! epoch; after a crash the next start rotates it.
//!
//! Units: history timestamps and stream deadlines are unix seconds, session
//! expiries unix milliseconds (`units`).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::{Broadcaster, Broker, BrokerError, Cacheable, Clock, Config, Memory, PresenceInfoOptions, Presenter, lock};
use crate::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use crate::logger::Logger;
use crate::{log_error, log_warn};
use persister::{Persister, Timing};
use queue::{WriteOp, WriteQueue};

pub use loader::{LoadReport, Previous};
pub use queue::OpKind;

mod files;
mod loader;
mod persister;
mod queue;
mod store;
mod units;

/// The broker's logger (`context=broker provider=sqlite`).
fn log() -> &'static Logger {
    static LOG: OnceLock<Logger> = OnceLock::new();
    LOG.get_or_init(|| Logger::default().with("context", "broker").with("provider", "sqlite"))
}

/// Where the broker reports its write path; the server turns these into
/// metrics. Calls come from the broadcast path and the persister
/// thread, so they must be cheap.
pub trait SqliteObserver: Send + Sync {
    /// Ops waiting to be written, after each write attempt.
    fn queue_size(&self, ops: usize);
    /// A batch was written in `took`.
    fn batch_written(&self, took: Duration);
    /// A batch write failed; the batch is kept and retried.
    fn write_failed(&self);
    /// An op of `kind` was dropped because the queue was full.
    fn dropped(&self, kind: OpKind);
}

/// Reports nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

impl SqliteObserver for NoopObserver {
    fn queue_size(&self, _ops: usize) {}
    fn batch_written(&self, _took: Duration) {}
    fn write_failed(&self) {}
    fn dropped(&self, _kind: OpKind) {}
}

/// How long opening waits for another process to release the database.
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
/// The write queue's op-count bound.
pub const QUEUE_CAPACITY: usize = 100_000;
/// The write queue's payload byte bound (64 MiB): the most broadcast and
/// session data that waits in memory while disk writes are slow.
pub const QUEUE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Where the database lives and how the broker treats it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteOptions {
    pub path: PathBuf,
    pub lock_timeout: Duration,
    pub queue_capacity: usize,
    pub queue_max_bytes: usize,
}

impl SqliteOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            lock_timeout: LOCK_TIMEOUT,
            queue_capacity: QUEUE_CAPACITY,
            queue_max_bytes: QUEUE_MAX_BYTES,
        }
    }
}

/// What `Broker::shutdown` gives the final write when no runner deadline
/// applies (the default `shutdown_timeout`).
pub const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);
/// How long `shutdown_within` waits past its budget for the writer to close
/// the file.
const FINISH_GRACE: Duration = Duration::from_secs(1);

/// The memory broker plus a write-behind SQLite copy (see the module docs).
pub struct SqliteBroker {
    memory: Memory,
    config: Config,
    path: PathBuf,
    queue: Arc<WriteQueue>,
    observer: Arc<dyn SqliteObserver>,
    report: LoadReport,
    writer: Mutex<Writer>,
}

enum Writer {
    /// Loaded; the thread is not started yet.
    Ready(Box<Persister>),
    /// The thread runs and reports whether its final flush marked the
    /// shutdown clean; nothing is sent if it panicked.
    Running {
        done: Receiver<bool>,
        thread: JoinHandle<()>,
    },
    Stopped,
}

/// How [`SqliteBroker::stop`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// The final write went through and marked the shutdown clean.
    Clean,
    /// The final write failed, ran out of time or history was dropped.
    Unclean,
    /// The writer did not report back within the budget plus the grace.
    TimedOut,
    /// The writer thread panicked earlier.
    WriterDied,
    /// A second call.
    AlreadyStopped,
}

impl SqliteBroker {
    /// Opens (creating if needed) the database at `options.path`, takes its
    /// lock and loads it into a new memory broker. Errors name the path:
    /// another process holds it, it can't be created or read, or its schema
    /// is newer. A corrupt file is moved aside instead.
    pub fn open(
        broadcaster: Option<Arc<dyn Broadcaster>>,
        presenter: Option<Arc<dyn Presenter>>,
        config: &Config,
        options: &SqliteOptions,
        clock: Arc<dyn Clock>,
        observer: Arc<dyn SqliteObserver>,
    ) -> Result<Self, BrokerError> {
        let memory = Memory::new(broadcaster, presenter, config, clock.clone());
        let loaded = loader::load(&memory, config, options, clock.as_ref())?;
        let queue = Arc::new(WriteQueue::with_max_bytes(
            options.queue_capacity,
            options.queue_max_bytes,
            persister::BATCH_SIZE,
        ));
        let persister = Persister::new(loaded.db, queue.clone(), config, clock, observer.clone(), Timing::standard());
        Ok(Self {
            memory,
            config: config.clone(),
            path: options.path.clone(),
            queue,
            observer,
            report: loaded.report,
            writer: Mutex::new(Writer::Ready(Box::new(persister))),
        })
    }

    /// What `open` restored.
    pub fn report(&self) -> &LoadReport {
        &self.report
    }

    pub fn epoch(&self) -> String {
        self.memory.epoch()
    }

    /// Stops the broker: the writer writes everything still queued and marks
    /// the shutdown clean if that finishes within `budget`, then checkpoints,
    /// closes the database and releases the lock. Returns whether the shutdown
    /// was marked clean (false for a second call).
    pub fn shutdown_within(&self, budget: Duration) -> bool {
        self.stop(budget) == Ending::Clean
    }

    /// [`SqliteBroker::shutdown_within`], saying how the writer ended.
    fn stop(&self, budget: Duration) -> Ending {
        let _ = Broker::shutdown(&self.memory);
        let deadline = Instant::now() + budget;
        self.queue.close(deadline);
        let path = self.path.display().to_string();
        let ended = |clean: bool| if clean { Ending::Clean } else { Ending::Unclean };
        match std::mem::replace(&mut *lock(&self.writer), Writer::Stopped) {
            Writer::Stopped => Ending::AlreadyStopped,
            Writer::Ready(persister) => ended(persister.finish(deadline)),
            Writer::Running { done, thread } => match done.recv_timeout(budget + FINISH_GRACE) {
                Ok(clean) => {
                    let _ = thread.join();
                    ended(clean)
                }
                Err(RecvTimeoutError::Timeout) => {
                    log_warn!(log(), &format!("the SQLite writer did not finish within {} ms; the next start uses a new epoch",
                        (budget + FINISH_GRACE).as_millis()), "path" => path);
                    Ending::TimedOut
                }
                Err(RecvTimeoutError::Disconnected) => {
                    let _ = thread.join();
                    log_warn!(log(), "the SQLite writer had stopped after a panic; the next start uses a new epoch",
                        "path" => path);
                    Ending::WriterDied
                }
            },
        }
    }

    fn enqueue(&self, op: WriteOp) {
        self.queue.push_with_drops(op, |kind| self.observer.dropped(kind));
    }
}

impl Drop for SqliteBroker {
    /// Without `shutdown` (a failed startup, a test), the writer stops at once,
    /// as after a crash: nothing more is written and the next start rotates
    /// the epoch.
    fn drop(&mut self) {
        self.queue.close(Instant::now());
    }
}

impl Broker for SqliteBroker {
    /// Starts the memory broker's expiration loop and the writer thread.
    fn start(&self) -> Result<(), BrokerError> {
        let mut writer = lock(&self.writer);
        let persister = match std::mem::replace(&mut *writer, Writer::Stopped) {
            Writer::Ready(persister) => persister,
            other => {
                *writer = other;
                return Ok(());
            }
        };
        Broker::start(&self.memory)?;
        let (done, finished) = mpsc::channel();
        let path = self.path.display().to_string();
        let thread = std::thread::Builder::new()
            .name("cabby-sqlite".into())
            .spawn(move || match catch_unwind(AssertUnwindSafe(|| persister.run())) {
                Ok(clean) => {
                    let _ = done.send(clean);
                }
                Err(panic) => {
                    let why = panic
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_owned())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_owned());
                    log_error!(log(), "the SQLite writer panicked; nothing more is written to the database",
                        "path" => path, "error" => why);
                }
            })
            .map_err(|e| BrokerError { message: format!("failed to start the SQLite writer: {e}") })?;
        *writer = Writer::Running { done: finished, thread };
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.shutdown_within(SHUTDOWN_BUDGET);
        Ok(())
    }

    fn announce(&self) -> String {
        let c = &self.config;
        format!(
            "Using SQLite broker: {} (epoch: {}, history limit: {}, history ttl: {}s, sessions ttl: {}s, presence ttl: {}s)",
            self.path.display(),
            self.memory.epoch(),
            c.history_limit,
            c.history_ttl,
            c.sessions_ttl,
            c.presence_ttl,
        )
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        if let Some(recorded) = self.memory.handle_broadcast_recorded(msg)? {
            self.enqueue(WriteOp::History {
                stream: msg.stream.clone(),
                offset: recorded.offset,
                ts_secs: recorded.timestamp,
                data: msg.data.clone(),
            });
        }
        Ok(())
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.memory.handle_command(msg)
    }

    fn subscribe(&self, stream: &str) -> String {
        self.memory.subscribe(stream)
    }

    fn unsubscribe(&self, stream: &str) -> String {
        self.memory.unsubscribe(stream)
    }

    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.memory.history_from(stream, epoch, offset)
    }

    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.memory.history_since(stream, ts)
    }

    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let data = session.to_cache_entry()?;
        let expires_at_ms = self.memory.commit_session_data(sid, data.clone());
        self.enqueue(WriteOp::SessionCommit { sid: sid.to_owned(), data, expires_at_ms });
        Ok(())
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        self.memory.restore_session(sid)
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        if let Some(expires_at_ms) = self.memory.touch_session_deadline(sid) {
            self.enqueue(WriteOp::SessionTouch { sid: sid.to_owned(), expires_at_ms });
        }
        Ok(())
    }

    /// Presence lives in memory only: every connection drops on restart.
    fn supports_presence(&self) -> bool {
        self.memory.supports_presence()
    }

    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        self.memory.presence_add(stream, sid, pid, info)
    }

    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        self.memory.presence_remove(stream, sid)
    }

    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        self.memory.presence_info(stream, opts)
    }

    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        self.memory.touch_presence(sid)
    }

    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.memory.peak(stream)
    }
}

/// Helpers shared by the module's tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::SqliteObserver;
    use super::queue::{OpKind, WriteOp};
    use crate::broker::lock;
    use crate::broker::test_clock::T0;

    /// A history op at the test clock's start.
    pub(crate) fn history(stream: &str, offset: u64) -> WriteOp {
        history_at(stream, offset, T0)
    }

    pub(crate) fn history_at(stream: &str, offset: u64, ts_secs: i64) -> WriteOp {
        WriteOp::History { stream: stream.into(), offset, ts_secs, data: format!("m{offset}") }
    }

    /// A session commit whose data is the session ID's bytes.
    pub(crate) fn commit(sid: &str, expires_at_ms: i64) -> WriteOp {
        WriteOp::SessionCommit { sid: sid.into(), data: sid.as_bytes().to_vec(), expires_at_ms }
    }

    pub(crate) fn touch(sid: &str, expires_at_ms: i64) -> WriteOp {
        WriteOp::SessionTouch { sid: sid.into(), expires_at_ms }
    }

    /// A fresh directory under the system temp dir, removed on drop.
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new() -> Self {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "cabby-sqlite-test-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::SeqCst)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("creating a temp dir");
            Self(dir)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }

        /// A file directly in the directory.
        pub(crate) fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }

        /// A database path whose parent directory does not exist yet.
        pub(crate) fn db(&self) -> PathBuf {
            self.0.join("data").join("cabby.db")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Records what the broker reports.
    #[derive(Default)]
    pub(crate) struct Recording {
        queue_sizes: Mutex<Vec<usize>>,
        batches: Mutex<u64>,
        failures: Mutex<u64>,
        dropped: Mutex<Vec<OpKind>>,
    }

    impl Recording {
        pub(crate) fn queue_sizes(&self) -> Vec<usize> {
            lock(&self.queue_sizes).clone()
        }

        pub(crate) fn batches(&self) -> u64 {
            *lock(&self.batches)
        }

        pub(crate) fn failures(&self) -> u64 {
            *lock(&self.failures)
        }

        pub(crate) fn dropped(&self) -> Vec<OpKind> {
            lock(&self.dropped).clone()
        }
    }

    impl SqliteObserver for Recording {
        fn queue_size(&self, ops: usize) {
            lock(&self.queue_sizes).push(ops);
        }

        fn batch_written(&self, _took: Duration) {
            *lock(&self.batches) += 1;
        }

        fn write_failed(&self) {
            *lock(&self.failures) += 1;
        }

        fn dropped(&self, kind: OpKind) {
            lock(&self.dropped).push(kind);
        }
    }

    /// Session state for `commit_session`.
    pub(crate) struct Bytes(pub(crate) Vec<u8>);

    impl crate::broker::Cacheable for Bytes {
        fn to_cache_entry(&self) -> Result<Vec<u8>, crate::broker::BrokerError> {
            Ok(self.0.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{Bytes, Recording, TempDir};
    use super::*;
    use crate::broker::test_clock::{T0, TestClock};
    use crate::common::StreamMessageMetadata;

    fn msg(stream: &str, data: &str, transient: bool) -> StreamMessage {
        StreamMessage {
            stream: stream.into(),
            data: data.into(),
            meta: transient.then(|| StreamMessageMetadata { transient: true, ..Default::default() }),
            ..Default::default()
        }
    }

    fn open(dir: &TempDir, clock: &Arc<TestClock>) -> SqliteBroker {
        SqliteBroker::open(
            None,
            None,
            &Config::new(),
            &SqliteOptions::new(dir.db()),
            clock.clone(),
            Arc::new(NoopObserver),
        )
        .unwrap()
    }

    fn history(broker: &SqliteBroker, stream: &str) -> Vec<(u64, String)> {
        broker.history_since(stream, 0).unwrap().unwrap_or_default().into_iter().map(|m| (m.offset, m.data)).collect()
    }

    #[test]
    fn a_graceful_restart_restores_history_offsets_sessions_and_the_epoch() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        let epoch = broker.epoch();
        for data in ["a", "b", "c"] {
            broker.handle_broadcast(&msg("s", data, false)).unwrap();
        }
        broker.handle_broadcast(&msg("s", "transient", true)).unwrap();
        broker.commit_session("sid", &Bytes(b"state".to_vec())).unwrap();
        assert!(broker.shutdown_within(Duration::from_secs(5)), "marked clean");
        assert!(!files::sibling(&dir.db(), "-wal").exists(), "no WAL left after a graceful shutdown");
        drop(broker);

        let broker = open(&dir, &clock);
        assert_eq!(epoch, broker.epoch());
        assert_eq!(Previous::Clean, broker.report().previous);
        let expected: Vec<(u64, String)> = vec![(1, "a".into()), (2, "b".into()), (3, "c".into())];
        assert_eq!(expected, history(&broker, "s"), "transient broadcasts are not persisted");
        assert_eq!(2, broker.history_from("s", &epoch, 1).unwrap().len(), "resumable by offset");
        assert_eq!(Some(b"state".to_vec()), broker.restore_session("sid").unwrap());
        broker.start().unwrap();
        broker.handle_broadcast(&msg("s", "d", false)).unwrap();
        assert_eq!(Some(4), broker.peak("s").unwrap().map(|m| m.offset));
        assert!(broker.shutdown_within(Duration::from_secs(5)));
    }

    #[test]
    fn a_session_touch_reaches_the_file_with_its_new_expiry() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        broker.commit_session("sid", &Bytes(b"state".to_vec())).unwrap();
        clock.advance(Duration::from_secs(5));
        broker.touch_session("sid").unwrap();
        broker.touch_session("unknown").unwrap();
        assert!(broker.shutdown_within(Duration::from_secs(5)));
        drop(broker);

        let conn = store::open(&dir.db()).unwrap();
        let expiry: i64 =
            conn.query_row("SELECT expires_at_ms FROM sessions WHERE sid = 'sid'", [], |r| r.get(0)).unwrap();
        assert_eq!((T0 + 5) * 1000 + 300_000, expiry);
        let rows: i64 = conn.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0)).unwrap();
        assert_eq!(1, rows, "a touch for an unknown session writes nothing");
    }

    #[test]
    fn dropping_the_broker_without_shutdown_rotates_the_epoch() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        let epoch = broker.epoch();
        drop(broker);

        // Waits (up to LOCK_TIMEOUT) for the old writer to release the lock.
        let broker = open(&dir, &clock);
        assert_ne!(epoch, broker.epoch());
        assert_eq!(Previous::Unclean, broker.report().previous);
    }

    #[test]
    fn a_shutdown_without_start_still_writes_and_marks_it_clean() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.handle_broadcast(&msg("s", "a", false)).unwrap();

        assert!(broker.shutdown_within(Duration::from_secs(5)));
        assert!(!broker.shutdown_within(Duration::from_secs(5)), "a second shutdown does nothing");
        drop(broker);

        let broker = open(&dir, &clock);
        assert_eq!(Previous::Clean, broker.report().previous);
        assert_eq!(vec![(1, "a".to_owned())], history(&broker, "s"));
    }

    #[test]
    fn queue_overflow_is_reported_to_the_observer() {
        let dir = TempDir::new();
        let observer = Arc::new(Recording::default());
        let mut options = SqliteOptions::new(dir.db());
        options.queue_capacity = 2;
        let broker =
            SqliteBroker::open(None, None, &Config::new(), &options, TestClock::new(), observer.clone()).unwrap();

        // Not started: nothing drains the queue.
        broker.commit_session("a", &Bytes(b"a".to_vec())).unwrap();
        broker.handle_broadcast(&msg("s", "x", false)).unwrap();
        broker.handle_broadcast(&msg("s", "y", false)).unwrap();
        broker.touch_session("a").unwrap();

        assert_eq!(vec![OpKind::History, OpKind::History], observer.dropped());
        assert_eq!(2, broker.history_since("s", 0).unwrap().unwrap_or_default().len(), "live history is unaffected");
    }

    #[test]
    fn announce_names_the_file_and_the_settings() {
        let dir = TempDir::new();
        let broker = open(&dir, &TestClock::new());
        assert_eq!(
            format!(
                "Using SQLite broker: {} (epoch: {}, history limit: 100, history ttl: 300s, sessions ttl: 300s, presence ttl: 15s)",
                dir.db().display(),
                broker.epoch()
            ),
            broker.announce()
        );
        assert!(broker.supports_presence());
    }

    #[test]
    fn dropped_history_rotates_the_epoch_and_keeps_the_streams_offset() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let mut options = SqliteOptions::new(dir.db());
        options.queue_capacity = 2;
        let broker =
            SqliteBroker::open(None, None, &Config::new(), &options, clock.clone(), Arc::new(NoopObserver)).unwrap();
        let epoch = broker.epoch();
        // Not started, so the writer is stalled: loud's broadcasts push quiet's 1-3 out of the queue.
        for data in ["q1", "q2", "q3"] {
            broker.handle_broadcast(&msg("quiet", data, false)).unwrap();
        }
        for data in ["l1", "l2"] {
            broker.handle_broadcast(&msg("loud", data, false)).unwrap();
        }
        assert!(!broker.shutdown_within(Duration::from_secs(5)), "not marked clean after dropping history");
        drop(broker);

        let broker = open(&dir, &clock);
        assert_eq!(Previous::Unclean, broker.report().previous);
        assert_ne!(epoch, broker.epoch(), "clients that saw quiet 1-3 must resync");
        assert_eq!(Some(vec![]), broker.history_since("quiet", 0).unwrap(), "quiet loads empty at its offset");
        assert_eq!(vec![(1, "l1".to_owned()), (2, "l2".to_owned())], history(&broker, "loud"));
        broker.handle_broadcast(&msg("quiet", "q4", false)).unwrap();
        assert_eq!(Some(4), broker.peak("quiet").unwrap().map(|m| m.offset), "the dropped offsets are not reused");
        assert!(broker.shutdown_within(Duration::from_secs(5)));
    }

    #[test]
    fn a_writer_stuck_past_the_budget_times_out() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        // Another connection holds SQLite's write lock: the writer's next batch
        // waits out its 5 s busy timeout, longer than the budget plus the grace.
        let blocker = rusqlite::Connection::open(dir.db()).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        broker.handle_broadcast(&msg("s", "a", false)).unwrap();

        let started = Instant::now();
        assert_eq!(Ending::TimedOut, broker.stop(Duration::ZERO));
        let waited = started.elapsed();
        assert!(waited >= FINISH_GRACE && waited < Duration::from_secs(4), "waited {waited:?}");
        blocker.execute_batch("ROLLBACK").unwrap();
        assert_eq!(Ending::AlreadyStopped, broker.stop(Duration::ZERO));
    }

    #[test]
    fn a_writer_that_panicked_is_reported_as_such() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        if let Writer::Ready(persister) = &mut *lock(&broker.writer) {
            persister.panic_on_tick = true;
        }
        broker.start().unwrap();
        // Waits for the panic (its first tick), so stop() can't race it.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !matches!(&*lock(&broker.writer), Writer::Running { thread, .. } if thread.is_finished()) {
            assert!(Instant::now() < deadline, "the writer did not panic");
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(Ending::WriterDied, broker.stop(Duration::from_secs(5)));
        drop(broker);
        assert_eq!(Previous::Unclean, open(&dir, &clock).report().previous);
    }

    #[test]
    fn a_graceful_stop_reports_clean() {
        let dir = TempDir::new();
        let broker = open(&dir, &TestClock::new());
        broker.start().unwrap();
        assert_eq!(Ending::Clean, broker.stop(Duration::from_secs(5)));
    }

    #[test]
    fn broadcasts_after_shutdown_are_counted_as_dropped() {
        let dir = TempDir::new();
        let observer = Arc::new(Recording::default());
        let broker = SqliteBroker::open(
            None,
            None,
            &Config::new(),
            &SqliteOptions::new(dir.db()),
            TestClock::new(),
            observer.clone(),
        )
        .unwrap();
        assert!(broker.shutdown_within(Duration::from_secs(5)));

        broker.handle_broadcast(&msg("s", "late", false)).unwrap();
        broker.commit_session("sid", &Bytes(b"late".to_vec())).unwrap();

        assert_eq!(vec![OpKind::History, OpKind::SessionCommit], observer.dropped());
    }

    #[test]
    fn concurrent_broadcasts_across_streams_restart_contiguous_at_their_live_offsets() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        let streams = ["a", "b", "c"];
        std::thread::scope(|s| {
            for worker in 0..4 {
                let broker = &broker;
                s.spawn(move || {
                    for i in 0..150 {
                        let stream = streams[(worker + i) % streams.len()];
                        broker.handle_broadcast(&msg(stream, &format!("{worker}-{i}"), false)).unwrap();
                    }
                });
            }
        });
        let live: Vec<u64> = streams.iter().map(|s| broker.peak(s).unwrap().unwrap().offset).collect();
        let tails: Vec<Vec<(u64, String)>> = streams.iter().map(|s| history(&broker, s)).collect();
        assert_eq!(600, live.iter().sum::<u64>());
        assert!(broker.shutdown_within(Duration::from_secs(5)));
        drop(broker);

        let broker = open(&dir, &clock);
        assert_eq!(Previous::Clean, broker.report().previous);
        for ((stream, live), tail) in streams.iter().zip(&live).zip(&tails) {
            let restored = history(&broker, stream);
            assert_eq!(tail, &restored, "{stream}: the restored tail is the live one");
            let offsets: Vec<u64> = restored.iter().map(|(o, _)| *o).collect();
            let expected: Vec<u64> = (live + 1 - offsets.len() as u64..=*live).collect();
            assert_eq!(expected, offsets, "{stream}: contiguous up to its live offset");
            assert_eq!(Some(*live), broker.peak(stream).unwrap().map(|m| m.offset));
        }
        assert!(broker.shutdown_within(Duration::from_secs(5)));
    }
}
