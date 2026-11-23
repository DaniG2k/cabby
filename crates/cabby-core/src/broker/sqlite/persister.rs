//! The writer thread (spec §3, §5, §6).
//!
//! Live delivery never waits for the disk: a failing disk only fills the
//! bounded queue (which then drops history first) and the log (the first
//! failure, then at most one line per interval, then the recovery).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use super::queue::{WriteOp, WriteQueue};
use super::store::{self, Touched};
use super::{SqliteObserver, log, units};
use crate::broker::{Clock, Config};
use crate::{log_error, log_info, log_warn};

/// A batch is written as soon as this many ops are queued (spec §3).
pub(super) const BATCH_SIZE: usize = 1_000;
/// Pause between attempts of the final write.
const FINAL_RETRY: Duration = Duration::from_millis(100);

/// The open database, its lock file and its path, owned by the persister.
pub(super) struct Database {
    pub(super) conn: Connection,
    pub(super) lock: File,
    pub(super) path: PathBuf,
}

/// The persister's intervals (spec §3, §6), fixed in production.
#[derive(Debug, Clone, Copy)]
pub(super) struct Timing {
    /// A partial batch is written after this long.
    pub(super) flush_interval: Duration,
    pub(super) prune_interval: Duration,
    /// At most one log line per interval while writes keep failing.
    pub(super) error_log_interval: Duration,
}

impl Timing {
    pub(super) fn standard() -> Self {
        Self {
            flush_interval: Duration::from_millis(50),
            prune_interval: Duration::from_secs(30),
            error_log_interval: Duration::from_secs(10),
        }
    }
}

/// Rate-limits write-failure logs: the first failure of a run, then at most one
/// per `interval`; the recovery once.
#[derive(Debug)]
pub(super) struct ErrorLog {
    interval: Duration,
    failing: bool,
    last_logged: Option<Instant>,
}

impl ErrorLog {
    pub(super) fn new(interval: Duration) -> Self {
        Self { interval, failing: false, last_logged: None }
    }

    /// Records a failure at `now`; true when it should be logged.
    pub(super) fn failed(&mut self, now: Instant) -> bool {
        let log = !self.failing || self.last_logged.is_none_or(|t| now.duration_since(t) >= self.interval);
        self.failing = true;
        if log {
            self.last_logged = Some(now);
        }
        log
    }

    /// Records a success; true when it ends a run of failures.
    pub(super) fn succeeded(&mut self) -> bool {
        std::mem::replace(&mut self.failing, false)
    }
}

pub(super) struct Persister {
    db: Database,
    queue: Arc<WriteQueue>,
    history_ttl: i64,
    history_limit: i64,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn SqliteObserver>,
    timing: Timing,
    /// A batch that failed to write, retried before anything newer.
    pending: Vec<WriteOp>,
    /// Streams written since the last prune.
    touched: Touched,
    last_prune: Instant,
    errors: ErrorLog,
}

impl Persister {
    pub(super) fn new(
        db: Database,
        queue: Arc<WriteQueue>,
        config: &Config,
        clock: Arc<dyn Clock>,
        observer: Arc<dyn SqliteObserver>,
        timing: Timing,
    ) -> Self {
        Self {
            db,
            queue,
            history_ttl: config.history_ttl,
            history_limit: i64::try_from(config.history_limit).unwrap_or(i64::MAX),
            clock,
            observer,
            timing,
            pending: Vec::new(),
            touched: Touched::new(),
            last_prune: Instant::now(),
            errors: ErrorLog::new(timing.error_log_interval),
        }
    }

    /// The thread body: ticks until the queue closes, then the final flush.
    /// Returns whether the shutdown was marked clean.
    pub(super) fn run(mut self) -> bool {
        while self.queue.closed().is_none() {
            self.tick();
        }
        let deadline = self.queue.closed().unwrap_or_else(Instant::now);
        self.finish(deadline)
    }

    /// One step: write the next batch (or retry the failed one after a pause),
    /// report the queue size, and prune when due.
    pub(super) fn tick(&mut self) {
        if self.pending.is_empty() {
            self.pending = self.queue.take_batch(self.timing.flush_interval);
        } else {
            self.queue.pause(self.timing.flush_interval);
        }
        if !self.pending.is_empty() {
            let ops = std::mem::take(&mut self.pending);
            if !self.write(&ops, false) {
                self.pending = ops;
            }
        }
        self.observer.queue_size(self.queue.len() + self.pending.len());
        if self.last_prune.elapsed() >= self.timing.prune_interval {
            self.prune();
        }
    }

    /// One batch; true when it was written.
    fn write(&mut self, ops: &[WriteOp], clean_shutdown: bool) -> bool {
        let started = Instant::now();
        let now_secs = units::now_secs(self.clock.as_ref());
        match store::write_batch(&mut self.db.conn, ops, self.history_ttl, now_secs, clean_shutdown) {
            Ok(touched) => {
                self.observer.batch_written(started.elapsed());
                self.touched.extend(touched);
                if self.errors.succeeded() {
                    log_info!(log(), "SQLite writes recovered", "path" => self.db.path.display().to_string());
                }
                true
            }
            Err(e) => {
                self.observer.write_failed();
                if self.errors.failed(Instant::now()) {
                    log_error!(log(), "failed to write to the SQLite database, retrying",
                        "path" => self.db.path.display().to_string(), "ops" => ops.len(), "error" => e.to_string());
                }
                false
            }
        }
    }

    fn prune(&mut self) {
        self.last_prune = Instant::now();
        let now = self.clock.now();
        let now_secs = units::secs(now);
        let prune = store::Prune {
            history_cutoff: units::history_cutoff(now_secs, self.history_ttl),
            now_secs,
            now_ms: units::millis(now),
            history_limit: self.history_limit,
            streams: &self.touched,
        };
        match store::prune(&mut self.db.conn, &prune) {
            Ok(()) => self.touched.clear(),
            Err(e) => log_warn!(log(), "failed to prune the SQLite database", "error" => e.to_string()),
        }
    }

    /// Writes everything still queued, marking the shutdown clean only if that
    /// succeeds before `deadline`; then checkpoints the WAL, closes the
    /// database and releases the lock. Returns whether it marked it clean.
    pub(super) fn finish(mut self, deadline: Instant) -> bool {
        let mut ops = std::mem::take(&mut self.pending);
        ops.extend(self.queue.take_all());
        let mut clean = false;
        while Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            let _ = self.db.conn.busy_timeout(left.min(store::BUSY_TIMEOUT));
            if self.write(&ops, true) {
                clean = true;
                break;
            }
            std::thread::sleep(FINAL_RETRY.min(deadline.saturating_duration_since(Instant::now())));
        }
        let path = self.db.path.display().to_string();
        if !clean {
            log_warn!(log(), "the final SQLite write did not finish in time; the next start uses a new epoch",
                "path" => path.clone(), "ops" => ops.len());
        }
        let Database { conn, lock, .. } = self.db;
        let _ = conn.busy_timeout(deadline.saturating_duration_since(Instant::now()));
        if let Err(e) = store::checkpoint(&conn) {
            log_warn!(log(), "failed to checkpoint the SQLite WAL", "path" => path.clone(), "error" => e.to_string());
        }
        if let Err((_, e)) = conn.close() {
            log_warn!(log(), "failed to close the SQLite database", "path" => path, "error" => e.to_string());
        }
        drop(lock);
        clean
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rusqlite::OpenFlags;

    use super::*;
    use crate::broker::sqlite::testing::{Recording, TempDir, history};
    use crate::broker::test_clock::{T0, TestClock};

    fn timing(flush_interval: Duration) -> Timing {
        Timing {
            flush_interval,
            prune_interval: Duration::from_secs(3600),
            error_log_interval: Duration::from_secs(10),
        }
    }

    fn persister(
        dir: &TempDir,
        conn: Connection,
        queue: &Arc<WriteQueue>,
        observer: &Arc<Recording>,
        config: &Config,
        timing: Timing,
    ) -> Persister {
        let lock = File::create(dir.file("cabby.db.lock")).unwrap();
        let db = Database { conn, lock, path: dir.file("cabby.db") };
        Persister::new(db, queue.clone(), config, TestClock::new(), observer.clone(), timing)
    }

    fn query_i64s(path: &Path, sql: &str) -> Vec<i64> {
        let conn = Connection::open(path).unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    }

    fn history_count(dir: &TempDir) -> i64 {
        query_i64s(&dir.file("cabby.db"), "SELECT count(*) FROM history")[0]
    }

    #[test]
    fn write_failures_are_logged_first_then_every_interval_and_the_recovery_once() {
        let t0 = Instant::now();
        let mut log = ErrorLog::new(Duration::from_secs(10));
        assert!(log.failed(t0), "the first failure");
        assert!(!log.failed(t0 + Duration::from_secs(1)));
        assert!(!log.failed(t0 + Duration::from_secs(9)));
        assert!(log.failed(t0 + Duration::from_secs(10)), "one line per interval");
        assert!(!log.failed(t0 + Duration::from_secs(11)));
        assert!(log.succeeded(), "the recovery");
        assert!(!log.succeeded(), "logged once");
        assert!(log.failed(t0 + Duration::from_secs(12)), "a new run of failures is logged at once");
    }

    #[test]
    fn a_full_batch_is_written_without_waiting_for_the_tick() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 3));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_secs(60)));
        for offset in 1..=3 {
            queue.push(history("s", offset));
        }

        let started = Instant::now();
        p.tick();

        assert!(started.elapsed() < Duration::from_secs(30), "did not wait for the 60 s tick");
        assert_eq!(3, history_count(&dir));
        assert_eq!(1, observer.batches());
    }

    #[test]
    fn a_partial_batch_is_written_after_the_flush_interval() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(50)));
        queue.push(history("s", 1));

        let started = Instant::now();
        p.tick();

        assert!(started.elapsed() >= Duration::from_millis(50), "waited for the tick");
        assert_eq!(1, history_count(&dir));
    }

    #[test]
    fn each_batch_upserts_the_streams_it_wrote() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 3));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_secs(60)));
        for op in [history("s", 1), history("s", 2), history("t", 1)] {
            queue.push(op);
        }

        p.tick();

        let path = dir.file("cabby.db");
        assert_eq!(vec![2, 1], query_i64s(&path, "SELECT max_seq FROM streams ORDER BY stream"));
        assert_eq!(vec![T0 + 3000, T0 + 3000], query_i64s(&path, "SELECT deadline FROM streams ORDER BY stream"));
    }

    #[test]
    fn a_failed_batch_is_kept_and_retried_on_the_next_tick() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let conn = store::open(&path).unwrap();
        conn.busy_timeout(Duration::from_millis(20)).unwrap();
        let queue = Arc::new(WriteQueue::new(100, 1));
        let observer = Arc::new(Recording::default());
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(20)));
        // Another connection holds SQLite's write lock: the batch fails with SQLITE_BUSY.
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        queue.push(history("s", 1));

        p.tick();
        assert_eq!(1, observer.failures());
        assert_eq!(Some(&1), observer.queue_sizes().last(), "the failed op is still waiting");

        blocker.execute_batch("ROLLBACK").unwrap();
        p.tick();
        assert_eq!(1, history_count(&dir));
        assert_eq!((1, 1), (observer.failures(), observer.batches()));
        assert_eq!(Some(&0), observer.queue_sizes().last());
    }

    #[test]
    fn pruning_runs_when_due() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_limit = 2;
        let mut t = timing(Duration::from_millis(10));
        t.prune_interval = Duration::ZERO;
        let queue = Arc::new(WriteQueue::new(100, 5));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &config, t);
        for offset in 1..=5 {
            queue.push(history("s", offset));
        }

        p.tick();

        assert_eq!(vec![4, 5], query_i64s(&dir.file("cabby.db"), "SELECT seq FROM history ORDER BY seq"));
    }

    #[test]
    fn the_final_flush_writes_everything_marks_the_shutdown_clean_and_leaves_no_wal() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        queue.push(history("s", 1));
        queue.push(history("s", 2));

        assert!(p.finish(Instant::now() + Duration::from_secs(5)));

        assert!(!dir.file("cabby.db-wal").exists(), "checkpointed and closed");
        assert_eq!(2, history_count(&dir));
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        assert_eq!(Some("1".to_owned()), store::meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn a_final_flush_to_a_read_only_database_leaves_the_shutdown_unclean() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let setup = store::open(&path).unwrap();
        store::meta_set(&setup, "clean_shutdown", "0").unwrap();
        let read_only = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let p = persister(&dir, read_only, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        queue.push(history("s", 1));

        assert!(!p.finish(Instant::now() + Duration::from_millis(300)));

        assert!(observer.failures() >= 1);
        assert_eq!(Some("0".to_owned()), store::meta_get(&setup, "clean_shutdown").unwrap());
        assert_eq!(0, history_count(&dir));
    }

    #[test]
    fn without_time_left_the_final_write_is_not_attempted() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        queue.push(history("s", 1));

        assert!(!p.finish(Instant::now()));

        assert_eq!(0, history_count(&dir));
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        assert_eq!(None, store::meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn run_ends_with_the_final_flush_once_the_queue_closes() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        let writer = std::thread::spawn(move || p.run());

        queue.push(history("s", 1));
        queue.close(Instant::now() + Duration::from_secs(5));

        assert!(writer.join().unwrap(), "marked clean");
        assert_eq!(1, history_count(&dir));
    }
}
