//! The SQLite broker: a Cabby addition (deviation D-065; design in
//! `docs/superpowers/specs/2026-11-23-sqlite-broker-design.md`).
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

use std::sync::OnceLock;
use std::time::Duration;

use crate::logger::Logger;

mod files;
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
/// metrics (spec §7). Calls come from the broadcast path and the persister
/// thread, so they must be cheap.
pub trait SqliteObserver: Send + Sync {
    /// Ops waiting to be written, after each write attempt.
    fn queue_size(&self, ops: usize);
    /// A batch was written in `took`.
    fn batch_written(&self, took: Duration);
    /// A batch write failed; the batch is kept and retried.
    fn write_failed(&self);
    /// An op of `kind` was dropped because the queue was full.
    fn dropped(&self, kind: queue::OpKind);
}

/// Reports nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

impl SqliteObserver for NoopObserver {
    fn queue_size(&self, _ops: usize) {}
    fn batch_written(&self, _took: Duration) {}
    fn write_failed(&self) {}
    fn dropped(&self, _kind: queue::OpKind) {}
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
}
