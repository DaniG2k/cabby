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

use crate::logger::Logger;

mod queue;
mod store;
mod units;

/// The broker's logger (`context=broker provider=sqlite`).
fn log() -> &'static Logger {
    static LOG: OnceLock<Logger> = OnceLock::new();
    LOG.get_or_init(|| Logger::default().with("context", "broker").with("provider", "sqlite"))
}

/// Helpers shared by the module's tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::queue::WriteOp;
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
}
