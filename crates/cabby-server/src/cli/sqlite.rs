//! The SQLite broker in the runner (Cabby only, deviation D-065): startup
//! validation, its metrics (spec §7), and a shutdown step that gives the final
//! write what is left of the shutdown timeout, but at least one second.

use std::sync::Arc;
use std::time::Duration;

use cabby_core::broker::{self as core_broker, OpKind, SqliteBroker, SqliteObserver, SqliteOptions};
use cabby_core::common::{RemoteCommandMessage, StreamMessage};

use super::adapters::{ShutdownContext, remaining};
use super::{Broker, CliError, Shutdownable};
use crate::config::Config;
use crate::metrics::Instrumenter;

pub(super) const QUEUE_SIZE: &str = "sqlite_queue_size";
pub(super) const BATCH_WRITE_MS: &str = "sqlite_batch_write_ms";
pub(super) const WRITE_ERRORS: &str = "sqlite_write_errors_total";
pub(super) const DROPPED: &str = "sqlite_dropped_total";
pub(super) const DROPPED_HISTORY: &str = "sqlite_dropped_history_total";
pub(super) const DROPPED_SESSION_TOUCH: &str = "sqlite_dropped_session_touch_total";
pub(super) const DROPPED_SESSION_COMMIT: &str = "sqlite_dropped_session_commit_total";

/// The least time the final write gets, even when the shutdown timeout is
/// already used up (owner decision for spec §5 step 2). If it still doesn't
/// finish, the file stays unclean and the next start rotates the epoch.
pub(super) const FINAL_FLUSH_FLOOR: Duration = Duration::from_secs(1);

/// The final write's budget: what is left of the shutdown timeout, at least
/// [`FINAL_FLUSH_FLOOR`].
pub(super) fn final_flush_budget(remaining: Duration) -> Duration {
    remaining.max(FINAL_FLUSH_FLOOR)
}

/// The broker's options from the configuration; the path is required.
pub(super) fn sqlite_options(c: &Config) -> Result<SqliteOptions, CliError> {
    if c.sqlite.path.trim().is_empty() {
        return Err(CliError(
            "--broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path".into(),
        ));
    }
    Ok(SqliteOptions::new(&c.sqlite.path))
}

/// Reports the broker's write path as metrics. The registry has no labels, so
/// drops are counted in total and per kind.
pub(super) struct MetricsObserver(Arc<dyn Instrumenter>);

impl MetricsObserver {
    pub(super) fn new(metrics: Arc<dyn Instrumenter>) -> Self {
        metrics.register_gauge(QUEUE_SIZE, "The number of SQLite broker writes waiting to be persisted");
        metrics.register_gauge(BATCH_WRITE_MS, "The duration of the last SQLite broker batch write (milliseconds)");
        metrics.register_counter(WRITE_ERRORS, "The total number of failed SQLite broker batch writes");
        metrics.register_counter(DROPPED, "The total number of SQLite broker writes dropped on queue overflow");
        metrics.register_counter(DROPPED_HISTORY, "The total number of history writes dropped on queue overflow");
        metrics
            .register_counter(DROPPED_SESSION_TOUCH, "The total number of session touches dropped on queue overflow");
        metrics
            .register_counter(DROPPED_SESSION_COMMIT, "The total number of session commits dropped on queue overflow");
        Self(metrics)
    }
}

impl SqliteObserver for MetricsObserver {
    fn queue_size(&self, ops: usize) {
        self.0.gauge_set(QUEUE_SIZE, u64::try_from(ops).unwrap_or(u64::MAX));
    }

    fn batch_written(&self, took: Duration) {
        self.0.gauge_set(BATCH_WRITE_MS, u64::try_from(took.as_millis()).unwrap_or(u64::MAX));
    }

    fn write_failed(&self) {
        self.0.counter_increment(WRITE_ERRORS);
    }

    fn dropped(&self, kind: OpKind) {
        self.0.counter_increment(DROPPED);
        self.0.counter_increment(match kind {
            OpKind::History => DROPPED_HISTORY,
            OpKind::SessionTouch => DROPPED_SESSION_TOUCH,
            OpKind::SessionCommit => DROPPED_SESSION_COMMIT,
        });
    }
}

/// The runner's view of the SQLite broker: its shutdown step gives the final
/// write the time left of the shutdown context, at least [`FINAL_FLUSH_FLOOR`].
pub(super) struct SqliteRunnerBroker {
    pub(super) broker: Arc<SqliteBroker>,
    pub(super) ctx: ShutdownContext,
}

impl Shutdownable for SqliteRunnerBroker {
    fn shutdown(&self) -> Result<(), CliError> {
        self.broker.shutdown_within(final_flush_budget(remaining(&self.ctx.get())));
        Ok(())
    }
}

impl Broker for SqliteRunnerBroker {
    fn start(&self) -> Result<(), CliError> {
        core_broker::Broker::start(self.broker.as_ref()).map_err(|e| CliError(e.message))
    }

    fn announce(&self) -> String {
        core_broker::Broker::announce(self.broker.as_ref())
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), CliError> {
        core_broker::Broker::handle_broadcast(self.broker.as_ref(), msg).map_err(|e| CliError(e.message))
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), CliError> {
        core_broker::Broker::handle_command(self.broker.as_ref(), msg).map_err(|e| CliError(e.message))
    }
}

#[cfg(test)]
mod tests {
    use cabby_core::broker::{Broker as _, Previous, SystemClock};

    use super::*;
    use crate::metrics::Metrics;

    #[test]
    fn the_sqlite_broker_needs_a_path() {
        let mut c = Config::new();
        c.broker.adapter = "sqlite".into();
        assert_eq!(
            "--broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path",
            sqlite_options(&c).unwrap_err().0
        );
        c.sqlite.path = "  ".into();
        assert!(sqlite_options(&c).is_err(), "blank is missing");

        c.sqlite.path = "/data/cabby.db".into();
        let options = sqlite_options(&c).unwrap();
        assert_eq!(std::path::PathBuf::from("/data/cabby.db"), options.path);
        assert_eq!(core_broker::LOCK_TIMEOUT, options.lock_timeout);
    }

    #[test]
    fn metrics_observer_registers_and_reports() {
        let m = Arc::new(Metrics::new(Vec::new(), 0));
        let observer = MetricsObserver::new(m.clone());

        observer.queue_size(7);
        observer.batch_written(Duration::from_millis(12));
        observer.write_failed();
        observer.dropped(OpKind::History);
        observer.dropped(OpKind::SessionCommit);

        assert_eq!(7, m.gauge(QUEUE_SIZE).unwrap().value());
        assert_eq!(12, m.gauge(BATCH_WRITE_MS).unwrap().value());
        assert_eq!(1, m.counter(WRITE_ERRORS).unwrap().value());
        assert_eq!(2, m.counter(DROPPED).unwrap().value());
        assert_eq!(1, m.counter(DROPPED_HISTORY).unwrap().value());
        assert_eq!(0, m.counter(DROPPED_SESSION_TOUCH).unwrap().value());
        assert_eq!(1, m.counter(DROPPED_SESSION_COMMIT).unwrap().value());
        assert!(m.prometheus().contains("anycable_go_sqlite_queue_size 7"), "{}", m.prometheus());
    }

    #[test]
    fn the_final_flush_gets_at_least_the_floor() {
        assert_eq!(FINAL_FLUSH_FLOOR, final_flush_budget(Duration::ZERO));
        assert_eq!(FINAL_FLUSH_FLOOR, final_flush_budget(Duration::from_millis(200)));
        assert_eq!(Duration::from_secs(7), final_flush_budget(Duration::from_secs(7)));
    }

    #[test]
    fn a_used_up_shutdown_timeout_still_allows_the_final_flush() {
        let dir = std::env::temp_dir().join(format!("cabby-sqlite-runner-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let options = SqliteOptions::new(dir.join("cabby.db"));
        let open = || {
            let config = cabby_core::broker::Config::new();
            SqliteBroker::open(
                None,
                None,
                &config,
                &options,
                Arc::new(SystemClock),
                Arc::new(core_broker::NoopObserver),
            )
            .unwrap()
        };
        let broker = Arc::new(open());
        broker.handle_broadcast(&StreamMessage { stream: "s".into(), data: "a".into(), ..Default::default() }).unwrap();

        // No time left: the runner's shutdown context is already past its deadline.
        let runner = SqliteRunnerBroker { broker: broker.clone(), ctx: ShutdownContext::new(Duration::ZERO) };
        runner.shutdown().unwrap();
        drop((runner, broker));

        let reopened = open();
        assert_eq!(Previous::Clean, reopened.report().previous, "the final write ran within the floor");
        assert_eq!(1, reopened.report().messages);
        drop(reopened);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
