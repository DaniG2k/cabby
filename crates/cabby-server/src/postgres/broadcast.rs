//! `--broadcast_adapter=postgres` (D-070): broadcasts from SQL.
//!
//! Applications insert the standard broadcast JSON (one message or a batch)
//! into `<prefix>broadcasts`, typically in the same transaction as the change
//! it announces, so a rolled-back transaction broadcasts nothing. The table's
//! statement-level `AFTER INSERT` trigger notifies `<channel>_in` (empty
//! payload, so one notification per statement); every node listens there,
//! and also polls every [`POLL_INTERVAL`] in case a notification was missed
//! while it reconnected.
//!
//! Claiming is at least once. A batch is one transaction holding the claim
//! lock (`pg_try_advisory_xact_lock`, so one node drains at a time and rows
//! go out in `id` order): `SELECT ... FOR UPDATE SKIP LOCKED`, each row
//! handed to the broker ([`Handler::handle_broadcast`]), the handled rows
//! deleted, commit. A crash mid-batch rolls back, so the rows already handled
//! are delivered again; a handler error ends the batch keeping the failed row
//! and the rows after it, and the claim is retried with backoff. A row that
//! is not JSON, or that fails [`MAX_HANDLER_FAILURES`] times in a row, is
//! deleted with an error log. A node that finds the lock taken retries a
//! second later; the server ends a claim transaction left idle for 60 s.
//! This is not a fan-out broadcaster.
//!
//! A JSON payload sent directly with `NOTIFY <channel>_in, '<json>'` (no
//! table) reaches every node, so each node delivers it to its own clients
//! ([`Handler::handle_pubsub`]): no history, no offsets, like the fan-out
//! `redis` broadcaster.

use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use cabby_core::broadcast::{BroadcastError, Broadcaster, Handler};
use cabby_core::logger::Logger;
use cabby_core::{log_debug, log_error, log_info, log_warn};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::connect::{self, ConnectSpec, ListenSink, Listener};
use super::{BROADCASTS, Metrics, Names, PgError, PostgresConfig, schema};

/// How often the outbox is polled without a notification.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Most rows claimed at once.
pub const CLAIM_BATCH: i64 = 100;
/// How soon a node tries again when another one holds the claim lock.
const BUSY_RETRY: Duration = Duration::from_secs(1);
/// The server ends a claim transaction idle this long (a node that died
/// mid-batch releases the lock and its rows).
const IDLE_IN_TRANSACTION: Duration = Duration::from_secs(60);
/// The longest one batch (claim, handle, delete, commit) may take.
const BATCH_TIMEOUT: Duration = Duration::from_secs(60);
/// A row the node fails to handle this many times in a row is dropped.
pub const MAX_HANDLER_FAILURES: u32 = 5;
/// How long `start` waits for the `LISTEN` connection.
const START_WAIT: Duration = Duration::from_secs(10);

/// What a notification on the ingestion channel asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Ingest<'a> {
    /// Claim rows from the outbox (the trigger's notification, or anything
    /// else that is not JSON).
    Claim,
    /// A broadcast sent directly with `NOTIFY` (JSON object or array).
    Direct(&'a str),
}

pub fn classify(payload: &str) -> Ingest<'_> {
    let trimmed = payload.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') { Ingest::Direct(payload) } else { Ingest::Claim }
}

struct Running {
    listener: Listener,
    stop: CancellationToken,
    claimer: tokio::task::JoinHandle<()>,
}

/// How long `shutdown` waits for a batch being handled.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(5);

/// Broadcast ingestion from a PostgreSQL outbox table and `NOTIFY`.
pub struct PostgresBroadcaster {
    node: Arc<dyn Handler>,
    names: Names,
    spec: Arc<ConnectSpec>,
    auto_migrate: bool,
    metrics: Metrics,
    log: Logger,
    running: Mutex<Option<Running>>,
}

impl PostgresBroadcaster {
    pub fn new(node: Arc<dyn Handler>, config: &PostgresConfig, metrics: Metrics) -> Result<Self, BroadcastError> {
        let names = Names::from_config(config).map_err(BroadcastError)?;
        let spec = ConnectSpec::from_config(config, "--broadcast_adapter=postgres").map_err(BroadcastError)?;
        Ok(Self {
            node,
            names,
            spec: Arc::new(spec),
            auto_migrate: config.auto_migrate,
            metrics,
            log: super::logger("broadcast"),
            running: Mutex::new(None),
        })
    }

    fn running(&self) -> std::sync::MutexGuard<'_, Option<Running>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Routes notifications: row IDs wake the claimer, JSON goes to the node.
struct Sink {
    wake: Arc<Notify>,
    direct: mpsc::UnboundedSender<String>,
}

impl ListenSink for Sink {
    fn notification(&self, _channel: &str, payload: &str) {
        match classify(payload) {
            Ingest::Claim => self.wake.notify_one(),
            Ingest::Direct(json) => {
                let _ = self.direct.send(json.to_owned());
            }
        }
    }

    /// Rows inserted while the connection was down are claimed now.
    fn connected(&self, _reconnected: bool) {
        self.wake.notify_one();
    }
}

/// Why a claim round ended.
#[derive(Debug, PartialEq, Eq)]
enum Round {
    /// The outbox is empty (as far as this transaction could see).
    Drained,
    /// Another node holds the claim lock and is draining.
    Busy,
    /// The node failed to handle a row; retry after a backoff.
    HandlerFailed,
}

struct Claimer {
    node: Arc<dyn Handler>,
    spec: Arc<ConnectSpec>,
    app: String,
    select_sql: String,
    delete_sql: String,
    lock_key: i64,
    metrics: Metrics,
    log: Logger,
    /// The row that failed last and how many times in a row.
    failing: Option<(i64, u32)>,
}

/// The delay before retrying after the `n`th handler failure in a row
/// (100 ms doubling, at most 5 s).
fn retry_delay(n: u32) -> Duration {
    Duration::from_millis(100u64 << n.min(6)).min(Duration::from_secs(5))
}

impl Claimer {
    async fn run(mut self, wake: Arc<Notify>, stop: CancellationToken) {
        let mut client: Option<tokio_postgres::Client> = None;
        // When to try again without a notification.
        let mut next = POLL_INTERVAL;
        let mut retries: u32 = 0;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = wake.notified(), if retries == 0 => {}
                () = tokio::time::sleep(next) => {}
            }
            if stop.is_cancelled() {
                return;
            }
            if client.as_ref().is_none_or(tokio_postgres::Client::is_closed) {
                match self.spec.connect(&self.app, None).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        self.metrics.error();
                        log_error!(self.log, "failed to claim broadcasts", "error" => e.0);
                        next = POLL_INTERVAL;
                        continue;
                    }
                }
            }
            let Some(c) = client.as_mut() else { continue };
            next = match self.drain(c, &stop).await {
                Ok(Round::Drained) => {
                    retries = 0;
                    POLL_INTERVAL
                }
                // The holder drains to empty; notifications and the poll cover the rest.
                Ok(Round::Busy) => {
                    retries = 0;
                    BUSY_RETRY
                }
                Ok(Round::HandlerFailed) => {
                    retries += 1;
                    retry_delay(retries - 1)
                }
                Err(e) => {
                    self.metrics.error();
                    log_error!(self.log, "failed to claim broadcasts", "error" => e.0);
                    // A broken transaction or connection: reconnect next time.
                    client = None;
                    retries = 0;
                    POLL_INTERVAL
                }
            };
        }
    }

    /// Claims and handles batches until the outbox is empty. Each batch is
    /// one transaction holding the claim lock (a transaction advisory lock),
    /// so one node drains at a time, in id order; a row is deleted in the
    /// transaction that handled it, so a crash mid-batch rolls the batch
    /// back and its rows are delivered again (at least once).
    async fn drain(&mut self, client: &mut tokio_postgres::Client, stop: &CancellationToken) -> Result<Round, PgError> {
        while !stop.is_cancelled() {
            let started = std::time::Instant::now();
            let round = tokio::time::timeout(BATCH_TIMEOUT, self.batch(client)).await;
            self.metrics.took(started.elapsed());
            match round {
                Ok(Ok(Some(round))) => return Ok(round),
                Ok(Ok(None)) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(PgError("claiming broadcasts timed out".into())),
            }
        }
        Ok(Round::Drained)
    }

    /// One batch; `None` when it was full and more rows may be waiting.
    async fn batch(&mut self, client: &mut tokio_postgres::Client) -> Result<Option<Round>, PgError> {
        let tx = client.transaction().await?;
        tx.batch_execute(&format!(
            "SET LOCAL idle_in_transaction_session_timeout = '{}s'",
            IDLE_IN_TRANSACTION.as_secs()
        ))
        .await?;
        let locked: bool = tx.query_one("SELECT pg_try_advisory_xact_lock($1)", &[&self.lock_key]).await?.get(0);
        if !locked {
            tx.rollback().await?;
            return Ok(Some(Round::Busy));
        }
        let rows = tx.query(self.select_sql.as_str(), &[&CLAIM_BATCH]).await?;
        let claimed: Vec<(i64, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
        if claimed.is_empty() {
            tx.commit().await?;
            return Ok(Some(Round::Drained));
        }
        let full = claimed.len() as i64 >= CLAIM_BATCH;
        log_debug!(self.log, "claimed broadcasts", "count" => claimed.len());

        let mut done: Vec<i64> = Vec::new();
        let mut valid: Vec<(i64, String)> = Vec::new();
        for (id, payload) in claimed {
            if serde_json::from_str::<serde::de::IgnoredAny>(&payload).is_err() {
                self.metrics.error();
                log_error!(self.log, "dropping a broadcast that is not JSON", "id" => id);
                done.push(id);
            } else {
                valid.push((id, payload));
            }
        }
        let (node, log) = (self.node.clone(), self.log.clone());
        // The handler may wait for the broker (another Postgres query). It
        // stops at the first failure: (handled IDs, failed ID).
        let (handled, failed) = tokio::task::spawn_blocking(move || {
            let mut handled = Vec::new();
            for (id, payload) in valid {
                if let Err(e) = node.handle_broadcast(payload.as_bytes()) {
                    log_error!(log, "failed to handle broadcast", "id" => id, "error" => e.0);
                    return (handled, Some(id));
                }
                handled.push(id);
            }
            (handled, None)
        })
        .await
        .map_err(|_| PgError("broadcast handler panicked".into()))?;
        self.metrics.claimed(handled.len());
        done.extend(&handled);
        let round = match failed {
            None => {
                self.failing = None;
                None
            }
            Some(id) => {
                self.metrics.error();
                let count = match self.failing {
                    Some((last, n)) if last == id => n + 1,
                    _ => 1,
                };
                if count >= MAX_HANDLER_FAILURES {
                    log_error!(self.log, &format!("dropping a broadcast the node failed to handle {count} times in a row"), "id" => id);
                    done.push(id);
                    self.failing = None;
                } else {
                    self.failing = Some((id, count));
                }
                Some(Round::HandlerFailed)
            }
        };
        if !done.is_empty() {
            tx.execute(self.delete_sql.as_str(), &[&done]).await?;
        }
        tx.commit().await?;
        Ok(match round {
            Some(r) => Some(r),
            None if full => None,
            None => Some(Round::Drained),
        })
    }
}

/// The claim lock's key (FNV-1a 64 bits of the outbox name), taken with
/// `pg_try_advisory_xact_lock`.
pub fn claim_lock_key(n: &Names) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in format!("cabby-claim:{}.{}", n.schema, n.prefix).bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::from_ne_bytes(hash.to_ne_bytes())
}

async fn direct_loop(node: Arc<dyn Handler>, mut rx: mpsc::UnboundedReceiver<String>, stop: CancellationToken) {
    loop {
        let json = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            m = rx.recv() => match m { Some(m) => m, None => return },
        };
        let node = node.clone();
        let _ = tokio::task::spawn_blocking(move || node.handle_pubsub(json.as_bytes())).await;
    }
}

impl Broadcaster for PostgresBroadcaster {
    fn start(&self, _done: Sender<BroadcastError>) -> Result<(), BroadcastError> {
        log_info!(
            self.log,
            &format!(
                "Starting Postgres broadcaster: {} (table: {}, channel: {})",
                self.spec.description,
                self.names.table_display(BROADCASTS),
                self.names.ingest_channel()
            )
        );
        let (spec, names, auto_migrate, log) =
            (self.spec.clone(), self.names.clone(), self.auto_migrate, self.log.clone());
        connect::block_on(
            async move { schema::prepare(&spec, &names, auto_migrate, schema::BROADCAST_TABLES, &log).await },
        )
        .map_err(|e| BroadcastError(format!("failed to prepare the Postgres schema, cause: {}", e.0)))?;

        let mut running = self.running();
        if running.is_some() {
            return Ok(());
        }
        let stop = CancellationToken::new();
        let wake = Arc::new(Notify::new());
        let table = self.names.table(BROADCASTS);
        let claimer = Claimer {
            node: self.node.clone(),
            spec: self.spec.clone(),
            app: format!("cabby broadcast {}", self.names.channel),
            lock_key: claim_lock_key(&self.names),
            select_sql: format!("SELECT id, payload FROM {table} ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED"),
            delete_sql: format!("DELETE FROM {table} WHERE id = ANY($1)"),
            metrics: self.metrics.clone(),
            log: self.log.clone(),
            failing: None,
        };
        let claimer = connect::runtime().spawn(claimer.run(wake.clone(), stop.clone()));
        let (dtx, drx) = mpsc::unbounded_channel();
        connect::runtime().spawn(direct_loop(self.node.clone(), drx, stop.clone()));

        let listener = Listener::start(
            self.spec.clone(),
            &format!("cabby broadcast-listen {}", self.names.channel),
            [self.names.ingest_channel()],
            Arc::new(Sink { wake, direct: dtx }),
            self.metrics.clone(),
            self.log.clone(),
        );
        let waiter = listener.waiter();
        *running = Some(Running { listener, stop, claimer });
        drop(running);
        if !waiter.wait_ready(START_WAIT) {
            log_warn!(self.log, "LISTEN connection is not ready yet; retrying in the background");
        }
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BroadcastError> {
        if let Some(running) = self.running().take() {
            log_debug!(self.log, "shutting down Postgres broadcaster");
            running.listener.shutdown();
            running.stop.cancel();
            // Rows already claimed are handled before this returns; no new
            // claims start.
            let claimer = running.claimer;
            let _ = connect::block_on(async move { tokio::time::timeout(SHUTDOWN_WAIT, claimer).await });
        }
        Ok(())
    }

    fn is_fanout(&self) -> bool {
        false
    }
}

impl Drop for PostgresBroadcaster {
    fn drop(&mut self) {
        let _ = Broadcaster::shutdown(self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_ids_claim_and_json_is_direct() {
        assert_eq!(Ingest::Claim, classify("123"));
        assert_eq!(Ingest::Claim, classify(""));
        assert_eq!(Ingest::Direct("{\"stream\":\"a\",\"data\":\"1\"}"), classify("{\"stream\":\"a\",\"data\":\"1\"}"));
        assert_eq!(Ingest::Direct(" [{}]"), classify(" [{}]"));
    }
}
