//! `--broadcast_adapter=postgres` (D-070): broadcasts from SQL.
//!
//! Applications insert the standard broadcast JSON (one message or a batch)
//! into `<prefix>broadcasts`, typically in the same transaction as the change
//! it announces, so a rolled-back transaction broadcasts nothing. The table's
//! `AFTER INSERT` trigger notifies `<channel>_in` with the row ID; every node
//! listens there, and also polls every [`POLL_INTERVAL`] in case a
//! notification was missed while it reconnected. Nodes claim rows with
//! `DELETE ... FOR UPDATE SKIP LOCKED ... RETURNING`, so each row is handled
//! by exactly one node (at most once: a claimed row is gone), which hands it
//! to the broker ([`Handler::handle_broadcast`]); the pub/sub adapter fans it
//! out. A node drains the outbox while holding a session advisory lock (the
//! claim lock), so rows are handled one batch at a time in insertion order,
//! across nodes, and offsets follow that order. This is
//! not a fan-out broadcaster.
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

use super::connect::{self, Conn, ConnectSpec, ListenSink, Listener};
use super::{BROADCASTS, Metrics, Names, PgError, PostgresConfig, schema};

/// How often the outbox is polled without a notification.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Most rows claimed at once.
pub const CLAIM_BATCH: i64 = 100;
/// How long a node waits for another one to finish draining the outbox.
const LOCK_WAIT: Duration = Duration::from_secs(60);
/// How long `start` waits for the `LISTEN` connection.
const START_WAIT: Duration = Duration::from_secs(10);

/// What a notification on the ingestion channel asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Ingest<'a> {
    /// Claim rows from the outbox (the trigger sends the row ID).
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

struct Claimer {
    node: Arc<dyn Handler>,
    spec: Arc<ConnectSpec>,
    app: String,
    claim_sql: String,
    lock_key: i64,
    metrics: Metrics,
    log: Logger,
}

impl Claimer {
    async fn run(self, wake: Arc<Notify>, stop: CancellationToken) {
        let mut conn: Option<Conn> = None;
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = wake.notified() => {}
                () = tokio::time::sleep(POLL_INTERVAL) => {}
            }
            if stop.is_cancelled() {
                return;
            }
            if conn.as_ref().is_none_or(|c| c.client.is_closed()) {
                match self.spec.connect(&self.app, None).await {
                    Ok(c) => conn = Some(Conn::new(c)),
                    Err(e) => {
                        self.metrics.error();
                        log_error!(self.log, "failed to claim broadcasts", "error" => e.0);
                        continue;
                    }
                }
            }
            let Some(c) = conn.as_ref() else { continue };
            if let Err(e) = self.drain(c, &stop).await {
                self.metrics.error();
                log_error!(self.log, "failed to claim broadcasts", "error" => e.0);
                // Closing the connection releases the lock.
                conn = None;
            }
        }
    }

    /// Claims and handles rows until the outbox is empty, holding the claim
    /// lock: one node drains at a time, so rows are handled in insertion
    /// order. Claimed rows are deleted when claimed (at most once).
    async fn drain(&self, conn: &Conn, stop: &CancellationToken) -> Result<(), PgError> {
        let locked =
            tokio::time::timeout(LOCK_WAIT, conn.query("SELECT pg_advisory_lock($1)", &[&self.lock_key])).await;
        match locked {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Err(PgError("timed out waiting for the claim lock".into())),
        }
        let drained = self.claim_all(conn, stop).await;
        let unlocked = tokio::time::timeout(
            connect::QUERY_TIMEOUT,
            conn.query("SELECT pg_advisory_unlock($1)", &[&self.lock_key]),
        )
        .await;
        drained?;
        match unlocked {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(PgError("timed out releasing the claim lock".into())),
        }
    }

    async fn claim_all(&self, conn: &Conn, stop: &CancellationToken) -> Result<(), PgError> {
        while !stop.is_cancelled() {
            let started = std::time::Instant::now();
            let rows = tokio::time::timeout(connect::QUERY_TIMEOUT, conn.query(&self.claim_sql, &[&CLAIM_BATCH]))
                .await
                .map_err(|_| PgError("claiming broadcasts timed out".into()))??;
            self.metrics.took(started.elapsed());
            let mut claimed: Vec<(i64, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
            if claimed.is_empty() {
                return Ok(());
            }
            claimed.sort_unstable_by_key(|(id, _)| *id);
            let full = claimed.len() as i64 >= CLAIM_BATCH;
            self.metrics.claimed(claimed.len());
            log_debug!(self.log, "claimed broadcasts", "count" => claimed.len());
            let (node, metrics, log) = (self.node.clone(), self.metrics.clone(), self.log.clone());
            // The handler may wait for the broker (another Postgres query).
            let handled = tokio::task::spawn_blocking(move || {
                for (id, payload) in claimed {
                    if let Err(e) = node.handle_broadcast(payload.as_bytes()) {
                        metrics.error();
                        log_error!(log, "failed to handle broadcast", "id" => id, "error" => e.0);
                    }
                }
            })
            .await;
            if handled.is_err() {
                self.metrics.error();
                log_error!(self.log, "broadcast handler panicked");
            }
            if !full {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// The claim lock's key (FNV-1a 64 bits of the outbox name).
fn claim_lock_key(n: &Names) -> i64 {
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
            claim_sql: format!(
                "DELETE FROM {table} WHERE id IN (SELECT id FROM {table} ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED) RETURNING id, payload"
            ),
            metrics: self.metrics.clone(),
            log: self.log.clone(),
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
        if !listener.wait_ready(START_WAIT) {
            log_warn!(self.log, "LISTEN connection is not ready yet; retrying in the background");
        }
        *running = Some(Running { listener, stop, claimer });
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
