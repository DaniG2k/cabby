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
//! are delivered again. The node's only error is a broker error (it accepts
//! payloads it can't parse, logging them), so a handler error ends the batch
//! keeping the failed row and the rows after it, and the claim is retried
//! with backoff (at most 5 s) for as long as it takes: no row is dropped for
//! it. A row that is not JSON is deleted with an error log without reaching
//! the node. A row the node can't take because Postgres can never store its
//! message (the broker's error says [`UNSTORABLE`]) is deleted with an error
//! log too, and the batch goes on. A node that finds the lock taken retries a
//! second later; the server ends a claim transaction left idle for 60 s.
//! A batch hands rows to the broker for at most 10 s, then commits what it
//! handled and goes on in a new transaction; only SQL statements time out,
//! never a handler call, so one handler call runs at a time.
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
use cabby_core::logger::{LogValuer, Logger, compact_value_bytes};
use cabby_core::{log_debug, log_error, log_info, log_warn};
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::connect::{self, ConnectSpec, ListenSink, Listener};
use super::{BROADCASTS, Metrics, Names, PgError, PostgresConfig, UNSTORABLE, limit_text, no_answer, schema};

/// How often the outbox is polled without a notification.
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Most rows claimed at once.
pub const CLAIM_BATCH: i64 = 100;
/// How soon a node tries again when another one holds the claim lock.
const BUSY_RETRY: Duration = Duration::from_secs(1);
/// The server ends a claim transaction idle this long (a node that died
/// mid-batch releases the lock and its rows).
const IDLE_IN_TRANSACTION: Duration = Duration::from_secs(60);
/// How long a batch hands rows to the broker before it commits what it
/// handled and goes on in a new transaction (well below
/// [`IDLE_IN_TRANSACTION`]; one handler call is bounded by the broker's own
/// timeouts).
const HANDLING_BUDGET: Duration = Duration::from_secs(10);

/// The server limit Cabby sets on claim transactions, for errors.
const CLAIM_LIMITS: &[(&str, &str)] =
    &[("idle_in_transaction_session_timeout", "Cabby ends a claim transaction left idle for 60s")];
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
    direct: connect::Inbox<String>,
}

impl ListenSink for Sink {
    fn notification(&self, _channel: &str, payload: &str) {
        match classify(payload) {
            Ingest::Claim => self.wake.notify_one(),
            Ingest::Direct(json) => self.direct.push(json.to_owned()),
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
    /// The outbox table (quoted).
    table: String,
    /// The outbox table as messages name it.
    outbox: String,
    /// The claim statement for the current connection's server encoding
    /// ([`select_sql`]).
    select_sql: String,
    delete_sql: String,
    lock_key: i64,
    metrics: Metrics,
    log: Logger,
    /// Failed handler calls in a row (for the log).
    failures: u64,
}

/// The claim statement. Payloads are read as bytes in UTF-8: the server
/// converts them from its encoding, except from `SQL_ASCII`, which has no
/// conversion and may hold bytes that aren't UTF-8; there the bytes are read
/// as stored, so such a row is set aside alone instead of failing the
/// statement for every row.
fn select_sql(table: &str, server_encoding: &str) -> String {
    let encoding = if server_encoding.eq_ignore_ascii_case("SQL_ASCII") { "SQL_ASCII" } else { "UTF8" };
    format!("SELECT id, convert_to(payload, '{encoding}') FROM {table} ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED")
}

/// What handing a batch's rows to the node came to.
struct Handled {
    /// Rows the node took.
    handled: Vec<i64>,
    /// Rows Postgres can't store ([`UNSTORABLE`]): ID, payload and error.
    unstorable: Vec<(i64, String, String)>,
    /// The row the node failed to take, and the error; it and the rows after
    /// it stay in the outbox.
    failed: Option<(i64, String)>,
    /// Rows not handed over because the handling budget ran out.
    left_over: bool,
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
                match self.connect().await {
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

    /// A claim statement with [`connect::QUERY_TIMEOUT`]; errors name the
    /// statement, the outbox and the server.
    async fn sql<T>(
        &self,
        statement: &str,
        fut: impl std::future::Future<Output = Result<T, tokio_postgres::Error>>,
    ) -> Result<T, PgError> {
        match tokio::time::timeout(connect::QUERY_TIMEOUT, fut).await {
            Ok(r) => r.map_err(|e| PgError(limit_text(&e, CLAIM_LIMITS))),
            Err(_) => Err(no_answer(
                &format!("{statement} (claiming from {})", self.outbox),
                connect::QUERY_TIMEOUT,
                &self.spec.description,
            )),
        }
    }

    /// Opens the claim connection and picks the claim statement for its
    /// server encoding.
    async fn connect(&mut self) -> Result<tokio_postgres::Client, PgError> {
        let client = self.spec.connect(&self.app).await?;
        let row = self.sql("SHOW server_encoding", client.query_one("SHOW server_encoding", &[])).await?;
        let encoding: String = row.try_get(0).map_err(PgError::from)?;
        self.select_sql = select_sql(&self.table, &encoding);
        Ok(client)
    }

    /// Claims and handles batches until the outbox is empty. Each batch is
    /// one transaction holding the claim lock (a transaction advisory lock),
    /// so one node drains at a time, in id order; a row is deleted in the
    /// transaction that handled it, so a crash mid-batch rolls the batch
    /// back and its rows are delivered again (at least once).
    async fn drain(&mut self, client: &mut tokio_postgres::Client, stop: &CancellationToken) -> Result<Round, PgError> {
        while !stop.is_cancelled() {
            let started = std::time::Instant::now();
            let round = self.batch(client, stop).await;
            self.metrics.took(started.elapsed());
            if let Some(round) = round? {
                return Ok(round);
            }
        }
        Ok(Round::Drained)
    }

    /// One batch; `None` when more rows may be waiting (the batch was full,
    /// or the handling budget ran out). Only SQL statements have timeouts:
    /// a handler call is never abandoned, so one runs at a time. Once `stop`
    /// is cancelled no further row is handed to the node: the rows handled
    /// are deleted and the others released for the next claim.
    async fn batch(
        &mut self,
        client: &mut tokio_postgres::Client,
        stop: &CancellationToken,
    ) -> Result<Option<Round>, PgError> {
        let tx = self.sql("BEGIN", client.transaction()).await?;
        let settings = format!("SET LOCAL idle_in_transaction_session_timeout = '{}s'", IDLE_IN_TRANSACTION.as_secs());
        self.sql("SET LOCAL", tx.batch_execute(&settings)).await?;
        let locked: bool = self
            .sql("the claim lock", tx.query_one("SELECT pg_try_advisory_xact_lock($1)", &[&self.lock_key]))
            .await?
            .get(0);
        if !locked {
            self.sql("ROLLBACK", tx.rollback()).await?;
            return Ok(Some(Round::Busy));
        }
        let rows =
            self.sql("SELECT ... FOR UPDATE SKIP LOCKED", tx.query(self.select_sql.as_str(), &[&CLAIM_BATCH])).await?;
        let claimed = rows
            .iter()
            .map(|r| Ok((r.try_get::<_, i64>(0)?, r.try_get::<_, Vec<u8>>(1)?)))
            .collect::<Result<Vec<_>, tokio_postgres::Error>>()?;
        if claimed.is_empty() {
            self.sql("COMMIT", tx.commit()).await?;
            return Ok(Some(Round::Drained));
        }
        let full = claimed.len() as i64 >= CLAIM_BATCH;
        log_debug!(self.log, "claimed broadcasts", "count" => claimed.len());

        let mut done: Vec<i64> = Vec::new();
        let mut valid: Vec<(i64, String)> = Vec::new();
        for (id, payload) in claimed {
            let json = String::from_utf8(payload)
                .map_err(|e| e.to_string())
                .and_then(|p| serde_json::from_str::<serde::de::IgnoredAny>(&p).map(|_| p).map_err(|e| e.to_string()));
            match json {
                Ok(p) => valid.push((id, p)),
                Err(e) => {
                    self.metrics.error();
                    log_error!(self.log, "dropping a broadcast that is not JSON", "id" => id, "error" => e);
                    done.push(id);
                }
            }
        }
        let (node, stop) = (self.node.clone(), stop.clone());
        let budget = std::time::Instant::now() + HANDLING_BUDGET;
        // The handler may wait for the broker (another Postgres query). It
        // stops at the first failure, or once the budget is used up; a row
        // Postgres can't store is set aside and the next one handed over.
        let Handled { handled, unstorable, failed, left_over } = tokio::task::spawn_blocking(move || {
            let mut out = Handled { handled: Vec::new(), unstorable: Vec::new(), failed: None, left_over: false };
            let total = valid.len();
            for (id, payload) in valid {
                let progress = out.handled.len() + out.unstorable.len();
                if stop.is_cancelled() || (progress > 0 && std::time::Instant::now() >= budget) {
                    break;
                }
                match node.handle_broadcast(payload.as_bytes()) {
                    Ok(()) => out.handled.push(id),
                    Err(e) if e.0.contains(UNSTORABLE) => out.unstorable.push((id, payload, e.0)),
                    Err(e) => {
                        out.failed = Some((id, e.0));
                        return out;
                    }
                }
            }
            out.left_over = out.handled.len() + out.unstorable.len() < total;
            out
        })
        .await
        .map_err(|_| PgError("broadcast handler panicked".into()))?;
        self.metrics.claimed(handled.len());
        done.extend(&handled);
        for (id, payload, error) in unstorable {
            self.metrics.error();
            log_error!(self.log, "dropping a broadcast Postgres can't store", "id" => id, "error" => error, "payload" => compact_value_bytes(payload.as_bytes()).log_value());
            done.push(id);
        }
        let round = match failed {
            None => {
                if self.failures > 0 {
                    log_info!(self.log, "handling broadcasts again", "failed_attempts" => self.failures);
                    self.failures = 0;
                }
                None
            }
            // A broker error: the row stays first in line and is retried.
            Some((id, error)) => {
                self.metrics.error();
                self.metrics.broadcast_retry();
                self.failures += 1;
                if self.failures.is_power_of_two() {
                    log_error!(self.log, "failed to handle broadcast, retrying", "id" => id, "failed_attempts" => self.failures, "error" => error);
                }
                Some(Round::HandlerFailed)
            }
        };
        if !done.is_empty() {
            self.sql("DELETE", tx.execute(self.delete_sql.as_str(), &[&done])).await?;
        }
        self.sql("COMMIT", tx.commit()).await?;
        Ok(match round {
            Some(r) => Some(r),
            None if full || left_over => None,
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

async fn direct_loop(node: Arc<dyn Handler>, mut rx: mpsc::Receiver<String>, stop: CancellationToken) {
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
            select_sql: select_sql(&table, "UTF8"),
            delete_sql: format!("DELETE FROM {table} WHERE id = ANY($1)"),
            table,
            outbox: self.names.table_display(BROADCASTS),
            metrics: self.metrics.clone(),
            log: self.log.clone(),
            failures: 0,
        };
        let claimer = connect::runtime().spawn(claimer.run(wake.clone(), stop.clone()));
        let (dtx, drx) = connect::Inbox::new(connect::INBOX_CAPACITY, self.metrics.clone(), self.log.clone());
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
            // The claimer hands no further row to the node and commits the
            // rows it handled; a claimer still busy after SHUTDOWN_WAIT (a
            // handler call or a statement that doesn't end) is aborted, which
            // rolls its transaction back, so those rows are claimed again.
            let mut claimer = running.claimer;
            connect::block_on(async move {
                if tokio::time::timeout(SHUTDOWN_WAIT, &mut claimer).await.is_err() {
                    claimer.abort();
                }
            });
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
    fn payloads_are_read_as_stored_only_from_sql_ascii() {
        let utf8 = select_sql("\"public\".\"cabby_broadcasts\"", "UTF8");
        assert_eq!(
            "SELECT id, convert_to(payload, 'UTF8') FROM \"public\".\"cabby_broadcasts\" ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED",
            utf8
        );
        assert_eq!(utf8, select_sql("\"public\".\"cabby_broadcasts\"", "LATIN1"), "the server converts");
        assert!(select_sql("t", "SQL_ASCII").contains("convert_to(payload, 'SQL_ASCII')"));
    }

    #[test]
    fn row_ids_claim_and_json_is_direct() {
        assert_eq!(Ingest::Claim, classify("123"));
        assert_eq!(Ingest::Claim, classify(""));
        assert_eq!(Ingest::Direct("{\"stream\":\"a\",\"data\":\"1\"}"), classify("{\"stream\":\"a\",\"data\":\"1\"}"));
        assert_eq!(Ingest::Direct(" [{}]"), classify(" [{}]"));
    }
}
