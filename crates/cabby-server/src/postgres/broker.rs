//! `--broker=postgres` (D-071): stream history and session cache shared by
//! every node through PostgreSQL.
//!
//! - **Offsets.** `handle_broadcast` assigns the next offset and stores the
//!   message in one statement: an upsert of `<prefix>streams` (`max_seq + 1`)
//!   feeding the insert into `<prefix>history`. The upsert locks the stream's
//!   row, so offsets are monotonic per stream across nodes.
//! - **Time.** Timestamps, TTLs and session expiry use the database clock
//!   (`clock_timestamp()`), so nodes with skewed clocks agree. A stream's
//!   timestamps never go backwards (`last_ts_sec`), which keeps retained
//!   history contiguous.
//! - **Reads.** History reads match the memory broker: an entry is retained
//!   while it is within `history_ttl` and among the last `history_limit`
//!   offsets; a stream exists until `10 x history_ttl` after its last
//!   message. `history_from` reports an unknown epoch, a missing stream, an
//!   offset older than the oldest retained one (a gap), an empty stream and
//!   an offset past the latest one with the memory broker's error texts. Each
//!   read is one statement, so it sees one consistent snapshot.
//! - **Epoch.** The first node to start inserts a random epoch into
//!   `<prefix>meta`; the others get the stored one from the same statement
//!   (`ON CONFLICT DO UPDATE ... RETURNING`), so all nodes share it, even
//!   when they create it at the same moment. Reads and writes return the current epoch with
//!   their results and a background task checks it every second: when the
//!   row is gone (an `UNLOGGED` table emptied by a database crash), a new
//!   epoch is created and clients resynchronize.
//! - **Pruning.** Every 30 s, one node (the one holding a transaction-level
//!   advisory lock) deletes expired history, streams and sessions and trims
//!   history beyond `history_limit`, 10000 rows per transaction (each with a
//!   server-side statement timeout) for at most 20 s per pass; reads filter
//!   by the same rules, so pruning only reclaims space.
//! - **Errors.** Database failures are returned as broker errors (and counted
//!   and logged); a failed `handle_broadcast` doesn't fan the message out.
//!   Presence is not supported (as with the NATS broker).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::{Duration, Instant};

use cabby_core::broker::{
    Broadcaster, Broker, BrokerError, Cacheable, Config as BrokerConfig, PresenceInfoOptions, StreamsTracker,
};
use cabby_core::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use cabby_core::logger::Logger;
use cabby_core::{log_error, log_info, log_warn};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::connect::{self, ConnectSpec, Params, Pool};
use super::{HISTORY, META, Metrics, Names, PgError, PostgresConfig, SESSIONS, STREAMS, error_text, schema};

/// Command connections shared by the broker's callers.
const POOL_SIZE: usize = 4;
/// How often the shared epoch is checked.
const EPOCH_INTERVAL: Duration = Duration::from_secs(1);
/// How often expired rows are deleted.
pub const PRUNE_INTERVAL: Duration = Duration::from_secs(30);

const NOW_S: &str = "EXTRACT(EPOCH FROM clock_timestamp())::bigint";
const NOW_MS: &str = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint";

fn err(message: impl Into<String>) -> BrokerError {
    BrokerError { message: message.into() }
}

fn decorate(message: &str, cause: &PgError) -> BrokerError {
    err(format!("{message}, cause: {}", cause.0))
}

/// A random epoch (the memory broker's 4-character nanoid alphabet).
fn new_epoch() -> String {
    const ALPHABET: &[u8] = b"useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict";
    let mut bytes = [0u8; 4];
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|b| ALPHABET[usize::from(b & 63)] as char).collect()
}

/// The broker's statements for one set of names.
struct Sql {
    broadcast: String,
    from: String,
    since: String,
    epoch_get: String,
    epoch_ensure: String,
    commit: String,
    restore: String,
    touch: String,
    prune: Vec<Prune>,
}

/// A pruning step; each statement deletes at most [`PRUNE_BATCH`] rows.
enum Prune {
    /// History beyond `history_limit` (`$1`).
    Limit(String),
    /// History older than `history_ttl` (`$1`).
    Ttl(String),
    /// Expired streams, then their history (`$1`: the deleted streams).
    Streams { streams: String, history: String },
    /// Expired sessions.
    Plain(String),
}

/// Rows deleted per pruning statement (each in its own transaction).
const PRUNE_BATCH: i64 = 10_000;
/// How long one pruning pass may take; the next pass goes on from there.
const PRUNE_BUDGET: Duration = Duration::from_secs(20);
/// The server-side limit for one pruning statement.
const PRUNE_STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);

impl Sql {
    fn new(n: &Names) -> Self {
        let (meta, streams, history, sessions) = (n.table(META), n.table(STREAMS), n.table(HISTORY), n.table(SESSIONS));
        // $1 stream, $2 history_ttl, $3 history_limit; `kept` is the retained history.
        let state = |filter: &str| {
            format!(
                "WITH clock AS (SELECT {NOW_S} AS now), \
                 st AS (SELECT s.max_seq FROM {streams} s, clock WHERE s.stream = $1 AND s.deadline_sec >= clock.now), \
                 kept AS (SELECT h.seq, h.ts_sec, h.data FROM {history} h, st, clock \
                          WHERE h.stream = $1 AND h.ts_sec >= clock.now - $2 AND h.seq > st.max_seq - $3) \
                 SELECT (SELECT value FROM {meta} WHERE key = 'epoch'), (SELECT max_seq FROM st), \
                        (SELECT min(seq) FROM kept), (SELECT max(seq) FROM kept), \
                        (SELECT COALESCE(json_agg(json_build_array(seq, data) ORDER BY seq), '[]'::json) \
                         FROM kept WHERE {filter})::text"
            )
        };
        Self {
            broadcast: format!(
                "WITH clock AS (SELECT {NOW_S} AS now), \
                 next_seq AS ( \
                   INSERT INTO {streams} AS s (stream, max_seq, deadline_sec, last_ts_sec) \
                   SELECT $1, 1, clock.now + $3, clock.now FROM clock \
                   ON CONFLICT (stream) DO UPDATE SET max_seq = s.max_seq + 1, \
                     deadline_sec = GREATEST(s.deadline_sec, EXCLUDED.deadline_sec), \
                     last_ts_sec = GREATEST(s.last_ts_sec, EXCLUDED.last_ts_sec) \
                   RETURNING max_seq, last_ts_sec), \
                 ins AS ( \
                   INSERT INTO {history} (stream, seq, ts_sec, data) \
                   SELECT $1, max_seq, last_ts_sec, $2 FROM next_seq \
                   ON CONFLICT (stream, seq) DO UPDATE SET ts_sec = EXCLUDED.ts_sec, data = EXCLUDED.data \
                   RETURNING seq) \
                 SELECT ins.seq, (SELECT value FROM {meta} WHERE key = 'epoch') FROM ins"
            ),
            from: state("seq > $4"),
            since: state("ts_sec >= $4"),
            epoch_get: format!("SELECT value FROM {meta} WHERE key = 'epoch'"),
            // DO UPDATE (to the same value) returns the row even when another
            // node inserted it concurrently: DO NOTHING would return nothing,
            // and a read in the same statement can't see that insert.
            epoch_ensure: format!(
                "INSERT INTO {meta} AS m (key, value) VALUES ('epoch', $1) \
                 ON CONFLICT (key) DO UPDATE SET value = m.value RETURNING value"
            ),
            commit: format!(
                "INSERT INTO {sessions} (sid, data, expires_at_ms) VALUES ($1, $2, {NOW_MS} + $3) \
                 ON CONFLICT (sid) DO UPDATE SET data = EXCLUDED.data, expires_at_ms = EXCLUDED.expires_at_ms"
            ),
            restore: format!("SELECT data FROM {sessions} WHERE sid = $1 AND expires_at_ms > {NOW_MS}"),
            touch: format!(
                "UPDATE {sessions} SET expires_at_ms = {NOW_MS} + $2 WHERE sid = $1 AND expires_at_ms > {NOW_MS}"
            ),
            prune: vec![
                Prune::Limit(format!(
                    "DELETE FROM {history} WHERE ctid IN (SELECT h.ctid FROM {history} h JOIN {streams} s \
                     ON h.stream = s.stream WHERE h.seq <= s.max_seq - $1 LIMIT {PRUNE_BATCH})"
                )),
                Prune::Ttl(format!(
                    "DELETE FROM {history} WHERE ctid IN (SELECT ctid FROM {history} WHERE ts_sec < {NOW_S} - $1 LIMIT {PRUNE_BATCH})"
                )),
                Prune::Streams {
                    streams: format!(
                        // The outer condition is checked again on the locked row, so a
                        // stream written meanwhile (deadline extended) is kept.
                        "DELETE FROM {streams} WHERE stream IN (SELECT stream FROM {streams} WHERE deadline_sec < {NOW_S} \
                         LIMIT {PRUNE_BATCH}) AND deadline_sec < {NOW_S} RETURNING stream"
                    ),
                    history: format!("DELETE FROM {history} WHERE stream = ANY($1)"),
                },
                Prune::Plain(format!(
                    "DELETE FROM {sessions} WHERE ctid IN (SELECT ctid FROM {sessions} WHERE expires_at_ms <= {NOW_MS} LIMIT {PRUNE_BATCH})"
                )),
            ],
        }
    }
}

/// What a history read returns: the epoch, the stream's last offset (when
/// the stream exists), the retained offsets and the selected entries.
struct State {
    epoch: Option<String>,
    max_seq: Option<i64>,
    low: Option<i64>,
    latest: Option<i64>,
    entries: Vec<(u64, String)>,
}

struct Inner {
    broadcaster: Option<Arc<dyn Broadcaster>>,
    conf: BrokerConfig,
    names: Names,
    spec: Arc<ConnectSpec>,
    auto_migrate: bool,
    metrics: Metrics,
    log: Logger,
    pool: Arc<Pool>,
    sql: Sql,
    tracker: StreamsTracker,
    epoch: RwLock<String>,
    started: AtomicBool,
    stop: CancellationToken,
    /// The maintenance task's own connection (pruning runs in a transaction).
    maintenance: tokio::sync::Mutex<Option<tokio_postgres::Client>>,
}

/// The shared Postgres broker.
pub struct PostgresBroker {
    inner: Arc<Inner>,
}

fn ttl(secs: i64) -> i64 {
    secs.max(0)
}

impl PostgresBroker {
    pub fn new(
        broadcaster: Option<Arc<dyn Broadcaster>>,
        config: &BrokerConfig,
        pg: &PostgresConfig,
        metrics: Metrics,
    ) -> Result<Self, BrokerError> {
        let names = Names::from_config(pg).map_err(err)?;
        let spec = Arc::new(ConnectSpec::from_config(pg, "--broker=postgres").map_err(err)?);
        let pool = Pool::new(spec.clone(), &format!("cabby broker {}", names.channel), POOL_SIZE, metrics.clone());
        Ok(Self {
            inner: Arc::new(Inner {
                broadcaster,
                conf: config.clone(),
                sql: Sql::new(&names),
                names,
                spec,
                auto_migrate: pg.auto_migrate,
                metrics,
                log: super::logger("broker"),
                pool,
                tracker: StreamsTracker::new(),
                epoch: RwLock::new(String::new()),
                started: AtomicBool::new(false),
                stop: CancellationToken::new(),
                maintenance: tokio::sync::Mutex::new(None),
            }),
        })
    }

    /// The shared epoch as this node last saw it.
    pub fn epoch(&self) -> String {
        self.inner.epoch()
    }

    /// Deletes expired rows now (the background task does it every
    /// [`PRUNE_INTERVAL`]); false when another node holds the lock.
    pub fn prune(&self) -> Result<bool, BrokerError> {
        let inner = self.inner.clone();
        connect::block_on(async move { inner.prune().await })
            .map_err(|e| decorate("failed to prune Postgres broker", &e))
    }

    /// Checks the shared epoch now (the background task does it every second).
    pub fn refresh_epoch(&self) -> Result<String, BrokerError> {
        let inner = self.inner.clone();
        connect::block_on(async move { inner.refresh_epoch().await })
            .map_err(|e| decorate("failed to read the Postgres broker epoch", &e))
    }
}

impl Inner {
    fn epoch(&self) -> String {
        self.epoch.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Records the epoch read from the database; returns it.
    fn observe_epoch(&self, epoch: &str) -> String {
        let mut current = self.epoch.write().unwrap_or_else(PoisonError::into_inner);
        if *current != epoch {
            if !current.is_empty() {
                log_warn!(self.log, "epoch updated", "epoch" => epoch);
            }
            epoch.clone_into(&mut current);
        }
        epoch.to_owned()
    }

    /// Inserts a new epoch unless one exists; returns the shared one.
    async fn ensure_epoch(&self) -> Result<String, PgError> {
        let params: Params = vec![Box::new(new_epoch())];
        let rows = self.pool.query(&self.sql.epoch_ensure, &params).await?;
        let epoch: String = rows.first().map(|r| r.get(0)).ok_or_else(|| PgError("no epoch row".into()))?;
        Ok(self.observe_epoch(&epoch))
    }

    /// The epoch from a query result, creating one if the row is gone.
    async fn epoch_or_new(&self, epoch: Option<String>) -> Result<String, PgError> {
        match epoch {
            Some(e) => Ok(self.observe_epoch(&e)),
            None => self.ensure_epoch().await,
        }
    }

    async fn refresh_epoch(&self) -> Result<String, PgError> {
        let rows = self.pool.query(&self.sql.epoch_get, &Vec::new()).await?;
        self.epoch_or_new(rows.first().map(|r| r.get(0))).await
    }

    async fn state(&self, sql: &str, stream: &str, arg: i64) -> Result<State, PgError> {
        let limit = i64::try_from(self.conf.history_limit).unwrap_or(i64::MAX);
        let params: Params =
            vec![Box::new(stream.to_owned()), Box::new(ttl(self.conf.history_ttl)), Box::new(limit), Box::new(arg)];
        let rows = self.pool.query(sql, &params).await?;
        let row = rows.first().ok_or_else(|| PgError("no result".into()))?;
        let entries: String = row.get(4);
        let entries: Vec<(u64, String)> =
            serde_json::from_str(&entries).map_err(|e| PgError(format!("unexpected history rows: {e}")))?;
        Ok(State { epoch: row.get(0), max_seq: row.get(1), low: row.get(2), latest: row.get(3), entries })
    }

    fn messages(stream: &str, epoch: &str, entries: Vec<(u64, String)>) -> Vec<StreamMessage> {
        entries
            .into_iter()
            .map(|(offset, data)| StreamMessage {
                stream: stream.to_owned(),
                data,
                offset,
                epoch: epoch.to_owned(),
                meta: None,
            })
            .collect()
    }

    async fn add(&self, msg: &StreamMessage) -> Result<StreamMessage, PgError> {
        let deadline = ttl(self.conf.history_ttl).saturating_mul(10);
        let params: Params = vec![Box::new(msg.stream.clone()), Box::new(msg.data.clone()), Box::new(deadline)];
        let rows = self.pool.query(&self.sql.broadcast, &params).await?;
        let row = rows.first().ok_or_else(|| PgError("no offset returned".into()))?;
        let seq: i64 = row.get(0);
        let epoch = self.epoch_or_new(row.get(1)).await?;
        Ok(StreamMessage { offset: u64::try_from(seq).unwrap_or(0), epoch, ..msg.clone() })
    }

    /// Deletes expired rows in batches, each batch one transaction holding
    /// the pruning lock (another node pruning makes this return false), until
    /// done or [`PRUNE_BUDGET`] is used up.
    async fn prune(&self) -> Result<bool, PgError> {
        let mut slot = self.maintenance.lock().await;
        if slot.as_ref().is_none_or(tokio_postgres::Client::is_closed) {
            *slot = Some(self.spec.connect(&format!("cabby broker-maintenance {}", self.names.channel())).await?);
        }
        let Some(client) = slot.as_mut() else { unreachable!("connected above") };
        let started = Instant::now();
        for step in &self.sql.prune {
            loop {
                let batch = tokio::time::timeout(
                    connect::QUERY_TIMEOUT + PRUNE_STATEMENT_TIMEOUT,
                    self.prune_batch(client, step),
                )
                .await;
                let deleted = match batch {
                    Ok(Ok(Some(n))) => n,
                    Ok(Ok(None)) => return Ok(false),
                    Ok(Err(e)) => {
                        self.metrics.error();
                        return Err(PgError(error_text(&e)));
                    }
                    Err(_) => {
                        self.metrics.error();
                        *slot = None;
                        return Err(PgError("pruning timed out".into()));
                    }
                };
                if deleted < PRUNE_BATCH as u64 {
                    break;
                }
                if started.elapsed() >= PRUNE_BUDGET {
                    return Ok(true);
                }
            }
        }
        Ok(true)
    }

    /// One batch of `step`: rows deleted, or `None` without the lock.
    async fn prune_batch(
        &self,
        client: &mut tokio_postgres::Client,
        step: &Prune,
    ) -> Result<Option<u64>, tokio_postgres::Error> {
        let tx = client.transaction().await?;
        tx.batch_execute(&format!("SET LOCAL statement_timeout = '{}s'", PRUNE_STATEMENT_TIMEOUT.as_secs())).await?;
        let locked: bool =
            tx.query_one("SELECT pg_try_advisory_xact_lock($1)", &[&prune_lock_key(&self.names)]).await?.get(0);
        if !locked {
            return Ok(None);
        }
        let limit = i64::try_from(self.conf.history_limit).unwrap_or(i64::MAX);
        let ttl = ttl(self.conf.history_ttl);
        let deleted = match step {
            Prune::Limit(sql) => tx.execute(sql.as_str(), &[&limit]).await?,
            Prune::Ttl(sql) => tx.execute(sql.as_str(), &[&ttl]).await?,
            Prune::Plain(sql) => tx.execute(sql.as_str(), &[]).await?,
            Prune::Streams { streams, history } => {
                let gone: Vec<String> = tx.query(streams.as_str(), &[]).await?.iter().map(|r| r.get(0)).collect();
                if !gone.is_empty() {
                    tx.execute(history.as_str(), &[&gone]).await?;
                }
                gone.len() as u64
            }
        };
        tx.commit().await?;
        Ok(Some(deleted))
    }

    async fn maintain(self: Arc<Self>) {
        let mut since_prune = Duration::ZERO;
        loop {
            tokio::select! {
                biased;
                () = self.stop.cancelled() => return,
                () = tokio::time::sleep(EPOCH_INTERVAL) => {}
            }
            if let Err(e) = self.refresh_epoch().await {
                log_warn!(self.log, "failed to read the shared epoch", "error" => e.0);
            }
            since_prune += EPOCH_INTERVAL;
            if since_prune >= PRUNE_INTERVAL {
                since_prune = Duration::ZERO;
                if let Err(e) = self.prune().await {
                    log_warn!(self.log, "failed to prune expired history and sessions", "error" => e.0);
                }
            }
        }
    }
}

/// The pruning lock's key (distinct from the migration lock's).
fn prune_lock_key(n: &Names) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in format!("cabby-prune:{}.{}", n.schema, n.prefix).bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::from_ne_bytes(hash.to_ne_bytes())
}

impl Drop for PostgresBroker {
    fn drop(&mut self) {
        self.inner.stop.cancel();
    }
}

/// Runs `f` on the Postgres runtime, waiting for it.
fn run<T: Send + 'static, F>(inner: &Arc<Inner>, f: impl FnOnce(Arc<Inner>) -> F) -> Result<T, PgError>
where
    F: std::future::Future<Output = Result<T, PgError>> + Send + 'static,
{
    connect::block_on(f(inner.clone()))
}

impl Broker for PostgresBroker {
    fn start(&self) -> Result<(), BrokerError> {
        let inner = self.inner.clone();
        let epoch = connect::block_on(async move {
            schema::prepare(&inner.spec, &inner.names, inner.auto_migrate, schema::BROKER_TABLES, &inner.log).await?;
            inner.ensure_epoch().await
        })
        .map_err(|e| decorate("failed to start Postgres broker", &e))?;
        if !self.inner.started.swap(true, Ordering::SeqCst) {
            connect::runtime().spawn(self.inner.clone().maintain());
        }
        log_info!(self.inner.log, &format!(
            "Postgres broker is ready (schema: {}, table prefix: {})",
            self.inner.names.schema, self.inner.names.prefix
        ), "epoch" => epoch);
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.inner.stop.cancel();
        Ok(())
    }

    fn announce(&self) -> String {
        let c = &self.inner.conf;
        format!(
            "Using Postgres broker: {} (history limit: {}, history ttl: {}s, sessions ttl: {}s)",
            self.inner.spec.description, c.history_limit, c.history_ttl, c.sessions_ttl
        )
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        if msg.meta.as_ref().is_some_and(|m| m.transient) {
            if let Some(b) = &self.inner.broadcaster {
                b.broadcast(msg);
            }
            return Ok(());
        }
        let owned = msg.clone();
        let stored = run(&self.inner, |inner| async move { inner.add(&owned).await }).map_err(|e| {
            log_error!(self.inner.log, "failed to add message to stream history", "stream" => &msg.stream, "error" => e.0.clone());
            decorate(&format!("failed to add message to Postgres history: stream={}", msg.stream), &e)
        })?;
        if let Some(b) = &self.inner.broadcaster {
            b.broadcast(&stored);
        }
        Ok(())
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        if let Some(b) = &self.inner.broadcaster {
            b.broadcast_command(msg);
        }
        Ok(())
    }

    fn subscribe(&self, stream: &str) -> String {
        self.inner.tracker.add(stream, &mut || {
            if let Some(b) = &self.inner.broadcaster {
                b.subscribe(stream);
            }
        });
        stream.to_owned()
    }

    fn unsubscribe(&self, stream: &str) -> String {
        self.inner.tracker.remove(stream, &mut || {
            if let Some(b) = &self.inner.broadcaster {
                b.unsubscribe(stream);
            }
        });
        stream.to_owned()
    }

    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        let name = stream.to_owned();
        let arg = i64::try_from(offset).unwrap_or(i64::MAX);
        let (current, st) = run(&self.inner, |inner| async move {
            let st = inner.state(&inner.sql.from, &name, arg).await?;
            let current = inner.epoch_or_new(st.epoch.clone()).await?;
            Ok((current, st))
        })
        .map_err(|e| decorate("failed to read history from Postgres", &e))?;

        if current != epoch {
            return Err(err(format!("unknown epoch: {epoch}, current: {current}")));
        }
        if st.max_seq.is_none() {
            return Err(err("stream not found"));
        }
        let low = st.low.and_then(|l| u64::try_from(l).ok()).unwrap_or(0);
        if low > offset {
            return Err(err(format!("requested offset couldn't be found: {offset}, lowest: {low}")));
        }
        let Some(latest) = st.latest.and_then(|l| u64::try_from(l).ok()) else {
            return Err(err("stream is empty"));
        };
        if offset > latest {
            return Err(err(format!("requested offset couldn't be found: {offset}, latest: {latest}")));
        }
        Ok(Inner::messages(stream, &current, st.entries))
    }

    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        let name = stream.to_owned();
        let (current, st) = run(&self.inner, |inner| async move {
            let st = inner.state(&inner.sql.since, &name, ts).await?;
            let current = inner.epoch_or_new(st.epoch.clone()).await?;
            Ok((current, st))
        })
        .map_err(|e| decorate("failed to read history from Postgres", &e))?;
        if st.max_seq.is_none() {
            return Ok(None);
        }
        Ok(Some(Inner::messages(stream, &current, st.entries)))
    }

    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let data = session.to_cache_entry().map_err(|e| err(format!("failed to serialize session, cause: {e}")))?;
        let ttl_ms = ttl(self.inner.conf.sessions_ttl).saturating_mul(1000);
        let params: Params = vec![Box::new(sid.to_owned()), Box::new(data), Box::new(ttl_ms)];
        run(&self.inner, |inner| async move { inner.pool.query(&inner.sql.commit, &params).await.map(drop) })
            .map_err(|e| decorate("failed to save session to Postgres", &e))
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        let params: Params = vec![Box::new(sid.to_owned())];
        run(&self.inner, |inner| async move {
            let rows = inner.pool.query(&inner.sql.restore, &params).await?;
            Ok(rows.first().map(|r| r.get::<_, Vec<u8>>(0)))
        })
        .map_err(|e| decorate("failed to restore session from Postgres", &e))
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        let ttl_ms = ttl(self.inner.conf.sessions_ttl).saturating_mul(1000);
        let params: Params = vec![Box::new(sid.to_owned()), Box::new(ttl_ms)];
        run(&self.inner, |inner| async move { inner.pool.query(&inner.sql.touch, &params).await.map(drop) })
            .map_err(|e| decorate("failed to touch session in Postgres", &e))
    }

    fn supports_presence(&self) -> bool {
        false
    }

    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(err("presence not supported"))
    }

    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        Err(err("presence not supported"))
    }

    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        Err(err("presence not supported"))
    }

    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        Ok(())
    }

    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        let name = stream.to_owned();
        let (current, st) = run(&self.inner, |inner| async move {
            // No entries are selected past the last offset.
            let st = inner.state(&inner.sql.from, &name, i64::MAX).await?;
            let current = inner.epoch_or_new(st.epoch.clone()).await?;
            Ok((current, st))
        })
        .map_err(|e| decorate("failed to read history from Postgres", &e))?;
        match st.latest.and_then(|l| u64::try_from(l).ok()) {
            Some(offset) if st.max_seq.is_some() && offset > 0 => {
                Ok(Some(StreamMessage { offset, epoch: current, ..Default::default() }))
            }
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epochs_are_short_random_ids() {
        let e = new_epoch();
        assert_eq!(4, e.len());
        assert_ne!(new_epoch() + &new_epoch() + &new_epoch(), e.repeat(3));
    }

    #[test]
    fn a_missing_url_fails_at_construction() {
        let e = PostgresBroker::new(None, &BrokerConfig::new(), &PostgresConfig::new(), Metrics::none()).err().unwrap();
        assert_eq!(
            "--broker=postgres needs a connection URL: set --postgres_url, CABBY_POSTGRES_URL or [postgres] url",
            e.message
        );
    }

    #[test]
    fn expired_streams_are_checked_again_on_the_locked_row() {
        let sql = Sql::new(&Names::from_config(&PostgresConfig::new()).unwrap());
        let Some(Prune::Streams { streams, .. }) = sql.prune.iter().find(|p| matches!(p, Prune::Streams { .. })) else {
            panic!("no stream pruning step");
        };
        let after_limit = streams.split_once("LIMIT").map(|(_, rest)| rest).unwrap();
        assert!(after_limit.contains(&format!("AND deadline_sec < {NOW_S}")), "{streams}");
    }

    #[test]
    fn statements_use_validated_quoted_names() {
        let mut c = PostgresConfig::new();
        c.schema = "rt".into();
        let sql = Sql::new(&Names::from_config(&c).unwrap());
        assert!(sql.broadcast.contains("INSERT INTO \"rt\".\"cabby_streams\" AS s"), "{}", sql.broadcast);
        assert!(sql.restore.contains("FROM \"rt\".\"cabby_sessions\""), "{}", sql.restore);
    }
}
