//! `--pubsub=postgres` (D-070): cross-node pub/sub over `LISTEN`/`NOTIFY`.
//!
//! Streams are hashed into [`SHARDS`] channels (`<channel>_0` ..
//! `<channel>_15`) rather than one channel per stream: stream names may be
//! longer than PostgreSQL's 63-byte identifiers and contain any character, and
//! `LISTEN` per stream would mean thousands of them. A node listens to a shard
//! while at least one of its local streams hashes into it, and delivers a
//! message only when its stream is subscribed locally. Remote commands use
//! `<channel>_cmd`, which every node listens to.
//!
//! Payloads are JSON envelopes: `{"t":"s","m":<message>}` for a stream
//! message, `{"t":"c","m":<command>}` for a remote command. `NOTIFY` payloads
//! must stay under 8000 bytes, so an envelope larger than
//! [`INLINE_LIMIT`] is stored in `<prefix>pubsub_overflow` (for 60 seconds)
//! and the notification carries its row: `{"t":"ref","id":123}`. Receivers
//! read the row; a background task deletes expired rows.
//!
//! As with Redis and NATS, a node receives its own publications through
//! pub/sub: the broker publishes, and every node, this one included, delivers
//! the message to its clients when it comes back. Publications wait in a
//! bounded queue ([`QUEUE_CAPACITY`]); a caller that finds it full blocks
//! until there is room.

use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use cabby_core::common::{PubSubMessage, RemoteCommandMessage, StreamMessage, pub_sub_message_from_json};
use cabby_core::logger::{LogValuer, Logger, compact_value_bytes};
use cabby_core::pubsub::{Handler, PubSubError, Published, Subscriber};
use cabby_core::utils::backpressure;
use cabby_core::{log_debug, log_error, log_info, log_warn};
use serde_json::value::RawValue;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::connect::{self, ConnectSpec, ListenSink, Listener, Params, Pool};
use super::{Metrics, Names, OVERFLOW, PgError, PostgresConfig, SHARDS, schema, shard};

/// The largest envelope sent inline (bytes); larger ones spill over.
pub const INLINE_LIMIT: usize = 7500;
/// Publications that may wait for Postgres before callers are held back (as
/// the Redis publisher, D-040).
pub const QUEUE_CAPACITY: usize = 1024;
/// Most publications sent in one pipelined batch.
const MAX_BATCH: usize = 64;
/// How long a spilled payload is kept.
const OVERFLOW_TTL_SECS: i64 = 60;
/// How often expired overflow rows are deleted.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// How long `start` waits for the `LISTEN` connection.
const START_WAIT: Duration = Duration::from_secs(10);
/// Command connections for publishing and reading spilled payloads.
const POOL_SIZE: usize = 2;

/// What a `NOTIFY` payload holds.
#[derive(Debug, PartialEq, Eq)]
pub enum Envelope<'a> {
    /// A stream message or a remote command (the pub/sub JSON).
    Inline(&'a [u8]),
    /// An overflow row.
    Ref(i64),
}

#[derive(serde::Deserialize)]
struct RawEnvelope<'a> {
    t: &'a str,
    #[serde(borrow, default)]
    m: Option<&'a RawValue>,
    #[serde(default)]
    id: Option<i64>,
}

/// Encodes a stream message (`"s"`) or remote command (`"c"`) envelope.
pub fn encode(kind: &str, message_json: &[u8]) -> String {
    let mut out = String::with_capacity(message_json.len() + 16);
    out.push_str("{\"t\":\"");
    out.push_str(kind);
    out.push_str("\",\"m\":");
    out.push_str(&String::from_utf8_lossy(message_json));
    out.push('}');
    out
}

/// The notification for a spilled envelope.
pub fn reference(id: i64) -> String {
    format!("{{\"t\":\"ref\",\"id\":{id}}}")
}

/// Whether an envelope must spill over.
pub fn spills(envelope: &str) -> bool {
    envelope.len() > INLINE_LIMIT
}

/// Decodes an envelope.
pub fn decode(payload: &str) -> Result<Envelope<'_>, String> {
    let env: RawEnvelope<'_> = serde_json::from_str(payload).map_err(|e| e.to_string())?;
    match (env.t, env.m, env.id) {
        ("s" | "c", Some(m), _) => Ok(Envelope::Inline(m.get().as_bytes())),
        ("ref", _, Some(id)) => Ok(Envelope::Ref(id)),
        (t, _, _) => Err(format!("unknown envelope type {t:?}")),
    }
}

/// Upstream `Publish`'s debug record.
fn log_publish(log: &Logger, channel: &str, msg: Published<'_>) {
    log_debug!(log, "publish message", "channel" => channel, "data" => msg.log_value());
}

struct Running {
    listener: Listener,
    tx: mpsc::Sender<(String, String)>,
    stop: CancellationToken,
}

#[derive(Default)]
struct Subscriptions {
    streams: HashSet<String>,
    shards: [usize; SHARDS],
}

/// Cross-node pub/sub over PostgreSQL `LISTEN`/`NOTIFY`.
pub struct PostgresSubscriber {
    node: Arc<dyn Handler>,
    names: Names,
    spec: Arc<ConnectSpec>,
    auto_migrate: bool,
    metrics: Metrics,
    log: Logger,
    subscriptions: Arc<Mutex<Subscriptions>>,
    running: Arc<Mutex<Option<Running>>>,
}

/// Waits until a subscriber listens to a stream's shard, without holding any
/// of its locks (tests, and callers that need a subscription to be live).
#[derive(Clone)]
pub struct ListenProbe {
    names: Names,
    subscriptions: Arc<Mutex<Subscriptions>>,
    running: Arc<Mutex<Option<Running>>>,
}

impl ListenProbe {
    fn waiter(&self) -> Option<connect::ListenWaiter> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner).as_ref().map(|r| r.listener.waiter())
    }

    /// Waits up to `timeout` until `stream`'s shard is listened to; true at
    /// once when `stream` is not subscribed.
    pub fn wait_stream(&self, stream: &str, timeout: Duration) -> bool {
        if !self.subscriptions.lock().unwrap_or_else(PoisonError::into_inner).streams.contains(stream) {
            return true;
        }
        self.waiter().is_some_and(|w| w.wait_listening(&self.names.stream_channel(stream), timeout))
    }

    /// Waits up to `timeout` until every wanted channel is listened to.
    pub fn wait_ready(&self, timeout: Duration) -> bool {
        self.waiter().is_some_and(|w| w.wait_ready(timeout))
    }
}

impl PostgresSubscriber {
    /// A [`ListenProbe`] for this subscriber.
    pub fn probe(&self) -> ListenProbe {
        ListenProbe {
            names: self.names.clone(),
            subscriptions: self.subscriptions.clone(),
            running: self.running.clone(),
        }
    }

    pub fn new(node: Arc<dyn Handler>, config: &PostgresConfig, metrics: Metrics) -> Result<Self, PubSubError> {
        let names = Names::from_config(config).map_err(PubSubError)?;
        let spec = ConnectSpec::from_config(config, "--pubsub=postgres").map_err(PubSubError)?;
        Ok(Self {
            node,
            names,
            spec: Arc::new(spec),
            auto_migrate: config.auto_migrate,
            metrics,
            log: super::logger("pubsub"),
            subscriptions: Arc::new(Mutex::new(Subscriptions::default())),
            running: Arc::new(Mutex::new(None)),
        })
    }

    fn subs(&self) -> std::sync::MutexGuard<'_, Subscriptions> {
        self.subscriptions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn running(&self) -> std::sync::MutexGuard<'_, Option<Running>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn publish(&self, channel: String, msg: Published<'_>, envelope: String) {
        log_publish(&self.log, &channel, msg);
        // Publishing may wait for room in the queue: not under the lock.
        let tx = self.running().as_ref().map(|r| r.tx.clone());
        if let Some(tx) = tx {
            let _ = backpressure::send_blocking(&tx, (channel, envelope), connect::runtime(), None);
        }
    }

    /// The channels to listen to: commands plus every shard in use.
    fn channels(&self, subs: &Subscriptions) -> Vec<String> {
        let mut out = vec![self.names.command_channel()];
        out.extend((0..SHARDS).filter(|i| subs.shards[*i] > 0).map(|i| self.names.shard_channel(i)));
        out
    }
}

/// Hands notifications to the dispatcher task, in order.
struct Forward(mpsc::UnboundedSender<(String, String)>);

impl ListenSink for Forward {
    fn notification(&self, channel: &str, payload: &str) {
        let _ = self.0.send((channel.to_owned(), payload.to_owned()));
    }
}

struct Dispatcher {
    node: Arc<dyn Handler>,
    pool: Arc<Pool>,
    subscriptions: Arc<Mutex<Subscriptions>>,
    overflow_select: String,
    log: Logger,
}

impl Dispatcher {
    async fn run(self, mut rx: mpsc::UnboundedReceiver<(String, String)>, stop: CancellationToken) {
        loop {
            let (channel, payload) = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                n = rx.recv() => match n { Some(n) => n, None => return },
            };
            match decode(&payload) {
                Ok(Envelope::Inline(json)) => self.deliver(&channel, json),
                Ok(Envelope::Ref(id)) => match self.fetch(id).await {
                    Ok(Some(full)) => match decode(&full) {
                        Ok(Envelope::Inline(json)) => self.deliver(&channel, json),
                        _ => {
                            log_warn!(self.log, "failed to parse pubsub message", "data" => compact_value_bytes(full.as_bytes()).log_value())
                        }
                    },
                    Ok(None) => log_warn!(self.log, "spilled pubsub message expired before it was read", "id" => id),
                    Err(e) => log_error!(self.log, "failed to read spilled pubsub message", "id" => id, "error" => e.0),
                },
                Err(e) => {
                    log_warn!(self.log, "failed to parse pubsub message", "data" => compact_value_bytes(payload.as_bytes()).log_value(), "error" => e);
                }
            }
        }
    }

    async fn fetch(&self, id: i64) -> Result<Option<String>, PgError> {
        let params: Params = vec![Box::new(id)];
        let rows = self.pool.query(&self.overflow_select, &params).await?;
        Ok(rows.first().map(|r| r.get(0)))
    }

    fn subscribed(&self, stream: &str) -> bool {
        self.subscriptions.lock().unwrap_or_else(PoisonError::into_inner).streams.contains(stream)
    }

    /// Upstream `handleMessage`: stream messages for local streams and
    /// remote commands go to the node.
    fn deliver(&self, channel: &str, json: &[u8]) {
        match pub_sub_message_from_json(json) {
            Ok(PubSubMessage::Stream(msg)) => {
                if self.subscribed(&msg.stream) {
                    log_debug!(self.log, "received broadcast message", "channel" => channel);
                    self.node.broadcast(&msg);
                }
            }
            Ok(PubSubMessage::RemoteCommand(cmd)) => {
                log_debug!(self.log, "received remote command", "channel" => channel);
                self.node.execute_remote_command(&cmd);
            }
            Ok(PubSubMessage::Batch(_)) => {
                log_warn!(self.log, "received unknown message", "channel" => channel, "data" => compact_value_bytes(json).log_value());
            }
            Err(e) => {
                log_warn!(self.log, "failed to parse pubsub message", "data" => compact_value_bytes(json).log_value(), "error" => e.to_string());
            }
        }
    }
}

/// Sends queued publications: pipelined `pg_notify` calls, or an overflow
/// insert plus notification for large envelopes, in queue order.
async fn publish_loop(
    pool: Arc<Pool>,
    notify_sql: String,
    spill_sql: String,
    mut rx: mpsc::Receiver<(String, String)>,
    metrics: Metrics,
    log: Logger,
    stop: CancellationToken,
) {
    loop {
        let first = tokio::select! {
            biased;
            () = stop.cancelled() => return,
            m = rx.recv() => match m { Some(m) => m, None => return },
        };
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(m) => batch.push(m),
                Err(_) => break,
            }
        }
        let deadline = tokio::time::Instant::now() + connect::QUERY_TIMEOUT;
        let conn = match pool.get(deadline).await {
            Ok(c) => c,
            Err(e) => {
                metrics.error();
                log_error!(log, "failed to publish messages", "count" => batch.len(), "error" => e.0);
                continue;
            }
        };
        // Both statements are prepared before the batch is sent: a first
        // spill preparing its statement mid-batch would let the messages after
        // it overtake it.
        let prepared = tokio::time::timeout_at(deadline, async {
            conn.prepare(&notify_sql).await?;
            conn.prepare(&spill_sql).await
        })
        .await;
        if !matches!(prepared, Ok(Ok(_))) {
            metrics.error();
            let error = match prepared {
                Ok(Err(e)) => super::error_text(&e),
                _ => "timed out".to_owned(),
            };
            log_error!(log, "failed to publish messages", "count" => batch.len(), "error" => error);
            pool.discard(&conn);
            continue;
        }
        let sends = batch.iter().map(|(channel, envelope)| {
            let conn = conn.clone();
            let spill = spills(envelope);
            if spill {
                metrics.spillover();
            }
            let sql = if spill { &spill_sql } else { &notify_sql };
            async move { conn.query(sql, &[channel, envelope]).await.map(drop) }
        });
        let started = std::time::Instant::now();
        let results = tokio::time::timeout_at(deadline, futures_util::future::join_all(sends)).await;
        metrics.took(started.elapsed());
        match results {
            Ok(results) => {
                let failed: Vec<String> =
                    results.into_iter().filter_map(Result::err).map(|e| super::error_text(&e)).collect();
                if let Some(first) = failed.first() {
                    for _ in &failed {
                        metrics.error();
                    }
                    log_error!(log, "failed to publish messages", "count" => failed.len(), "error" => first.clone());
                    if conn.client.is_closed() {
                        pool.discard(&conn);
                    }
                }
            }
            Err(_) => {
                metrics.error();
                log_error!(log, "failed to publish messages", "count" => batch.len(), "error" => "timed out");
                pool.discard(&conn);
            }
        }
    }
}

async fn sweep_loop(pool: Arc<Pool>, sql: String, log: Logger, stop: CancellationToken) {
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            () = tokio::time::sleep(SWEEP_INTERVAL) => {}
        }
        if let Err(e) = pool.query(&sql, &Vec::new()).await {
            log_warn!(log, "failed to delete expired pubsub overflow rows", "error" => e.0);
        }
    }
}

impl Subscriber for PostgresSubscriber {
    fn start(&self, _done: Sender<PubSubError>) -> Result<(), PubSubError> {
        log_info!(
            self.log,
            &format!(
                "Starting Postgres pub/sub: {} (channel: {}, table: {})",
                self.spec.description,
                self.names.channel,
                self.names.table_display(OVERFLOW)
            )
        );
        let (spec, names, auto_migrate, log) =
            (self.spec.clone(), self.names.clone(), self.auto_migrate, self.log.clone());
        connect::block_on(
            async move { schema::prepare(&spec, &names, auto_migrate, schema::PUBSUB_TABLES, &log).await },
        )
        .map_err(|e| PubSubError(format!("failed to prepare the Postgres schema, cause: {}", e.0)))?;

        // Lock order: subscriptions, then running (as in `subscribe`), so a
        // subscription made while starting is either in the initial channels
        // or listened to once `running` is set.
        let subs = self.subs();
        let mut running = self.running();
        if running.is_some() {
            return Ok(());
        }
        let stop = CancellationToken::new();
        let pool = Pool::new(
            self.spec.clone(),
            &format!("cabby pubsub {}", self.names.channel),
            POOL_SIZE,
            self.metrics.clone(),
        );
        let overflow = self.names.table(OVERFLOW);

        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        connect::runtime().spawn(publish_loop(
            pool.clone(),
            "SELECT pg_notify($1, $2)".to_owned(),
            format!(
                "WITH o AS (INSERT INTO {overflow} (payload, expires_at) VALUES ($2, NOW() + INTERVAL '{OVERFLOW_TTL_SECS} seconds') RETURNING id) \
                 SELECT pg_notify($1, '{{\"t\":\"ref\",\"id\":' || id || '}}') FROM o"
            ),
            rx,
            self.metrics.clone(),
            self.log.clone(),
            stop.clone(),
        ));
        connect::runtime().spawn(sweep_loop(
            pool.clone(),
            format!("DELETE FROM {overflow} WHERE expires_at < NOW()"),
            self.log.clone(),
            stop.clone(),
        ));

        let (ntx, nrx) = mpsc::unbounded_channel();
        let dispatcher = Dispatcher {
            node: self.node.clone(),
            pool,
            subscriptions: self.subscriptions.clone(),
            overflow_select: format!("SELECT payload FROM {overflow} WHERE id = $1"),
            log: self.log.clone(),
        };
        connect::runtime().spawn(dispatcher.run(nrx, stop.clone()));

        let listener = Listener::start(
            self.spec.clone(),
            &format!("cabby pubsub-listen {}", self.names.channel),
            self.channels(&subs),
            Arc::new(Forward(ntx)),
            self.metrics.clone(),
            self.log.clone(),
        );
        let waiter = listener.waiter();
        *running = Some(Running { listener, tx, stop });
        drop(running);
        drop(subs);
        if !waiter.wait_ready(START_WAIT) {
            log_warn!(self.log, "LISTEN connection is not ready yet; retrying in the background");
        }
        Ok(())
    }

    fn shutdown(&self) -> Result<(), PubSubError> {
        let Some(running) = self.running().take() else {
            return Ok(());
        };
        log_debug!(self.log, "shutting down Postgres pub/sub");
        running.listener.shutdown();
        running.stop.cancel();
        Ok(())
    }

    fn broadcast(&self, msg: &StreamMessage) {
        let envelope = encode("s", &cabby_core::gojson::to_vec(msg));
        self.publish(self.names.stream_channel(&msg.stream), Published::Stream(msg), envelope);
    }

    fn broadcast_command(&self, cmd: &RemoteCommandMessage) {
        let envelope = encode("c", &cabby_core::gojson::to_vec(cmd));
        self.publish(self.names.command_channel(), Published::Command(cmd), envelope);
    }

    /// Registers the stream and, for a shard's first stream, asks the
    /// `LISTEN` connection to listen to it; does not wait for the server
    /// (see [`PostgresSubscriber::probe`]), so no lock is held for long.
    fn subscribe(&self, stream: &str) {
        let mut subs = self.subs();
        if !subs.streams.insert(stream.to_owned()) {
            return;
        }
        log_debug!(self.log, "subscribing", "channel" => stream);
        let i = shard(stream);
        subs.shards[i] += 1;
        if subs.shards[i] == 1
            && let Some(r) = self.running().as_ref()
        {
            r.listener.listen(&self.names.shard_channel(i));
        }
    }

    fn unsubscribe(&self, stream: &str) {
        let mut subs = self.subs();
        if !subs.streams.remove(stream) {
            return;
        }
        log_debug!(self.log, "unsubscribing", "channel" => stream);
        let i = shard(stream);
        subs.shards[i] -= 1;
        if subs.shards[i] == 0
            && let Some(r) = self.running().as_ref()
        {
            r.listener.unlisten(&self.names.shard_channel(i));
        }
    }

    fn is_multi_node(&self) -> bool {
        true
    }
}

impl Drop for PostgresSubscriber {
    fn drop(&mut self) {
        let _ = Subscriber::shutdown(self);
    }
}

/// Brokers publish through the subscriber's `Broadcaster` subset.
impl cabby_core::broker::Broadcaster for PostgresSubscriber {
    fn broadcast(&self, msg: &StreamMessage) {
        Subscriber::broadcast(self, msg);
    }

    fn broadcast_command(&self, msg: &RemoteCommandMessage) {
        Subscriber::broadcast_command(self, msg);
    }

    fn subscribe(&self, stream: &str) {
        Subscriber::subscribe(self, stream);
    }

    fn unsubscribe(&self, stream: &str) {
        Subscriber::unsubscribe(self, stream);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_envelopes_go_inline() {
        let msg = StreamMessage { stream: "chat".into(), data: "hi".into(), ..Default::default() };
        let json = cabby_core::gojson::to_vec(&msg);
        let env = encode("s", &json);
        assert!(!spills(&env));
        assert_eq!(Ok(Envelope::Inline(json.as_slice())), decode(&env));

        let cmd = RemoteCommandMessage { command: "disconnect".into(), payload: "{\"identifier\":\"x\"}".into() };
        let json = cabby_core::gojson::to_vec(&cmd);
        assert_eq!(Ok(Envelope::Inline(json.as_slice())), decode(&encode("c", &json)));
    }

    #[test]
    fn envelopes_over_the_limit_spill_over() {
        let at_limit = "x".repeat(INLINE_LIMIT - encode("s", b"\"\"").len());
        let env = encode("s", format!("\"{at_limit}\"").as_bytes());
        assert_eq!(INLINE_LIMIT, env.len());
        assert!(!spills(&env), "exactly at the limit stays inline");
        let env = encode("s", format!("\"{at_limit}x\"").as_bytes());
        assert!(spills(&env));

        let r = reference(123);
        assert_eq!("{\"t\":\"ref\",\"id\":123}", r);
        assert_eq!(Ok(Envelope::Ref(123)), decode(&r));
        // The notification itself stays far below PostgreSQL's 8000-byte limit.
        assert!(reference(i64::MAX).len() < 8000);
    }

    #[test]
    fn unknown_envelopes_are_rejected() {
        assert!(decode("{\"t\":\"x\",\"m\":{}}").is_err());
        assert!(decode("{\"t\":\"ref\"}").is_err());
        assert!(decode("not json").is_err());
    }
}
