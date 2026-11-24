//! The Redis broker (`--broker=redis`), a Cabby addition (deviation D-069 in
//! `compat/deviations.md` describes its observable behavior).
//!
//! Every node reads and writes the same Redis, so history and sessions are
//! shared without a node-local copy:
//!
//! * `<prefix>:epoch`: the epoch, created once (`SET NX` semantics) and read
//!   back by every operation that hands out or checks offsets, so all nodes
//!   use the same one. When Redis loses it (a restart without persistence, an
//!   eviction), the next operation creates a new one and clients resync.
//! * `<prefix>:{s:<stream>}:seq` and `<prefix>:{s:<stream>}:hist`: a stream's
//!   last offset (`INCR`) and its history (a Redis Stream with the explicit
//!   IDs `<offset>-0`). The hash tag keeps both in one Redis Cluster slot, so
//!   one Lua script assigns the offset, appends and sets the expiries.
//! * `<prefix>:sess:<sid>`: a session's cached state.
//!
//! History reads apply the memory broker's contract at read time: the newest
//! `history_limit` entries (`MAXLEN ~` may keep more), without the entries
//! older than `history_ttl`. Timestamps come from the Redis server's clock
//! (`TIME`), so every node agrees on what has expired.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock};
use std::time::{Duration, Instant};

use cabby_core::broker::{
    Broadcaster, Broker, BrokerError, Cacheable, Config, Entry, PresenceInfoOptions, StreamsTracker,
};
use cabby_core::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use cabby_core::logger::Logger;
use cabby_core::toml_config;
use cabby_core::{log_debug, log_error, log_info, log_warn};
use redis::Value;

use crate::metrics::Instrumenter;
use crate::redis::RedisConfig;
use crate::redis::connect::{self, Commands, ConnectSpec};

/// The default key prefix (`[redis_broker] key_prefix`).
pub const DEFAULT_KEY_PREFIX: &str = "__cabby__";
/// The default per-operation timeout in seconds (`[redis_broker] timeout`).
pub const DEFAULT_TIMEOUT: i64 = 5;

fn err(message: impl Into<String>) -> BrokerError {
    BrokerError { message: message.into() }
}

/// `[redis_broker]`: settings of the Redis broker (Cabby only, D-069). The
/// connection itself is `[redis]` (`--redis_url` and friends).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisBrokerConfig {
    /// Prefix of every key the broker writes, so applications can share one Redis.
    pub key_prefix: String,
    /// How long one broker operation may take, in seconds.
    pub timeout: i64,
}

cabby_core::toml_section!(RedisBrokerConfig, "redis_broker.Config" {
    "key_prefix" => key_prefix,
    "timeout" => timeout,
});

impl RedisBrokerConfig {
    pub fn new() -> Self {
        Self { key_prefix: DEFAULT_KEY_PREFIX.to_owned(), timeout: DEFAULT_TIMEOUT }
    }

    /// Renders the annotated TOML section.
    pub fn to_toml(&self) -> String {
        let mut out = String::from("# Prefix of the keys the Redis broker writes (--broker=redis)\n");
        out.push_str(&format!("key_prefix = {}\n", toml_config::go_quote(&self.key_prefix)));
        out.push_str("# Timeout of one Redis broker operation (seconds)\n");
        out.push_str(&format!("timeout = {}\n", self.timeout));
        out.push('\n');
        out
    }

    /// Checks the settings the broker can't work with.
    pub fn validate(&self) -> Result<(), BrokerError> {
        // A brace in the prefix would move the keys' Redis Cluster hash tag.
        if self.key_prefix.is_empty() || self.key_prefix.contains(['{', '}']) {
            return Err(err("--redis_broker_key_prefix must not be empty or contain '{' or '}'"));
        }
        if self.timeout < 1 {
            return Err(err("--redis_broker_timeout must be at least 1 second"));
        }
        Ok(())
    }
}

/// The broker's key layout (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Keys {
    prefix: String,
}

impl Keys {
    pub(crate) fn new(prefix: &str) -> Self {
        Self { prefix: prefix.to_owned() }
    }

    pub(crate) fn epoch(&self) -> String {
        format!("{}:epoch", self.prefix)
    }

    pub(crate) fn session(&self, sid: &str) -> String {
        format!("{}:sess:{sid}", self.prefix)
    }

    /// The stream's last assigned offset.
    pub(crate) fn seq(&self, stream: &str) -> String {
        format!("{}:{{s:{stream}}}:seq", self.prefix)
    }

    /// The stream's history (a Redis Stream).
    pub(crate) fn hist(&self, stream: &str) -> String {
        format!("{}:{{s:{stream}}}:hist", self.prefix)
    }
}

/// A stream as one read saw it: the Redis server's clock, whether the stream
/// still exists (its offset counter outlives its entries), and its newest
/// entries, newest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Snapshot {
    /// Unix seconds.
    pub now: i64,
    pub exists: bool,
    pub newest_first: Vec<Entry>,
}

impl Snapshot {
    /// The entries the memory broker would still hold, oldest first: the
    /// newest `limit`, without those older than `ttl` seconds.
    pub(crate) fn visible(&self, ttl: i64, limit: usize) -> Vec<Entry> {
        let cutoff = self.now.saturating_sub(ttl);
        let mut out: Vec<Entry> = self.newest_first.iter().take(limit).cloned().collect();
        out.reverse();
        // Like the memory broker's expiry: drop the oldest entries while they
        // are older than the TTL.
        let expired = out.iter().take_while(|e| e.timestamp < cutoff).count();
        out.drain(..expired);
        out
    }
}

/// The entries after `offset`, with the memory broker's errors when the
/// offset is no longer (or not yet) in the history.
pub(crate) fn entries_after(visible: &[Entry], offset: u64) -> Result<&[Entry], BrokerError> {
    let low = visible.first().map_or(0, |e| e.offset);
    if low > offset {
        return Err(err(format!("requested offset couldn't be found: {offset}, lowest: {low}")));
    }
    let Some(latest) = visible.last() else {
        return Err(err("stream is empty"));
    };
    if offset > latest.offset {
        return Err(err(format!("requested offset couldn't be found: {offset}, latest: {}", latest.offset)));
    }
    Ok(&visible[visible.partition_point(|e| e.offset <= offset)..])
}

fn unexpected(what: &str, v: &Value) -> BrokerError {
    err(format!("unexpected {what} reply from Redis: {v:?}"))
}

fn items(v: &Value) -> Option<&[Value]> {
    match v {
        Value::Array(items) => Some(items),
        _ => None,
    }
}

fn bytes(v: &Value) -> Option<&[u8]> {
    match v {
        Value::BulkString(b) => Some(b),
        Value::SimpleString(s) => Some(s.as_bytes()),
        _ => None,
    }
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(n) => Some(*n),
        other => std::str::from_utf8(bytes(other)?).ok()?.parse().ok(),
    }
}

/// The epoch script's reply: the epoch, and whether this call created it.
pub(crate) fn parse_epoch(v: &Value) -> Result<(String, bool), BrokerError> {
    let parsed = items(v).and_then(|i| match i {
        [epoch, created] => Some((String::from_utf8_lossy(bytes(epoch)?).into_owned(), int(created)? == 1)),
        _ => None,
    });
    parsed.ok_or_else(|| unexpected("epoch", v))
}

/// The add script's reply: the assigned offset.
pub(crate) fn parse_offset(v: &Value) -> Result<u64, BrokerError> {
    let offset = items(v).and_then(|i| i.first()).and_then(int).and_then(|n| u64::try_from(n).ok());
    offset.ok_or_else(|| unexpected("offset", v))
}

/// One `XRANGE`-style entry: `[<offset>-0, [field, value, ...]]`.
fn parse_entry(v: &Value) -> Option<Entry> {
    let [id, fields] = items(v)? else { return None };
    let id = std::str::from_utf8(bytes(id)?).ok()?;
    let offset = id.strip_suffix("-0")?.parse().ok()?;
    let (mut data, mut timestamp) = (None, None);
    for [field, value] in items(fields)?.as_chunks::<2>().0 {
        match bytes(field)? {
            b"d" => data = Some(String::from_utf8_lossy(bytes(value)?).into_owned()),
            b"t" => timestamp = int(value),
            _ => {}
        }
    }
    Some(Entry { offset, timestamp: timestamp?, data: data? })
}

/// The read script's reply.
pub(crate) fn parse_snapshot(v: &Value) -> Result<Snapshot, BrokerError> {
    let parsed = items(v).and_then(|i| match i {
        [now, exists, entries] => Some(Snapshot {
            now: int(now)?,
            exists: int(exists)? > 0,
            newest_first: items(entries)?.iter().map(parse_entry).collect::<Option<_>>()?,
        }),
        _ => None,
    });
    parsed.ok_or_else(|| unexpected("history", v))
}

/// `redis_broker_errors_total`: failed operations (timeouts included).
pub const METRIC_ERRORS: &str = "redis_broker_errors_total";
/// `redis_broker_timeouts_total`: operations that ran out of time.
pub const METRIC_TIMEOUTS: &str = "redis_broker_timeouts_total";
/// `redis_broker_reconnects_total`: connections opened again after a failure.
pub const METRIC_RECONNECTS: &str = "redis_broker_reconnects_total";
/// `redis_broker_epoch_rotations_total`: epochs created because the key was missing.
pub const METRIC_EPOCH_ROTATIONS: &str = "redis_broker_epoch_rotations_total";
/// `redis_broker_operation_ms`: the duration of the last operation.
pub const METRIC_OPERATION_MS: &str = "redis_broker_operation_ms";

/// How long a caller waits past the operation timeout for the Redis runtime
/// to report (it always does unless the runtime is wedged).
const REPORT_GRACE: Duration = Duration::from_secs(1);

/// The broker's logger (`context=broker provider=redis`).
fn log() -> &'static Logger {
    static LOG: OnceLock<Logger> = OnceLock::new();
    LOG.get_or_init(|| Logger::default().with("context", "broker").with("provider", "redis"))
}

fn read<T>(l: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(l: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(PoisonError::into_inner)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Seconds as a positive TTL (Redis rejects 0 and negative expiries).
fn ttl_secs(secs: i64) -> i64 {
    secs.max(1)
}

/// A count for `COUNT` / `MAXLEN` (Redis takes a signed 64-bit integer).
fn count_arg(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// The broker's Lua scripts. Each touches the keys of one Redis Cluster slot
/// (the epoch key, or one stream's keys).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lua {
    /// KEYS: epoch. ARGV: a new epoch. Returns `{epoch, created}`: the stored
    /// epoch, or the new one stored now when there was none (`SET NX`).
    Epoch,
    /// KEYS: seq, hist. ARGV: data, history limit, history TTL, offset TTL.
    /// Returns `{offset}`. A counter lost while its history stayed (an
    /// eviction) continues after the newest entry, so IDs keep increasing.
    Add,
    /// KEYS: seq, hist. ARGV: how many of the newest entries to read. Returns
    /// `{now, exists, entries}`, entries newest first.
    Read,
}

const EPOCH_LUA: &str = r#"
local current = redis.call('GET', KEYS[1])
if current then
  return {current, 0}
end
redis.call('SET', KEYS[1], ARGV[1])
return {ARGV[1], 1}
"#;

const ADD_LUA: &str = r#"
local now = tonumber(redis.call('TIME')[1])
local offset = redis.call('INCR', KEYS[1])
if offset == 1 then
  local top = redis.call('XREVRANGE', KEYS[2], '+', '-', 'COUNT', 1)[1]
  if top then
    local last = tonumber(string.match(top[1], '^%d+'))
    if last >= offset then
      offset = last + 1
      redis.call('SET', KEYS[1], string.format('%d', offset))
    end
  end
end
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], string.format('%d-0', offset), 'd', ARGV[1], 't', now)
redis.call('EXPIRE', KEYS[2], ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[4])
return {offset}
"#;

const READ_LUA: &str = r#"
local now = tonumber(redis.call('TIME')[1])
local exists = redis.call('EXISTS', KEYS[1], KEYS[2])
local entries = {}
if tonumber(ARGV[1]) > 0 then
  entries = redis.call('XREVRANGE', KEYS[2], '+', '-', 'COUNT', ARGV[1])
end
return {now, exists, entries}
"#;

impl Lua {
    const ALL: [Lua; 3] = [Lua::Epoch, Lua::Add, Lua::Read];

    fn body(self) -> &'static str {
        match self {
            Lua::Epoch => EPOCH_LUA,
            Lua::Add => ADD_LUA,
            Lua::Read => READ_LUA,
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// One command of an operation.
#[derive(Clone)]
enum Call {
    Eval { lua: Lua, keys: Vec<String>, args: Vec<Vec<u8>> },
    Cmd(redis::Cmd),
}

/// Why an operation failed, before it is described.
enum Failure {
    Closed,
    Timeout(Duration),
    Redis(redis::RedisError),
    Reply(BrokerError),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Closed => f.write_str("Redis broker is shut down"),
            Failure::Timeout(d) => write!(f, "operation timed out after {}s", d.as_secs()),
            Failure::Redis(e) => write!(f, "{e}"),
            Failure::Reply(e) => write!(f, "{e}"),
        }
    }
}

/// Whether the connection should be opened again after `e`.
fn is_connection_error(e: &redis::RedisError) -> bool {
    e.is_io_error() || e.is_timeout() || e.is_connection_dropped() || e.is_unrecoverable_error()
}

/// State shared with the operations running on the Redis runtime.
struct Inner {
    spec: ConnectSpec,
    keys: Keys,
    timeout: Duration,
    conn: Mutex<Option<Commands>>,
    /// Whether a connection was opened before (later ones are reconnects).
    connected: AtomicBool,
    /// `SCRIPT LOAD` digests by [`Lua::index`]; unknown ones are sent with `EVAL`.
    digests: [OnceLock<String>; 3],
    metrics: Option<Arc<dyn Instrumenter>>,
    closed: AtomicBool,
}

impl Inner {
    fn count(&self, name: &str) {
        if let Some(m) = &self.metrics {
            m.counter_increment(name);
        }
    }

    fn command(&self, call: &Call, cached: bool) -> redis::Cmd {
        match call {
            Call::Cmd(c) => c.clone(),
            Call::Eval { lua, keys, args } => {
                let mut c = match self.digests[lua.index()].get().filter(|_| cached) {
                    Some(sha) => {
                        let mut c = redis::cmd("EVALSHA");
                        c.arg(sha);
                        c
                    }
                    None => {
                        let mut c = redis::cmd("EVAL");
                        c.arg(lua.body());
                        c
                    }
                };
                c.arg(keys.len());
                for k in keys {
                    c.arg(k.as_bytes());
                }
                for a in args {
                    c.arg(a.as_slice());
                }
                c
            }
        }
    }

    async fn connection(&self) -> redis::RedisResult<Commands> {
        let existing = lock(&self.conn).clone();
        if let Some(c) = existing {
            return Ok(c);
        }
        let c = self.spec.commands(Some(self.timeout)).await?;
        if self.connected.swap(true, Ordering::SeqCst) {
            self.count(METRIC_RECONNECTS);
        }
        *lock(&self.conn) = Some(c.clone());
        Ok(c)
    }

    fn forget_connection(&self) {
        lock(&self.conn).take();
    }

    /// Runs one call, falling back to `EVAL` when Redis doesn't know the script.
    async fn exec_one(&self, conn: &mut Commands, call: &Call) -> redis::RedisResult<Value> {
        match self.command(call, true).query_async::<Value>(conn).await {
            Err(e) if e.code() == Some("NOSCRIPT") => self.command(call, false).query_async(conn).await,
            other => other,
        }
    }

    /// Runs the calls in order: pipelined on one node, one after the other
    /// in a cluster (their keys are in different slots).
    async fn exec_on(&self, conn: &mut Commands, calls: &[Call]) -> redis::RedisResult<Vec<Value>> {
        let mut out = Vec::with_capacity(calls.len());
        if calls.len() == 1 || matches!(conn, Commands::Cluster(_)) {
            for call in calls {
                out.push(self.exec_one(conn, call).await?);
            }
            return Ok(out);
        }
        let mut pipe = redis::pipe();
        pipe.ignore_errors();
        for call in calls {
            pipe.add_command(self.command(call, true));
        }
        let replies: Vec<redis::RedisResult<Value>> = pipe.query_async(conn).await?;
        for (call, reply) in calls.iter().zip(replies) {
            match reply {
                // Not run: safe to send again with the script's body.
                Err(e) if e.code() == Some("NOSCRIPT") => out.push(self.command(call, false).query_async(conn).await?),
                other => out.push(other?),
            }
        }
        Ok(out)
    }

    /// Runs the calls, on a new connection when there is none. Idempotent
    /// calls are tried once more on a new connection after a connection error.
    async fn exec(self: Arc<Self>, calls: Vec<Call>, idempotent: bool) -> redis::RedisResult<Vec<Value>> {
        let mut retried = false;
        loop {
            let mut conn = self.connection().await?;
            match self.exec_on(&mut conn, &calls).await {
                Err(e) if is_connection_error(&e) => {
                    self.forget_connection();
                    if idempotent && !retried {
                        retried = true;
                        continue;
                    }
                    return Err(e);
                }
                other => return other,
            }
        }
    }
}

/// Runs `fut` on the Redis runtime and waits for it at most `timeout`.
fn wait<F>(timeout: Duration, fut: F) -> Option<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    connect::runtime().spawn(async move {
        let _ = tx.send(fut.await);
    });
    cabby_core::nats::client::blocking(move || rx.recv_timeout(timeout).ok())
}

/// The Redis broker (see the module docs): multi-node stream history and
/// session cache in one Redis, shared by every node using the same key prefix.
pub struct RedisBroker {
    inner: Arc<Inner>,
    broadcaster: Option<Arc<dyn Broadcaster>>,
    conf: Config,
    tracker: StreamsTracker,
    /// The epoch the last operation saw (empty before [`Broker::start`]).
    epoch: RwLock<String>,
    announce: String,
}

impl RedisBroker {
    /// A broker on `redis` with `options`; nothing is sent to Redis before
    /// [`Broker::start`]. Fails on invalid options or Redis settings.
    pub fn new(
        broadcaster: Option<Arc<dyn Broadcaster>>,
        config: &Config,
        redis: &RedisConfig,
        options: &RedisBrokerConfig,
        metrics: Option<Arc<dyn Instrumenter>>,
    ) -> Result<Self, BrokerError> {
        options.validate()?;
        let spec =
            redis.connect_spec().map_err(|e| err(format!("failed to configure the Redis broker, cause: {e}")))?;
        let mut resolved = redis.clone();
        let _ = resolved.to_client_options();
        let announce = format!(
            "{} (key prefix: {}, history limit: {}, history ttl: {}s, sessions ttl: {}s)",
            crate::redis::pubsub_start_message("Using Redis broker", &resolved),
            options.key_prefix,
            config.history_limit,
            config.history_ttl,
            config.sessions_ttl,
        );
        if let Some(m) = &metrics {
            m.register_counter(METRIC_ERRORS, "The total number of failed Redis broker operations");
            m.register_counter(METRIC_TIMEOUTS, "The total number of Redis broker operations that timed out");
            m.register_counter(
                METRIC_RECONNECTS,
                "The total number of Redis broker connections opened after a failure",
            );
            m.register_counter(
                METRIC_EPOCH_ROTATIONS,
                "The total number of epochs the Redis broker created because the epoch key was missing",
            );
            m.register_gauge(METRIC_OPERATION_MS, "The duration of the last Redis broker operation (milliseconds)");
        }
        Ok(Self {
            inner: Arc::new(Inner {
                spec,
                keys: Keys::new(&options.key_prefix),
                timeout: Duration::from_secs(u64::try_from(options.timeout).unwrap_or(1)),
                conn: Mutex::new(None),
                connected: AtomicBool::new(false),
                digests: Default::default(),
                metrics,
                closed: AtomicBool::new(false),
            }),
            broadcaster,
            conf: config.clone(),
            tracker: StreamsTracker::new(),
            epoch: RwLock::new(String::new()),
            announce,
        })
    }

    /// The epoch the last operation saw.
    pub fn epoch(&self) -> String {
        read(&self.epoch).clone()
    }

    /// Runs the calls within the operation timeout.
    fn run(&self, calls: Vec<Call>, idempotent: bool) -> Result<Vec<Value>, Failure> {
        let inner = &self.inner;
        if inner.closed.load(Ordering::SeqCst) {
            return Err(Failure::Closed);
        }
        let timeout = inner.timeout;
        let task = inner.clone().exec(calls, idempotent);
        let result = wait(timeout + REPORT_GRACE, async move { tokio::time::timeout(timeout, task).await });
        match result {
            Some(Ok(Ok(values))) => Ok(values),
            Some(Ok(Err(e))) => Err(Failure::Redis(e)),
            Some(Err(_)) | None => {
                // A connection that doesn't answer is not used again.
                inner.forget_connection();
                inner.count(METRIC_TIMEOUTS);
                Err(Failure::Timeout(timeout))
            }
        }
    }

    /// One operation: runs `calls`, parses the replies and reports failures
    /// (logged, counted, and returned as `<what>, cause: <reason>`).
    fn op<T>(
        &self,
        what: impl FnOnce() -> String,
        calls: Vec<Call>,
        idempotent: bool,
        parse: impl FnOnce(&[Value]) -> Result<T, BrokerError>,
    ) -> Result<T, BrokerError> {
        let started = Instant::now();
        let result = self.run(calls, idempotent).and_then(|values| parse(&values).map_err(Failure::Reply));
        if let Some(m) = &self.inner.metrics {
            m.gauge_set(METRIC_OPERATION_MS, u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX));
        }
        result.map_err(|failure| {
            if matches!(failure, Failure::Closed) {
                return err(failure.to_string());
            }
            self.inner.count(METRIC_ERRORS);
            let message = format!("{}, cause: {failure}", what());
            log_error!(log(), &message);
            err(message)
        })
    }

    /// The epoch call that starts every offset-related operation.
    fn epoch_call(&self) -> Call {
        Call::Eval { lua: Lua::Epoch, keys: vec![self.inner.keys.epoch()], args: vec![connect::nanoid6().into_bytes()] }
    }

    /// Takes the epoch Redis returned; an epoch created outside start-up
    /// means the key was lost, so clients resync.
    fn observe_epoch(&self, epoch: &str, created: bool) {
        let changed = {
            let mut current = write(&self.epoch);
            let changed = *current != epoch;
            if changed {
                epoch.clone_into(&mut current);
            }
            changed
        };
        if created {
            self.inner.count(METRIC_EPOCH_ROTATIONS);
            log_warn!(log(), "the epoch key was missing, created a new epoch", "epoch" => epoch);
        } else if changed {
            log_warn!(log(), "epoch updated", "epoch" => epoch);
        }
    }

    /// The stream's epoch and its newest `count` entries.
    fn snapshot(&self, stream: &str, count: usize) -> Result<(String, Snapshot), BrokerError> {
        let keys = &self.inner.keys;
        let calls = vec![
            self.epoch_call(),
            Call::Eval {
                lua: Lua::Read,
                keys: vec![keys.seq(stream), keys.hist(stream)],
                args: vec![count_arg(count).to_string().into_bytes()],
            },
        ];
        let what = || format!("failed to read history from Redis: stream={stream}");
        let (epoch, created, snapshot) = self.op(what, calls, true, |v| {
            Ok((parse_epoch(&v[0])?, parse_snapshot(&v[1])?)).map(|((e, c), s)| (e, c, s))
        })?;
        self.observe_epoch(&epoch, created);
        Ok((epoch, snapshot))
    }

    fn message(stream: &str, epoch: &str, e: &Entry) -> StreamMessage {
        StreamMessage {
            stream: stream.to_owned(),
            data: e.data.clone(),
            offset: e.offset,
            epoch: epoch.to_owned(),
            meta: None,
        }
    }

    fn session_call(&self, cmd: &str, sid: &str) -> redis::Cmd {
        let mut c = redis::cmd(cmd);
        c.arg(self.inner.keys.session(sid));
        c
    }
}

impl Broker for RedisBroker {
    /// Loads the scripts and reads (or creates) the epoch; fails when Redis
    /// can't be reached within the timeout.
    fn start(&self) -> Result<(), BrokerError> {
        let mut calls: Vec<Call> = Lua::ALL
            .iter()
            .map(|lua| {
                let mut c = redis::cmd("SCRIPT");
                c.arg("LOAD").arg(lua.body());
                Call::Cmd(c)
            })
            .collect();
        calls.push(self.epoch_call());
        let (digests, (epoch, created)) = self.op(
            || "failed to start the Redis broker".to_owned(),
            calls,
            true,
            |v| {
                let digests = v[..Lua::ALL.len()]
                    .iter()
                    .map(|d| {
                        let text = bytes(d).and_then(|b| std::str::from_utf8(b).ok());
                        text.map(str::to_owned).ok_or_else(|| unexpected("SCRIPT LOAD", d))
                    })
                    .collect::<Result<Vec<String>, _>>()?;
                Ok((digests, parse_epoch(&v[Lua::ALL.len()])?))
            },
        )?;
        for (slot, digest) in self.inner.digests.iter().zip(digests) {
            let _ = slot.set(digest);
        }
        epoch.clone_into(&mut write(&self.epoch));
        let how = if created { "new" } else { "existing" };
        log_info!(log(), &format!("Redis broker is ready (epoch {epoch}, {how})"));
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.forget_connection();
        log_debug!(log(), "Redis broker is shut down");
        Ok(())
    }

    fn announce(&self) -> String {
        self.announce.clone()
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        if msg.meta.as_ref().is_some_and(|m| m.transient) {
            if let Some(b) = &self.broadcaster {
                b.broadcast(msg);
            }
            return Ok(());
        }
        let keys = &self.inner.keys;
        let ttl = ttl_secs(self.conf.history_ttl);
        let calls = vec![
            self.epoch_call(),
            Call::Eval {
                lua: Lua::Add,
                keys: vec![keys.seq(&msg.stream), keys.hist(&msg.stream)],
                args: vec![
                    msg.data.as_bytes().to_vec(),
                    count_arg(self.conf.history_limit).to_string().into_bytes(),
                    ttl.to_string().into_bytes(),
                    ttl.saturating_mul(10).to_string().into_bytes(),
                ],
            },
        ];
        let what = || format!("failed to add message to Redis stream: stream={}", msg.stream);
        // Not retried: the offset may have been assigned before a connection error.
        let ((epoch, created), offset) =
            self.op(what, calls, false, |v| Ok((parse_epoch(&v[0])?, parse_offset(&v[1])?)))?;
        self.observe_epoch(&epoch, created);
        let msg = StreamMessage { epoch, offset, ..msg.clone() };
        if let Some(b) = &self.broadcaster {
            b.broadcast(&msg);
        }
        Ok(())
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        if let Some(b) = &self.broadcaster {
            b.broadcast_command(msg);
        }
        Ok(())
    }

    fn subscribe(&self, stream: &str) -> String {
        self.tracker.add(stream, &mut || {
            if let Some(b) = &self.broadcaster {
                b.subscribe(stream);
            }
        });
        stream.to_owned()
    }

    fn unsubscribe(&self, stream: &str) -> String {
        self.tracker.remove(stream, &mut || {
            if let Some(b) = &self.broadcaster {
                b.unsubscribe(stream);
            }
        });
        stream.to_owned()
    }

    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        let limit = self.conf.history_limit;
        let (current, snapshot) = self.snapshot(stream, limit)?;
        if current != epoch {
            return Err(err(format!("unknown epoch: {epoch}, current: {current}")));
        }
        if !snapshot.exists {
            return Err(err("stream not found"));
        }
        let visible = snapshot.visible(self.conf.history_ttl, limit);
        Ok(entries_after(&visible, offset)?.iter().map(|e| Self::message(stream, &current, e)).collect())
    }

    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        let limit = self.conf.history_limit;
        let (epoch, snapshot) = self.snapshot(stream, limit)?;
        if !snapshot.exists {
            return Ok(None);
        }
        let visible = snapshot.visible(self.conf.history_ttl, limit);
        Ok(Some(visible.iter().filter(|e| e.timestamp >= ts).map(|e| Self::message(stream, &epoch, e)).collect()))
    }

    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let data = session.to_cache_entry().map_err(|e| err(format!("failed to serialize session, cause: {e}")))?;
        let mut c = self.session_call("SET", sid);
        c.arg(data).arg("EX").arg(ttl_secs(self.conf.sessions_ttl));
        self.op(|| "failed to save session to Redis".to_owned(), vec![Call::Cmd(c)], true, |_| Ok(()))
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        let c = self.session_call("GET", sid);
        self.op(
            || "failed to restore session from Redis".to_owned(),
            vec![Call::Cmd(c)],
            true,
            |v| match &v[0] {
                Value::Nil => Ok(None),
                other => bytes(other).map(|b| Some(b.to_vec())).ok_or_else(|| unexpected("session", other)),
            },
        )
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        let mut c = self.session_call("EXPIRE", sid);
        c.arg(ttl_secs(self.conf.sessions_ttl));
        self.op(|| "failed to touch session in Redis".to_owned(), vec![Call::Cmd(c)], true, |_| Ok(()))
    }

    /// Presence is not shared through Redis (yet): like the NATS broker, the
    /// Redis broker has none.
    fn supports_presence(&self) -> bool {
        false
    }

    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: serde_json::Value,
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
        let limit = self.conf.history_limit;
        let (epoch, snapshot) = self.snapshot(stream, limit.min(1))?;
        if !snapshot.exists {
            return Ok(None);
        }
        let visible = snapshot.visible(self.conf.history_ttl, limit.min(1));
        Ok(visible.last().map(|e| StreamMessage { offset: e.offset, epoch, ..Default::default() }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use cabby_core::broker::{Memory, SystemClock};
    use redis::cluster_routing::Slot;

    use super::*;

    fn entry(offset: u64, timestamp: i64, data: &str) -> Entry {
        Entry { offset, timestamp, data: data.to_owned() }
    }

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    fn broker(options: &RedisBrokerConfig, metrics: Option<Arc<dyn Instrumenter>>) -> Result<RedisBroker, BrokerError> {
        let mut config = Config::new();
        config.history_limit = 7;
        RedisBroker::new(None, &config, &RedisConfig::new(), options, metrics)
    }

    /// Phase one has no presence: the Redis broker answers as the NATS broker
    /// does, so the server treats both the same way (D-069).
    #[test]
    fn presence_is_not_supported_as_with_the_nats_broker() {
        let broker = broker(&RedisBrokerConfig::new(), None).unwrap();
        assert!(!broker.supports_presence());
        let unsupported = err("presence not supported");
        assert_eq!(unsupported, broker.presence_add("s", "sid", "pid", serde_json::Value::Null).unwrap_err());
        assert_eq!(unsupported, broker.presence_remove("s", "sid").unwrap_err());
        assert_eq!(unsupported, broker.presence_info("s", None).unwrap_err());
        assert_eq!(Ok(()), broker.touch_presence("sid"));
    }

    #[test]
    fn announce_names_the_redis_and_the_settings() {
        let broker = broker(&RedisBrokerConfig::new(), None).unwrap();
        assert_eq!(
            "Using Redis broker: localhost:6379 (key prefix: __cabby__, history limit: 7, history ttl: 300s, sessions ttl: 300s)",
            broker.announce()
        );
        assert_eq!("", broker.epoch(), "no epoch before start");
    }

    #[test]
    fn invalid_options_are_rejected_before_connecting() {
        let options = RedisBrokerConfig { key_prefix: "{app}".into(), timeout: 5 };
        assert!(broker(&options, None).is_err());
        let mut redis = RedisConfig::new();
        redis.url = "redis://localhost:notaport".into();
        let e = RedisBroker::new(None, &Config::new(), &redis, &RedisBrokerConfig::new(), None).err().unwrap();
        assert!(e.message.starts_with("failed to configure the Redis broker, cause: "), "{e}");
    }

    #[test]
    fn metrics_are_registered() {
        let m = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
        broker(&RedisBrokerConfig::new(), Some(m.clone())).unwrap();
        for counter in [METRIC_ERRORS, METRIC_TIMEOUTS, METRIC_RECONNECTS, METRIC_EPOCH_ROTATIONS] {
            assert_eq!(Some(0), m.counter(counter).map(|c| c.value()), "{counter}");
        }
        assert_eq!(Some(0), m.gauge(METRIC_OPERATION_MS).map(|g| g.value()));
        assert!(m.prometheus().contains("anycable_go_redis_broker_errors_total 0"), "{}", m.prometheus());
    }

    /// The scripts only touch the keys they are given (so Redis Cluster can
    /// route them, one slot each): every call but `TIME` names a `KEYS[n]`.
    #[test]
    fn scripts_touch_only_their_keys() {
        for lua in Lua::ALL {
            let calls: Vec<&str> = lua.body().split("redis.call('").skip(1).collect();
            assert!(!calls.is_empty(), "{lua:?}");
            for call in calls {
                let (command, rest) = call.split_once('\'').unwrap_or((call, ""));
                assert!(
                    command == "TIME" || rest.starts_with(", KEYS["),
                    "{lua:?} calls {command} on a key it wasn't given: {call}"
                );
            }
        }
    }

    #[test]
    fn keys_share_the_prefix_and_a_hash_tag_per_stream() {
        let keys = Keys::new("__cabby__");
        assert_eq!("__cabby__:epoch", keys.epoch());
        assert_eq!("__cabby__:sess:abc", keys.session("abc"));
        assert_eq!("__cabby__:{s:chat/1}:seq", keys.seq("chat/1"));
        assert_eq!("__cabby__:{s:chat/1}:hist", keys.hist("chat/1"));
    }

    /// A stream's keys land in one Redis Cluster slot whatever its name, so
    /// the scripts never touch two slots; different streams spread out.
    #[test]
    fn a_streams_keys_share_one_cluster_slot() {
        let keys = Keys::new("app");
        for stream in ["", "a", "a}b", "{x}", "a{b}c", "}", "{", "{}", "chat:1:{room}", "ünïcode"] {
            assert_eq!(
                Slot::for_key(keys.seq(stream)),
                Slot::for_key(keys.hist(stream)),
                "seq and hist of {stream:?} in different slots"
            );
            // Any other key of the stream would share it too: the tag lies in
            // the part the keys have in common, and is never empty.
            assert_eq!(Slot::for_key(keys.seq(stream)), Slot::for_key(format!("{}:other", keys.seq(stream))));
        }
        let slots: std::collections::HashSet<_> = (0..64).map(|i| Slot::for_key(keys.seq(&format!("s{i}")))).collect();
        assert!(slots.len() > 1, "streams are spread over slots");
    }

    #[test]
    fn a_prefix_with_braces_or_an_empty_prefix_is_rejected() {
        let mut c = RedisBrokerConfig::new();
        assert_eq!(Ok(()), c.validate());
        for bad in ["", "a{b", "a}b", "{app}"] {
            c.key_prefix = bad.into();
            assert!(c.validate().is_err(), "{bad:?}");
        }
        c.key_prefix = "app:prod".into();
        assert_eq!(Ok(()), c.validate());
        c.timeout = 0;
        assert_eq!("--redis_broker_timeout must be at least 1 second", c.validate().unwrap_err().message);
        c.key_prefix = String::new();
        c.timeout = 5;
        assert_eq!(
            "--redis_broker_key_prefix must not be empty or contain '{' or '}'",
            c.validate().unwrap_err().message
        );
    }

    #[test]
    fn config_toml_round_trip() {
        let c = RedisBrokerConfig::new();
        assert_eq!(
            "# Prefix of the keys the Redis broker writes (--broker=redis)\nkey_prefix = \"__cabby__\"\n\
             # Timeout of one Redis broker operation (seconds)\ntimeout = 5\n\n",
            c.to_toml()
        );
        let custom = RedisBrokerConfig { key_prefix: "app:\"x\"".into(), timeout: 9 };
        let mut back = RedisBrokerConfig::new();
        let unknown = toml_config::decode(&mut back, &custom.to_toml()).unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!(custom, back);
    }

    #[test]
    fn visible_entries_follow_limit_and_ttl() {
        let snap = Snapshot {
            now: 100,
            exists: true,
            newest_first: vec![entry(5, 100, "e"), entry(4, 99, "d"), entry(3, 90, "c"), entry(2, 80, "b")],
        };
        let offsets = |v: Vec<Entry>| v.into_iter().map(|e| e.offset).collect::<Vec<_>>();
        assert_eq!(vec![2, 3, 4, 5], offsets(snap.visible(60, 100)));
        assert_eq!(vec![4, 5], offsets(snap.visible(60, 2)), "the newest `limit`");
        assert_eq!(vec![3, 4, 5], offsets(snap.visible(10, 100)), "ts >= now - ttl stays");
        assert_eq!(vec![4, 5], offsets(snap.visible(1, 100)));
        assert!(snap.visible(60, 0).is_empty());
        assert!(Snapshot { now: 500, ..snap }.visible(60, 100).is_empty(), "everything expired");
    }

    /// `entries_after` answers exactly as the memory broker's `history_from`
    /// for every offset, including trimmed and future ones (same errors).
    #[test]
    fn entries_after_matches_the_memory_broker() {
        for limit in [1usize, 2, 3, 10] {
            let mut config = Config::new();
            config.history_limit = limit;
            let memory = Memory::new(None, None, &config, Arc::new(SystemClock));
            let mut all = Vec::new();
            for (i, data) in ["a", "b", "c", "d", "e"].iter().enumerate() {
                memory.add("s", data);
                all.push(entry(i as u64 + 1, 0, data));
            }
            let snap = Snapshot { now: 0, exists: true, newest_first: all.iter().rev().cloned().collect() };
            let visible = snap.visible(60, limit);
            for offset in 0..8 {
                let expected = memory
                    .history_from("s", &memory.epoch(), offset)
                    .map(|h| h.into_iter().map(|m| (m.offset, m.data)).collect::<Vec<_>>());
                let actual = entries_after(&visible, offset)
                    .map(|h| h.iter().map(|e| (e.offset, e.data.clone())).collect::<Vec<_>>());
                assert_eq!(expected, actual, "limit {limit}, offset {offset}");
            }
        }
        assert_eq!("stream is empty", entries_after(&[], 0).unwrap_err().message);
    }

    #[test]
    fn parses_script_replies() {
        let epoch = Value::Array(vec![bulk("abc123"), Value::Int(1)]);
        assert_eq!(("abc123".to_owned(), true), parse_epoch(&epoch).unwrap());
        let epoch = Value::Array(vec![bulk("abc123"), Value::Int(0)]);
        assert_eq!(("abc123".to_owned(), false), parse_epoch(&epoch).unwrap());
        assert!(parse_epoch(&Value::Nil).is_err());

        assert_eq!(42, parse_offset(&Value::Array(vec![Value::Int(42)])).unwrap());
        assert!(parse_offset(&Value::Array(vec![])).is_err());

        let fields = |d: &str, t: &str| Value::Array(vec![bulk("d"), bulk(d), bulk("t"), bulk(t)]);
        let reply = Value::Array(vec![
            Value::Int(1700000000),
            Value::Int(1),
            Value::Array(vec![
                Value::Array(vec![bulk("7-0"), fields("{\"x\":1}", "1699999999")]),
                // Field order doesn't matter.
                Value::Array(vec![
                    bulk("6-0"),
                    Value::Array(vec![bulk("t"), bulk("1699999998"), bulk("d"), bulk("b")]),
                ]),
            ]),
        ]);
        assert_eq!(
            Snapshot {
                now: 1700000000,
                exists: true,
                newest_first: vec![entry(7, 1699999999, "{\"x\":1}"), entry(6, 1699999998, "b")],
            },
            parse_snapshot(&reply).unwrap()
        );
        let missing = Value::Array(vec![Value::Int(5), Value::Int(0), Value::Array(vec![])]);
        assert_eq!(Snapshot { now: 5, exists: false, newest_first: vec![] }, parse_snapshot(&missing).unwrap());
        let bad_id = Value::Array(vec![
            Value::Int(5),
            Value::Int(1),
            Value::Array(vec![Value::Array(vec![bulk("x-0"), fields("a", "1")])]),
        ]);
        assert!(parse_snapshot(&bad_id).is_err());
    }
}
