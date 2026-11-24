//! The Redis broker (`--broker=redis`), a Cabby addition (deviation D-069 in
//! `compat/deviations.md` describes its observable behavior).
//!
//! Every node reads and writes the same Redis, so history and sessions are
//! shared without a node-local copy:
//!
//! * `<prefix>:{s:<stream>}:meta`: a hash with the stream's last offset, its
//!   epoch and the Redis instance (`run_id:master_replid`) that wrote it last.
//! * `<prefix>:{s:<stream>}:hist`: the stream's history, a Redis Stream with
//!   the explicit IDs `<offset>-0`; every entry records the epoch it was
//!   written in.
//! * `<prefix>:sess:<sid>`: a session's cached state.
//!
//! The hash tag keeps a stream's keys in one Redis Cluster slot, so one Lua
//! script assigns the offset, appends and sets the expiries atomically.
//!
//! Epochs are per stream. A stream gets a new one whenever its offsets could
//! repeat: when it is created, when its state was lost (an eviction, a
//! restart without persistence), and when the Redis instance that holds it
//! changed (a restart from a snapshot, which brings the counter back; a
//! failover, which may lose the newest writes). Old entries keep their old
//! epoch and are never served under the new one, so a client holding an old
//! position gets `unknown epoch` and resyncs instead of missing messages.
//!
//! History reads apply the memory broker's contract: the newest
//! `history_limit` entries (`MAXLEN ~` may keep more), without the entries
//! older than `history_ttl`. Timestamps come from the Redis server's clock
//! (`TIME`), so every node agrees on what has expired.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

/// The Redis commands the broker uses, as ACL rules (`ACL SETUSER <user>
/// ... <rules>`); with `~<prefix>:*` they are all a broker's user needs.
/// `SELECT` is needed only for a database other than 0.
pub const ACL_COMMANDS: &str =
    "+eval +evalsha +script|load +info +time +hmget +hset +exists +xinfo|stream +xadd +xrevrange +expire +get +set";

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

    pub(crate) fn session(&self, sid: &str) -> String {
        format!("{}:sess:{sid}", self.prefix)
    }

    /// The stream's state: last offset, epoch and Redis instance.
    pub(crate) fn meta(&self, stream: &str) -> String {
        format!("{}:{{s:{stream}}}:meta", self.prefix)
    }

    /// The stream's history (a Redis Stream).
    pub(crate) fn hist(&self, stream: &str) -> String {
        format!("{}:{{s:{stream}}}:hist", self.prefix)
    }
}

/// A stream as one read saw it. `low` and `latest` bound the entries the
/// memory broker would still hold (the newest `history_limit` of the current
/// epoch, none older than `history_ttl`; 0 when there are none); `entries`
/// are those of them the read asked for, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Snapshot {
    pub exists: bool,
    pub epoch: String,
    pub low: u64,
    pub latest: u64,
    pub entries: Vec<Entry>,
}

impl Snapshot {
    /// `history_from`: the entries after `offset` (the read asked only for
    /// those), with the memory broker's errors when the epoch is not the
    /// stream's or the offset is no longer (or not yet) in the history.
    pub(crate) fn after(self, epoch: &str, offset: u64) -> Result<Vec<Entry>, BrokerError> {
        if !self.exists {
            return Err(err("stream not found"));
        }
        if self.epoch != epoch {
            return Err(err(format!("unknown epoch: {epoch}, current: {}", self.epoch)));
        }
        if self.low > offset {
            return Err(err(format!("requested offset couldn't be found: {offset}, lowest: {}", self.low)));
        }
        if self.latest == 0 {
            return Err(err("stream is empty"));
        }
        if offset > self.latest {
            return Err(err(format!("requested offset couldn't be found: {offset}, latest: {}", self.latest)));
        }
        Ok(self.entries)
    }
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

fn text(v: &Value) -> Option<String> {
    bytes(v).map(|b| String::from_utf8_lossy(b).into_owned())
}

fn int(v: &Value) -> Option<i64> {
    match v {
        Value::Int(n) => Some(*n),
        other => std::str::from_utf8(bytes(other)?).ok()?.parse().ok(),
    }
}

fn uint(v: &Value) -> Option<u64> {
    int(v).and_then(|n| u64::try_from(n).ok())
}

/// What a broadcast did to the stream's epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EpochChange {
    /// The stream continued in its epoch.
    Kept,
    /// A new stream, in a new epoch.
    NewStream,
    /// The stream's state was lost or its Redis instance changed: new epoch.
    Rotated,
}

/// The add script's reply: the offset, the epoch and what happened to it.
pub(crate) fn parse_added(v: &Value) -> Result<(u64, String, EpochChange), BrokerError> {
    let parsed = items(v).and_then(|i| match i {
        [offset, epoch, change] => {
            let change = match int(change)? {
                0 => EpochChange::Kept,
                1 => EpochChange::NewStream,
                2 => EpochChange::Rotated,
                _ => return None,
            };
            Some((uint(offset)?, text(epoch)?, change))
        }
        _ => None,
    });
    parsed.ok_or_else(|| unexpected("broadcast", v))
}

/// One entry of the read script's reply: `[offset, ts, data]`.
fn parse_entry(v: &Value) -> Option<Entry> {
    let [offset, timestamp, data] = items(v)? else { return None };
    Some(Entry { offset: uint(offset)?, timestamp: int(timestamp)?, data: text(data)? })
}

/// The read script's reply: `[exists, epoch, low, latest, entries]`, entries
/// newest first.
pub(crate) fn parse_snapshot(v: &Value) -> Result<Snapshot, BrokerError> {
    let parsed = items(v).and_then(|i| match i {
        [exists, epoch, low, latest, entries] => {
            let mut entries = items(entries)?.iter().map(parse_entry).collect::<Option<Vec<_>>>()?;
            entries.reverse();
            Some(Snapshot {
                exists: int(exists)? > 0,
                epoch: text(epoch).unwrap_or_default(),
                low: uint(low)?,
                latest: uint(latest)?,
                entries,
            })
        }
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
/// `redis_broker_epoch_rotations_total`: streams that got a new epoch because
/// their state was lost or their Redis instance changed.
pub const METRIC_EPOCH_ROTATIONS: &str = "redis_broker_epoch_rotations_total";
/// `redis_broker_operation_ms`: the duration of the last operation.
pub const METRIC_OPERATION_MS: &str = "redis_broker_operation_ms";

/// How long a caller waits past the operation timeout for the Redis runtime
/// to report (it always does unless the runtime is wedged).
const REPORT_GRACE: Duration = Duration::from_secs(1);
/// The least time between two warnings about rotated epochs (a restart
/// rotates every stream it holds, at its next broadcast).
const ROTATION_WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// The broker's logger (`context=broker provider=redis`).
fn log() -> &'static Logger {
    static LOG: OnceLock<Logger> = OnceLock::new();
    LOG.get_or_init(|| Logger::default().with("context", "broker").with("provider", "redis"))
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

fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

/// The broker's Lua scripts. Each touches the keys of one stream (one Redis
/// Cluster slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lua {
    /// KEYS: meta, hist. ARGV: data, history limit, history TTL, state TTL,
    /// a new epoch. Returns `{offset, epoch, change}` (see [`EpochChange`]).
    Add,
    /// KEYS: meta, hist. ARGV: history limit, history TTL, `after` (offset),
    /// `since` (unix seconds), the client's epoch (empty: any). Returns
    /// `{exists, epoch, low, latest, entries}`: the window of the current
    /// epoch, and its entries after `after`, at or after `since`, newest
    /// first (none when the client's epoch is not the stream's).
    Read,
}

/// The instance is the Redis process (`run_id`, new at every start) and its
/// replication history (`master_replid`, new after a failover).
const ADD_LUA: &str = r#"
local function info(section, name)
  return string.match(redis.call('INFO', section), name .. ':(%x+)') or ''
end
local instance = info('server', 'run_id') .. ':' .. info('replication', 'master_replid')
local now = tonumber(redis.call('TIME')[1])
local meta = redis.call('HMGET', KEYS[1], 'offset', 'epoch', 'instance')
local offset = tonumber(meta[1])
local epoch = meta[2]
local change = 0
if not offset or not epoch or meta[3] ~= instance then
  -- A new epoch; offsets continue after the newest one the history saw.
  change = (offset and epoch) and 2 or 1
  offset = offset or 0
  epoch = ARGV[5]
  if redis.call('EXISTS', KEYS[2]) == 1 then
    local stream = redis.call('XINFO', 'STREAM', KEYS[2])
    for i = 1, #stream, 2 do
      if stream[i] == 'last-generated-id' then
        offset = math.max(offset, tonumber(string.match(stream[i + 1], '^%d+')))
      end
    end
    change = 2
  end
end
offset = offset + 1
redis.call('HSET', KEYS[1], 'offset', string.format('%d', offset), 'epoch', epoch, 'instance', instance)
redis.call('EXPIRE', KEYS[1], ARGV[4])
if tonumber(ARGV[2]) > 0 then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], string.format('%d-0', offset), 'd', ARGV[1], 't', now, 'e', epoch)
  redis.call('EXPIRE', KEYS[2], ARGV[3])
end
return {offset, epoch, change}
"#;

const READ_LUA: &str = r#"
local meta = redis.call('HMGET', KEYS[1], 'offset', 'epoch')
local epoch = meta[2]
if not meta[1] or not epoch then
  return {0, '', 0, 0, {}}
end
local cutoff = tonumber(redis.call('TIME')[1]) - tonumber(ARGV[2])
local after = tonumber(ARGV[3])
local since = tonumber(ARGV[4])
local wanted = ARGV[5] == '' or ARGV[5] == epoch
local entries = {}
if tonumber(ARGV[1]) > 0 then
  entries = redis.call('XREVRANGE', KEYS[2], '+', '-', 'COUNT', ARGV[1])
end
local low, latest, out = 0, 0, {}
for _, entry in ipairs(entries) do
  local fields = {}
  for i = 1, #entry[2], 2 do
    fields[entry[2][i]] = entry[2][i + 1]
  end
  local ts = tonumber(fields['t'])
  if fields['e'] ~= epoch or ts < cutoff then
    break
  end
  local offset = tonumber(string.match(entry[1], '^%d+'))
  if latest == 0 then
    latest = offset
  end
  low = offset
  if wanted and offset > after and ts >= since then
    out[#out + 1] = {offset, ts, fields['d']}
  end
end
return {1, epoch, low, latest, out}
"#;

impl Lua {
    const ALL: [Lua; 2] = [Lua::Add, Lua::Read];

    fn body(self) -> &'static str {
        match self {
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
    digests: [OnceLock<String>; 2],
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
    /// in a cluster (their keys may be in different slots).
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

/// The Redis broker (see the module docs): multi-node stream history and
/// session cache in one Redis, shared by every node using the same key prefix.
pub struct RedisBroker {
    inner: Arc<Inner>,
    broadcaster: Option<Arc<dyn Broadcaster>>,
    conf: Config,
    tracker: StreamsTracker,
    /// When the last rotation warning was logged (unix seconds, 0: never).
    rotation_warned: AtomicU64,
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
                "The total number of Redis broker connections opened again after a failure",
            );
            m.register_counter(
                METRIC_EPOCH_ROTATIONS,
                "The total number of streams the Redis broker gave a new epoch because their state was lost or their Redis instance changed",
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
            rotation_warned: AtomicU64::new(0),
            announce,
        })
    }

    /// Runs the calls within the operation timeout.
    fn run(&self, calls: Vec<Call>, idempotent: bool) -> Result<Vec<Value>, Failure> {
        let inner = &self.inner;
        if inner.closed.load(Ordering::SeqCst) {
            return Err(Failure::Closed);
        }
        let timeout = inner.timeout;
        let expected = calls.len();
        let task = inner.clone().exec(calls, idempotent);
        let result = connect::wait_on(timeout + REPORT_GRACE, async move { tokio::time::timeout(timeout, task).await });
        match result {
            Some(Ok(Ok(values))) if values.len() == expected => Ok(values),
            Some(Ok(Ok(values))) => {
                Err(Failure::Reply(err(format!("expected {expected} replies from Redis, got {}", values.len()))))
            }
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

    /// Counts a rotated epoch; warns at most once a minute.
    fn rotated(&self, stream: &str, epoch: &str) {
        self.inner.count(METRIC_EPOCH_ROTATIONS);
        let now = unix_secs();
        let last = self.rotation_warned.load(Ordering::SeqCst);
        if now.saturating_sub(last) >= ROTATION_WARNING_INTERVAL.as_secs()
            && self.rotation_warned.compare_exchange(last, now, Ordering::SeqCst, Ordering::SeqCst).is_ok()
        {
            log_warn!(
                log(),
                "a stream got a new epoch: its state was lost or its Redis instance changed (a restart, a failover); clients resync",
                "stream" => stream,
                "epoch" => epoch
            );
        } else {
            log_debug!(log(), "a stream got a new epoch", "stream" => stream, "epoch" => epoch);
        }
    }

    /// The stream's window and the entries a read asks for (see [`Lua::Read`]).
    fn snapshot(
        &self,
        stream: &str,
        limit: usize,
        after: u64,
        since: i64,
        epoch: &str,
    ) -> Result<Snapshot, BrokerError> {
        let keys = &self.inner.keys;
        let call = Call::Eval {
            lua: Lua::Read,
            keys: vec![keys.meta(stream), keys.hist(stream)],
            args: vec![
                count_arg(limit).to_string().into_bytes(),
                self.conf.history_ttl.to_string().into_bytes(),
                after.to_string().into_bytes(),
                since.to_string().into_bytes(),
                epoch.as_bytes().to_vec(),
            ],
        };
        let what = || format!("failed to read history from Redis: stream={stream}");
        self.op(what, vec![call], true, |v| parse_snapshot(&v[0]))
    }

    fn message(stream: &str, epoch: &str, e: Entry) -> StreamMessage {
        StreamMessage { stream: stream.to_owned(), data: e.data, offset: e.offset, epoch: epoch.to_owned(), meta: None }
    }

    fn session_call(&self, cmd: &str, sid: &str) -> redis::Cmd {
        let mut c = redis::cmd(cmd);
        c.arg(self.inner.keys.session(sid));
        c
    }
}

impl Broker for RedisBroker {
    /// Loads the scripts; fails when Redis can't be reached within the timeout.
    fn start(&self) -> Result<(), BrokerError> {
        let mut calls: Vec<Call> = Lua::ALL
            .iter()
            .map(|lua| {
                let mut c = redis::cmd("SCRIPT");
                c.arg("LOAD").arg(lua.body());
                Call::Cmd(c)
            })
            .collect();
        let mut info = redis::cmd("INFO");
        info.arg("server");
        calls.push(Call::Cmd(info));
        let (digests, version) = self.op(
            || "failed to start the Redis broker".to_owned(),
            calls,
            true,
            |v| {
                let digests = v[..Lua::ALL.len()]
                    .iter()
                    .map(|d| text(d).ok_or_else(|| unexpected("SCRIPT LOAD", d)))
                    .collect::<Result<Vec<String>, _>>()?;
                let info = text(&v[Lua::ALL.len()]).unwrap_or_default();
                let version = info.lines().find_map(|l| l.strip_prefix("redis_version:")).unwrap_or("unknown");
                Ok((digests, version.trim().to_owned()))
            },
        )?;
        for (slot, digest) in self.inner.digests.iter().zip(digests) {
            let _ = slot.set(digest);
        }
        log_info!(log(), &format!("Redis broker is ready (Redis {version}, epochs per stream)"));
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
        let call = Call::Eval {
            lua: Lua::Add,
            keys: vec![keys.meta(&msg.stream), keys.hist(&msg.stream)],
            args: vec![
                msg.data.as_bytes().to_vec(),
                count_arg(self.conf.history_limit).to_string().into_bytes(),
                ttl.to_string().into_bytes(),
                ttl.saturating_mul(10).to_string().into_bytes(),
                connect::nanoid6().into_bytes(),
            ],
        };
        let what = || format!("failed to add message to Redis stream: stream={}", msg.stream);
        // Not retried: the offset may have been assigned before a connection error.
        let (offset, epoch, change) = self.op(what, vec![call], false, |v| parse_added(&v[0]))?;
        if change == EpochChange::Rotated {
            self.rotated(&msg.stream, &epoch);
        }
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
        let snapshot = self.snapshot(stream, self.conf.history_limit, offset, i64::MIN, epoch)?;
        let current = snapshot.epoch.clone();
        Ok(snapshot.after(epoch, offset)?.into_iter().map(|e| Self::message(stream, &current, e)).collect())
    }

    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        let snapshot = self.snapshot(stream, self.conf.history_limit, 0, ts, "")?;
        if !snapshot.exists {
            return Ok(None);
        }
        let epoch = snapshot.epoch;
        Ok(Some(snapshot.entries.into_iter().map(|e| Self::message(stream, &epoch, e)).collect()))
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
        // The newest entry decides: no entries are needed back.
        let snapshot = self.snapshot(stream, self.conf.history_limit.min(1), u64::MAX, i64::MIN, "")?;
        if !snapshot.exists || snapshot.latest == 0 {
            return Ok(None);
        }
        Ok(Some(StreamMessage { offset: snapshot.latest, epoch: snapshot.epoch, ..Default::default() }))
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
    /// route them, one slot each): every call but `TIME` and `INFO` names a
    /// `KEYS[n]`, and the commands they use are in [`ACL_COMMANDS`].
    #[test]
    fn scripts_touch_only_their_keys_with_the_documented_commands() {
        for lua in Lua::ALL {
            let calls: Vec<&str> = lua.body().split("redis.call(").skip(1).collect();
            assert!(!calls.is_empty(), "{lua:?}");
            for call in calls {
                let args = call.split(')').next().unwrap_or("");
                let command = args.split(',').next().unwrap_or("").trim_matches(['\'', ' ']).to_lowercase();
                let rule = match command.as_str() {
                    "xinfo" => "+xinfo|stream".to_owned(),
                    // `INFO` gets its section from a variable.
                    "" | "section" => "+info".to_owned(),
                    other => format!("+{other}"),
                };
                assert!(ACL_COMMANDS.split(' ').any(|r| r == rule), "{lua:?} uses {rule}, not in ACL_COMMANDS");
                assert!(
                    matches!(rule.as_str(), "+time" | "+info") || args.contains("KEYS["),
                    "{lua:?} calls {command} on a key it wasn't given: {call}"
                );
            }
        }
    }

    #[test]
    fn keys_share_the_prefix_and_a_hash_tag_per_stream() {
        let keys = Keys::new("__cabby__");
        assert_eq!("__cabby__:sess:abc", keys.session("abc"));
        assert_eq!("__cabby__:{s:chat/1}:meta", keys.meta("chat/1"));
        assert_eq!("__cabby__:{s:chat/1}:hist", keys.hist("chat/1"));
    }

    /// A stream's keys land in one Redis Cluster slot whatever its name, so
    /// the scripts never touch two slots; different streams spread out.
    #[test]
    fn a_streams_keys_share_one_cluster_slot() {
        let keys = Keys::new("app");
        for stream in ["", "a", "a}b", "{x}", "a{b}c", "}", "{", "{}", "chat:1:{room}", "ünïcode"] {
            assert_eq!(
                Slot::for_key(keys.meta(stream)),
                Slot::for_key(keys.hist(stream)),
                "meta and hist of {stream:?} in different slots"
            );
            // Any other key of the stream would share it too: the tag lies in
            // the part the keys have in common, and is never empty.
            assert_eq!(Slot::for_key(keys.meta(stream)), Slot::for_key(format!("{}:other", keys.meta(stream))));
        }
        let slots: std::collections::HashSet<_> = (0..64).map(|i| Slot::for_key(keys.meta(&format!("s{i}")))).collect();
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

    /// What the read script returns for `visible` (the window, oldest first)
    /// when asked for the entries after `after`.
    fn read_reply(epoch: &str, visible: &[Entry], after: u64) -> Snapshot {
        Snapshot {
            exists: true,
            epoch: epoch.to_owned(),
            low: visible.first().map_or(0, |e| e.offset),
            latest: visible.last().map_or(0, |e| e.offset),
            entries: visible.iter().filter(|e| e.offset > after).cloned().collect(),
        }
    }

    /// `Snapshot::after` answers exactly as the memory broker's `history_from`
    /// for every offset, including trimmed and future ones (same errors).
    #[test]
    fn history_from_errors_match_the_memory_broker() {
        for limit in [1usize, 2, 3, 10] {
            let mut config = Config::new();
            config.history_limit = limit;
            let memory = Memory::new(None, None, &config, Arc::new(SystemClock));
            let mut all = Vec::new();
            for (i, data) in ["a", "b", "c", "d", "e"].iter().enumerate() {
                memory.add("s", data);
                all.push(entry(i as u64 + 1, 0, data));
            }
            let visible = &all[all.len().saturating_sub(limit)..];
            for offset in 0..8 {
                let expected = memory
                    .history_from("s", &memory.epoch(), offset)
                    .map(|h| h.into_iter().map(|m| (m.offset, m.data)).collect::<Vec<_>>());
                let actual = read_reply("e1", visible, offset)
                    .after("e1", offset)
                    .map(|h| h.into_iter().map(|e| (e.offset, e.data)).collect::<Vec<_>>());
                assert_eq!(expected, actual, "limit {limit}, offset {offset}");
            }
        }
        assert_eq!("stream is empty", read_reply("e1", &[], 0).after("e1", 0).unwrap_err().message);
        assert_eq!("stream not found", Snapshot::default().after("e1", 0).unwrap_err().message);
        assert_eq!(
            "unknown epoch: e0, current: e1",
            read_reply("e1", &[entry(1, 0, "a")], 0).after("e0", 0).unwrap_err().message
        );
    }

    #[test]
    fn parses_script_replies() {
        let added = Value::Array(vec![Value::Int(42), bulk("abc123"), Value::Int(2)]);
        assert_eq!((42, "abc123".to_owned(), EpochChange::Rotated), parse_added(&added).unwrap());
        let added = Value::Array(vec![Value::Int(1), bulk("abc123"), Value::Int(1)]);
        assert_eq!(EpochChange::NewStream, parse_added(&added).unwrap().2);
        assert!(parse_added(&Value::Array(vec![Value::Int(1), bulk("e"), Value::Int(7)])).is_err());
        assert!(parse_added(&Value::Nil).is_err());

        let reply = Value::Array(vec![
            Value::Int(1),
            bulk("e1"),
            Value::Int(5),
            Value::Int(7),
            Value::Array(vec![
                Value::Array(vec![Value::Int(7), Value::Int(1699999999), bulk("{\"x\":1}")]),
                Value::Array(vec![Value::Int(6), Value::Int(1699999998), bulk("b")]),
            ]),
        ]);
        assert_eq!(
            Snapshot {
                exists: true,
                epoch: "e1".into(),
                low: 5,
                latest: 7,
                entries: vec![entry(6, 1699999998, "b"), entry(7, 1699999999, "{\"x\":1}")],
            },
            parse_snapshot(&reply).unwrap()
        );
        let missing = Value::Array(vec![Value::Int(0), bulk(""), Value::Int(0), Value::Int(0), Value::Array(vec![])]);
        assert_eq!(Snapshot::default(), parse_snapshot(&missing).unwrap());
        let bad = Value::Array(vec![Value::Int(1), bulk("e"), Value::Int(0), Value::Int(0), Value::Int(3)]);
        assert!(parse_snapshot(&bad).is_err());
    }
}
