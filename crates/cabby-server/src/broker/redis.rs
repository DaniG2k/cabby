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
//! failover, which may lose the newest writes). Positional reads
//! (`history_from`) only serve entries of the current epoch, and none while
//! the stream's instance differs from the one it was written on, so a client
//! holding an old position gets `unknown epoch` and resyncs instead of
//! missing messages. `history_since` returns every kept entry with its own
//! epoch.
//!
//! The instance (`run_id:master_replid`) is read with `INFO` by the broker
//! for each connection to a single node (and at most once a second while it
//! is used), and passed to the scripts; on a Redis Cluster connection, which
//! spans many nodes, the scripts read it on the node they run on.
//!
//! History reads apply the memory broker's contract: the newest
//! `history_limit` entries (`MAXLEN ~` may keep more), without the entries
//! older than `history_ttl`. Timestamps come from the Redis server's clock
//! (`TIME`), so every node agrees on what has expired.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cabby_core::broker::{Broadcaster, Broker, BrokerError, Cacheable, Config, PresenceInfoOptions, StreamsTracker};
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

/// A stored history entry with the epoch it was written in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Item {
    pub offset: u64,
    /// Unix seconds (the Redis server's clock).
    pub timestamp: i64,
    pub data: String,
    pub epoch: String,
}

/// A stream as one read saw it. `low` and `latest` bound the entries the
/// memory broker would still hold (the newest `history_limit` of the current
/// epoch, none older than `history_ttl`; 0 when there are none); `entries`
/// are those the read asked for, oldest first. `stale`: the stream was
/// written on another Redis instance than the one it is on now.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Snapshot {
    pub exists: bool,
    pub epoch: String,
    pub low: u64,
    pub latest: u64,
    pub entries: Vec<Item>,
    pub stale: bool,
}

impl Snapshot {
    /// `history_from`: the entries after `offset` (the read asked only for
    /// those), with the memory broker's errors when the epoch is not the
    /// stream's or the offset is no longer (or not yet) in the history. A
    /// stale stream knows no position: its offsets may have been issued
    /// again since.
    pub(crate) fn after(self, epoch: &str, offset: u64) -> Result<Vec<Item>, BrokerError> {
        if !self.exists {
            return Err(err("stream not found"));
        }
        if self.stale {
            return Err(err(format!("unknown epoch: {epoch}, current: {} (the Redis instance changed)", self.epoch)));
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
        Value::VerbatimString { text, .. } => Some(text.as_bytes()),
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
    /// The stream continued in its epoch (or the broadcast was a resend).
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

/// One entry of the read script's reply: `[offset, ts, data, epoch]`.
fn parse_item(v: &Value) -> Option<Item> {
    let [offset, timestamp, data, epoch] = items(v)? else { return None };
    Some(Item { offset: uint(offset)?, timestamp: int(timestamp)?, data: text(data)?, epoch: text(epoch)? })
}

/// The read script's reply: `[exists, epoch, low, latest, entries, stale]`,
/// entries newest first.
pub(crate) fn parse_snapshot(v: &Value) -> Result<Snapshot, BrokerError> {
    let parsed = items(v).and_then(|i| match i {
        [exists, epoch, low, latest, entries, stale] => {
            let mut entries = items(entries)?.iter().map(parse_item).collect::<Option<Vec<_>>>()?;
            entries.reverse();
            Some(Snapshot {
                exists: int(exists)? > 0,
                epoch: text(epoch).unwrap_or_default(),
                low: uint(low)?,
                latest: uint(latest)?,
                entries,
                stale: int(stale)? > 0,
            })
        }
        _ => None,
    });
    parsed.ok_or_else(|| unexpected("history", v))
}

/// The Redis instance in `INFO` output: `run_id:master_replid`, and whether
/// both were there (without them a restart or failover goes unnoticed).
pub(crate) fn parse_instance(info: &str) -> (String, bool) {
    let field = |name: &str| {
        info.lines()
            .find_map(|l| l.strip_prefix(name)?.strip_prefix(':'))
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    let (run_id, replid) = (field("run_id"), field("master_replid"));
    let complete = run_id.is_some() && replid.is_some();
    (format!("{}:{}", run_id.unwrap_or_default(), replid.unwrap_or_default()), complete)
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
/// How often the instance of a single-node connection is read again while
/// the connection is used.
const INSTANCE_REFRESH: Duration = Duration::from_secs(1);
/// How often a failure during the back-off is logged (the others are
/// counted in the next record).
const UNAVAILABLE_LOG_INTERVAL: Duration = Duration::from_secs(1);

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

/// A random broadcast token (16 bytes, hex).
fn token() -> String {
    let mut b = [0u8; 16];
    let _ = getrandom::fill(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The broker's Lua scripts. Each touches the keys of one stream (one Redis
/// Cluster slot). Both take the Redis instance as their last argument; an
/// empty one (a cluster connection) makes them read it on their node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lua {
    /// See [`ADD_LUA`].
    Add,
    /// See [`READ_LUA`].
    Read,
}

/// The node's instance, read in a script. Two single-section `INFO` calls:
/// they work on every Redis version and measured faster than one
/// `INFO server replication` (Redis 8.12, 16 clients).
macro_rules! instance_lua {
    () => {
        r#"
local function current_instance()
  local server = redis.call('INFO', 'server')
  local replication = redis.call('INFO', 'replication')
  return (string.match(server, 'run_id:(%x+)') or '') .. ':' .. (string.match(replication, 'master_replid:(%x+)') or '')
end
"#
    };
}

/// The broadcast script. KEYS: meta, hist. ARGV: data, history limit,
/// history TTL, state TTL, a new epoch, the broadcast's token, the instance.
/// Returns `{offset, epoch, change}` (see [`EpochChange`]). A resend of the
/// last broadcast (same token) returns its offset and epoch and stores
/// nothing. Public for tests.
pub const ADD_LUA: &str = concat!(
    instance_lua!(),
    r#"
local meta = redis.call('HMGET', KEYS[1], 'offset', 'epoch', 'instance', 'token')
if meta[1] and meta[2] and meta[4] == ARGV[6] then
  return {tonumber(meta[1]), meta[2], 0}
end
local instance = ARGV[7]
if instance == '' then
  instance = current_instance()
end
local now = tonumber(redis.call('TIME')[1])
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
redis.call('HSET', KEYS[1], 'offset', string.format('%d', offset), 'epoch', epoch, 'instance', instance, 'token', ARGV[6])
redis.call('EXPIRE', KEYS[1], ARGV[4])
if tonumber(ARGV[2]) > 0 then
  redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], string.format('%d-0', offset), 'd', ARGV[1], 't', now, 'e', epoch)
  redis.call('EXPIRE', KEYS[2], ARGV[3])
end
return {offset, epoch, change}
"#
);

/// The read script. KEYS: meta, hist. ARGV: history limit, history TTL,
/// mode (`from`, `since` or `peak`), `after` (offset), `since` (unix
/// seconds), the client's epoch, the instance. Returns `{exists, epoch, low,
/// latest, entries, stale}`: the window of the current epoch, and the
/// entries asked for, newest first, as `{offset, ts, data, epoch}`: `from`
/// those of the window after `after` (none when the client's epoch is not
/// the stream's, or the stream is stale: written on another instance);
/// `since` every kept entry at or after `since`, whatever its epoch.
pub const READ_LUA: &str = concat!(
    instance_lua!(),
    r#"
local meta = redis.call('HMGET', KEYS[1], 'offset', 'epoch', 'instance')
local epoch = meta[2]
if not meta[1] or not epoch then
  return {0, '', 0, 0, {}, 0}
end
local mode = ARGV[3]
if mode == 'from' then
  local instance = ARGV[7]
  if instance == '' then
    instance = current_instance()
  end
  if meta[3] ~= instance then
    return {1, epoch, 0, 0, {}, 1}
  end
end
local cutoff = tonumber(redis.call('TIME')[1]) - tonumber(ARGV[2])
local after = tonumber(ARGV[4])
local since = tonumber(ARGV[5])
local wanted = ARGV[6] == epoch
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
  if ts < cutoff or (mode ~= 'since' and fields['e'] ~= epoch) then
    break
  end
  local offset = tonumber(string.match(entry[1], '^%d+'))
  if latest == 0 then
    latest = offset
  end
  low = offset
  if (mode == 'from' and wanted and offset > after) or (mode == 'since' and ts >= since) then
    out[#out + 1] = {offset, ts, fields['d'], fields['e']}
  end
end
return {1, epoch, low, latest, out, 0}
"#
);

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

/// What the read script returns (see [`READ_LUA`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadMode {
    From,
    Since,
    Peak,
}

impl ReadMode {
    fn arg(self) -> &'static str {
        match self {
            ReadMode::From => "from",
            ReadMode::Since => "since",
            ReadMode::Peak => "peak",
        }
    }
}

/// One command of an operation. A script call gets the instance appended
/// as its last argument when it is sent.
#[derive(Clone)]
enum Call {
    Eval { lua: Lua, keys: Vec<String>, args: Vec<Vec<u8>> },
    Cmd(redis::Cmd),
}

/// Why an operation failed, before it is described.
enum Failure {
    Closed,
    Timeout(Duration),
    /// A recent attempt failed; the broker waits a moment before the next.
    Unavailable,
    Connect(String),
    Redis(redis::RedisError),
    Reply(BrokerError),
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::Closed => f.write_str("Redis broker is shut down"),
            Failure::Timeout(d) => write!(f, "operation timed out after {}s", d.as_secs()),
            Failure::Unavailable => {
                f.write_str("Redis is unavailable after a failure; retrying after a short back-off")
            }
            Failure::Connect(e) => write!(f, "{e}"),
            Failure::Redis(e) => write!(f, "{e}"),
            Failure::Reply(e) => write!(f, "{e}"),
        }
    }
}

/// How long operations fail at once after a timeout or a failed connection
/// attempt, instead of each waiting the full timeout again.
const BACKOFF: Duration = Duration::from_secs(1);

/// Errors of a node that can't take writes now (a demoted master, a replica
/// loading its data or cut off from its master): Redis ran nothing. Redis
/// 7 returns them from scripts with their own code; Redis 6.2 wraps them
/// (`ERR Error running script ... -READONLY ...`).
fn ran_nothing(e: &redis::RedisError) -> bool {
    const CODES: [&str; 3] = ["READONLY", "LOADING", "MASTERDOWN"];
    match e.code() {
        Some(code) if CODES.contains(&code) => true,
        Some(_) => {
            let text = e.to_string();
            CODES.iter().any(|c| text.contains(&format!("-{c} ")))
        }
        None => false,
    }
}

/// Whether the connection should be opened again after `e`.
fn is_connection_error(e: &redis::RedisError) -> bool {
    e.is_io_error() || e.is_connection_dropped() || e.is_unrecoverable_error() || ran_nothing(e)
}

/// Reads the instance of the node `conn` is connected to (one section per
/// `INFO`, as every Redis version takes it).
async fn read_instance(conn: &mut Commands) -> redis::RedisResult<(String, bool)> {
    let (server, replication): (String, String) =
        redis::pipe().cmd("INFO").arg("server").cmd("INFO").arg("replication").query_async(conn).await?;
    Ok(parse_instance(&(server + &replication)))
}

type Connecting =
    futures_util::future::Shared<futures_util::future::BoxFuture<'static, Result<(Commands, String), String>>>;

/// The instance of a connection: read when it opened and again at most once
/// a [`INSTANCE_REFRESH`] while it is used; `None` for a cluster connection
/// (the scripts read it on their node).
#[derive(Clone)]
struct Instance(Option<(String, Instant)>);

/// The broker's connection. One connection attempt at a time, shared by the
/// operations waiting for it; every connection has a generation, so a
/// failure only drops the connection it happened on.
enum Slot {
    Idle,
    Connecting(u64, Connecting),
    Ready(u64, Commands, Instance),
    /// Operations fail at once until then (see [`BACKOFF`]).
    BackingOff(Instant),
}

/// A connection as an operation got it: its generation, the connection, the
/// instance to pass to the scripts (`None`: read it first), and whether this
/// operation waited for it to open.
struct Conn {
    generation: u64,
    commands: Commands,
    instance: Option<String>,
    fresh: bool,
}

/// State shared with the operations running on the Redis runtime.
struct Inner {
    spec: ConnectSpec,
    keys: Keys,
    timeout: Duration,
    slot: Mutex<Slot>,
    generation: AtomicU64,
    /// A connection was lost or failed since the last one opened.
    lost: AtomicBool,
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

    fn command(&self, call: &Call, cached: bool, instance: &str) -> redis::Cmd {
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
                c.arg(instance);
                c
            }
        }
    }

    /// The current connection, or the one being opened (opening one when
    /// there is none); `used` records its generation for [`Inner::back_off`].
    async fn connection(&self, used: &AtomicU64) -> Result<Conn, Failure> {
        let (generation, attempt) = {
            let mut slot = lock(&self.slot);
            match &*slot {
                Slot::Ready(g, c, Instance(instance)) => {
                    used.store(*g, Ordering::SeqCst);
                    let instance = match instance {
                        None => Some(String::new()),
                        Some((i, read_at)) if read_at.elapsed() < INSTANCE_REFRESH => Some(i.clone()),
                        Some(_) => None,
                    };
                    return Ok(Conn { generation: *g, commands: c.clone(), instance, fresh: false });
                }
                Slot::BackingOff(until) if Instant::now() < *until => return Err(Failure::Unavailable),
                Slot::Connecting(g, attempt) => (*g, attempt.clone()),
                Slot::Idle | Slot::BackingOff(_) => {
                    let g = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
                    let spec = self.spec.clone();
                    // No response timeout on the connection: the operation
                    // timeout bounds every wait.
                    let attempt: Connecting =
                        futures_util::FutureExt::shared(futures_util::FutureExt::boxed(async move {
                            let mut c = spec.commands(None).await.map_err(|e| e.to_string())?;
                            let instance = match c {
                                Commands::Cluster(_) => String::new(),
                                Commands::Single(_) => read_instance(&mut c).await.map_err(|e| e.to_string())?.0,
                            };
                            Ok((c, instance))
                        }));
                    *slot = Slot::Connecting(g, attempt.clone());
                    (g, attempt)
                }
            }
        };
        used.store(generation, Ordering::SeqCst);
        let opened = attempt.await;
        let mut slot = lock(&self.slot);
        let current = matches!(&*slot, Slot::Connecting(g, _) if *g == generation);
        match opened {
            Ok((c, instance)) => {
                if current {
                    let cached = match c {
                        Commands::Cluster(_) => None,
                        Commands::Single(_) => Some((instance.clone(), Instant::now())),
                    };
                    *slot = Slot::Ready(generation, c.clone(), Instance(cached));
                    if self.lost.swap(false, Ordering::SeqCst) {
                        self.count(METRIC_RECONNECTS);
                    }
                }
                Ok(Conn { generation, commands: c, instance: Some(instance), fresh: true })
            }
            Err(e) => {
                if current {
                    *slot = Slot::BackingOff(Instant::now() + BACKOFF);
                    self.lost.store(true, Ordering::SeqCst);
                }
                Err(Failure::Connect(e))
            }
        }
    }

    /// Records a fresh read of connection `generation`'s instance.
    fn refreshed(&self, generation: u64, instance: &str) {
        let mut slot = lock(&self.slot);
        if let Slot::Ready(g, _, Instance(cached)) = &mut *slot
            && *g == generation
        {
            *cached = Some((instance.to_owned(), Instant::now()));
        }
    }

    /// Drops connection `generation` after a connection error; the next
    /// operation opens a new one.
    fn forget(&self, generation: u64) {
        let mut slot = lock(&self.slot);
        if matches!(&*slot, Slot::Ready(g, ..) if *g == generation) {
            *slot = Slot::Idle;
            self.lost.store(true, Ordering::SeqCst);
        }
    }

    /// After a timeout on connection `generation` (or its attempt), or a node
    /// that still can't take writes on a new connection: it is not used
    /// again, and operations fail at once for [`BACKOFF`].
    fn back_off(&self, generation: u64) {
        let mut slot = lock(&self.slot);
        let stalled = match &*slot {
            Slot::Ready(g, ..) | Slot::Connecting(g, _) => *g == generation,
            Slot::Idle | Slot::BackingOff(_) => false,
        };
        if stalled {
            *slot = Slot::BackingOff(Instant::now() + BACKOFF);
            self.lost.store(true, Ordering::SeqCst);
        }
    }

    fn close(&self) {
        *lock(&self.slot) = Slot::Idle;
    }

    /// Runs one call, falling back to `EVAL` when Redis doesn't know the script.
    async fn exec_one(&self, conn: &mut Commands, call: &Call, instance: &str) -> redis::RedisResult<Value> {
        match self.command(call, true, instance).query_async::<Value>(conn).await {
            Err(e) if e.code() == Some("NOSCRIPT") => self.command(call, false, instance).query_async(conn).await,
            other => other,
        }
    }

    /// Runs the calls in order: pipelined on one node, one after the other
    /// in a cluster (their keys may be in different slots).
    async fn exec_on(&self, conn: &mut Commands, calls: &[Call], instance: &str) -> redis::RedisResult<Vec<Value>> {
        let mut out = Vec::with_capacity(calls.len());
        if calls.len() == 1 || matches!(conn, Commands::Cluster(_)) {
            for call in calls {
                out.push(self.exec_one(conn, call, instance).await?);
            }
            return Ok(out);
        }
        let mut pipe = redis::pipe();
        pipe.ignore_errors();
        for call in calls {
            pipe.add_command(self.command(call, true, instance));
        }
        let replies: Vec<redis::RedisResult<Value>> = pipe.query_async(conn).await?;
        for (call, reply) in calls.iter().zip(replies) {
            match reply {
                // Not run: safe to send again with the script's body.
                Err(e) if e.code() == Some("NOSCRIPT") => {
                    out.push(self.command(call, false, instance).query_async(conn).await?)
                }
                other => out.push(other?),
            }
        }
        Ok(out)
    }

    /// Runs the calls on `conn`, reading its instance first when it is due.
    async fn exec_with(&self, conn: &mut Conn, calls: &[Call]) -> redis::RedisResult<Vec<Value>> {
        let instance = match conn.instance.take() {
            Some(i) => i,
            None => {
                let (i, _) = read_instance(&mut conn.commands).await?;
                self.refreshed(conn.generation, &i);
                i
            }
        };
        self.exec_on(&mut conn.commands, calls, &instance).await
    }

    /// Runs the calls. After a connection error the connection is dropped,
    /// and the calls run once more on a new one when that is safe: they are
    /// idempotent, or Redis ran nothing (see [`ran_nothing`]). A node that
    /// still can't take writes on a new connection starts the back-off.
    async fn exec(
        self: Arc<Self>,
        calls: Vec<Call>,
        idempotent: bool,
        used: Arc<AtomicU64>,
    ) -> Result<Vec<Value>, Failure> {
        let mut retried = false;
        loop {
            let mut conn = self.connection(&used).await?;
            let generation = conn.generation;
            match self.exec_with(&mut conn, &calls).await {
                Err(e) if e.is_timeout() => return Err(Failure::Timeout(self.timeout)),
                Err(e) if ran_nothing(&e) && (retried || conn.fresh) => {
                    self.back_off(generation);
                    return Err(Failure::Redis(e));
                }
                Err(e) if is_connection_error(&e) => {
                    self.forget(generation);
                    if !retried && (idempotent || ran_nothing(&e)) {
                        retried = true;
                        continue;
                    }
                    return Err(Failure::Redis(e));
                }
                other => return other.map_err(Failure::Redis),
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
    /// When the last back-off failure was logged, and how many were not.
    unavailable_logged: Mutex<(Option<Instant>, u64)>,
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
                slot: Mutex::new(Slot::Idle),
                generation: AtomicU64::new(0),
                lost: AtomicBool::new(false),
                digests: Default::default(),
                metrics,
                closed: AtomicBool::new(false),
            }),
            broadcaster,
            conf: config.clone(),
            tracker: StreamsTracker::new(),
            rotation_warned: AtomicU64::new(0),
            unavailable_logged: Mutex::new((None, 0)),
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
        let used = Arc::new(AtomicU64::new(0));
        let task = inner.clone().exec(calls, idempotent, used.clone());
        let result = connect::wait_on(timeout + REPORT_GRACE, async move { tokio::time::timeout(timeout, task).await });
        let result = match result {
            Some(Ok(result)) => result,
            Some(Err(_)) | None => Err(Failure::Timeout(timeout)),
        };
        match result {
            Ok(values) if values.len() == expected => Ok(values),
            Ok(values) => {
                Err(Failure::Reply(err(format!("expected {expected} replies from Redis, got {}", values.len()))))
            }
            Err(Failure::Timeout(d)) => {
                inner.back_off(used.load(Ordering::SeqCst));
                inner.count(METRIC_TIMEOUTS);
                Err(Failure::Timeout(d))
            }
            Err(f) => Err(f),
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
            if matches!(failure, Failure::Unavailable) {
                self.log_unavailable(&message);
            } else {
                log_error!(log(), &message);
            }
            err(message)
        })
    }

    /// Logs a failure during the back-off at most once a second, with the
    /// number of those not logged since.
    fn log_unavailable(&self, message: &str) {
        let suppressed = {
            let mut logged = lock(&self.unavailable_logged);
            if logged.0.is_some_and(|at| at.elapsed() < UNAVAILABLE_LOG_INTERVAL) {
                logged.1 += 1;
                return;
            }
            let suppressed = logged.1;
            *logged = (Some(Instant::now()), 0);
            suppressed
        };
        if suppressed == 0 {
            log_error!(log(), message);
        } else {
            log_error!(log(), &format!("{message} ({suppressed} more since the last record)"));
        }
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

    /// A read of the stream (see [`READ_LUA`]).
    fn snapshot(
        &self,
        stream: &str,
        mode: ReadMode,
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
                mode.arg().as_bytes().to_vec(),
                after.to_string().into_bytes(),
                since.to_string().into_bytes(),
                epoch.as_bytes().to_vec(),
            ],
        };
        let what = || format!("failed to read history from Redis: stream={stream}");
        self.op(what, vec![call], true, |v| parse_snapshot(&v[0]))
    }

    fn message(stream: &str, item: Item) -> StreamMessage {
        StreamMessage { stream: stream.to_owned(), data: item.data, offset: item.offset, epoch: item.epoch, meta: None }
    }

    fn session_call(&self, cmd: &str, sid: &str) -> redis::Cmd {
        let mut c = redis::cmd(cmd);
        c.arg(self.inner.keys.session(sid));
        c
    }
}

impl Broker for RedisBroker {
    /// Loads the scripts and reads the server's version and instance; fails
    /// when Redis can't be reached within the timeout.
    fn start(&self) -> Result<(), BrokerError> {
        let mut calls: Vec<Call> = Lua::ALL
            .iter()
            .map(|lua| {
                let mut c = redis::cmd("SCRIPT");
                c.arg("LOAD").arg(lua.body());
                Call::Cmd(c)
            })
            .collect();
        for section in ["server", "replication"] {
            let mut info = redis::cmd("INFO");
            info.arg(section);
            calls.push(Call::Cmd(info));
        }
        let (digests, info) = self.op(
            || "failed to start the Redis broker".to_owned(),
            calls,
            true,
            |v| {
                let digests = v[..Lua::ALL.len()]
                    .iter()
                    .map(|d| text(d).ok_or_else(|| unexpected("SCRIPT LOAD", d)))
                    .collect::<Result<Vec<String>, _>>()?;
                let info: String = v[Lua::ALL.len()..].iter().filter_map(text).collect();
                Ok((digests, info))
            },
        )?;
        for (slot, digest) in self.inner.digests.iter().zip(digests) {
            let _ = slot.set(digest);
        }
        let version = info.lines().find_map(|l| l.strip_prefix("redis_version:")).unwrap_or("unknown").trim();
        if !parse_instance(&info).1 {
            log_warn!(
                log(),
                "Redis reports no run_id or master_replid: restart and failover detection is off, so offsets may repeat in an epoch after a restart from a snapshot or a failover"
            );
        }
        log_info!(log(), &format!("Redis broker is ready (Redis {version}, epochs per stream)"));
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.close();
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
                token().into_bytes(),
            ],
        };
        let what = || format!("failed to add message to Redis stream: stream={}", msg.stream);
        // Not run again by the broker after a connection error (unless Redis
        // ran nothing): the broadcast may have been stored. A resend by the
        // Redis Cluster client carries the same token and is stored once.
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
        let snapshot = self.snapshot(stream, ReadMode::From, self.conf.history_limit, offset, i64::MIN, epoch)?;
        Ok(snapshot.after(epoch, offset)?.into_iter().map(|i| Self::message(stream, i)).collect())
    }

    /// Every kept message at or after `ts`, each with the epoch it was
    /// written in.
    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        let snapshot = self.snapshot(stream, ReadMode::Since, self.conf.history_limit, 0, ts, "")?;
        if !snapshot.exists {
            return Ok(None);
        }
        Ok(Some(snapshot.entries.into_iter().map(|i| Self::message(stream, i)).collect()))
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
        let snapshot = self.snapshot(stream, ReadMode::Peak, self.conf.history_limit.min(1), 0, i64::MIN, "")?;
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

    fn entry(offset: u64, timestamp: i64, data: &str) -> Item {
        Item { offset, timestamp, data: data.to_owned(), epoch: "e1".to_owned() }
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

    fn server_error(resp: &str) -> redis::RedisError {
        match redis::parse_redis_value(resp.as_bytes()).unwrap() {
            Value::ServerError(e) => e.into(),
            other => panic!("not an error: {other:?}"),
        }
    }

    /// A node that can't take writes now (a demoted master, a replica still
    /// loading or cut off from its master) ran nothing: the broker connects
    /// again and retries, even a broadcast.
    #[test]
    fn unavailable_nodes_are_connection_errors_that_ran_nothing() {
        for resp in [
            "-READONLY You can't write against a read only replica. script: b8a6, on @user_script:27.\r\n",
            "-LOADING Redis is loading the dataset in memory\r\n",
            "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n",
        ] {
            let e = server_error(resp);
            assert!(is_connection_error(&e), "{e}");
            assert!(ran_nothing(&e), "{e}");
        }
        let wrapped = "-ERR Error running script (call to f_73f5): @user_script:1: @user_script: 1: -READONLY You can't write against a read only replica.\r\n";
        let e = server_error(wrapped);
        assert!(is_connection_error(&e) && ran_nothing(&e), "Redis 6.2 wraps the error: {e}");
        let e = server_error("-ERR Error running script\r\n");
        assert!(!is_connection_error(&e) && !ran_nothing(&e), "{e}");
        let io = redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert!(is_connection_error(&io) && !ran_nothing(&io));
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
    fn read_reply(epoch: &str, visible: &[Item], after: u64) -> Snapshot {
        Snapshot {
            exists: true,
            epoch: epoch.to_owned(),
            low: visible.first().map_or(0, |e| e.offset),
            latest: visible.last().map_or(0, |e| e.offset),
            entries: visible.iter().filter(|e| e.offset > after).cloned().collect(),
            stale: false,
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
        let stale = Snapshot { stale: true, ..read_reply("e1", &[entry(1, 0, "a")], 0) };
        assert_eq!(
            "unknown epoch: e1, current: e1 (the Redis instance changed)",
            stale.after("e1", 1).unwrap_err().message,
            "no position is known on a stale stream"
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
                Value::Array(vec![Value::Int(7), Value::Int(1699999999), bulk("{\"x\":1}"), bulk("e1")]),
                Value::Array(vec![Value::Int(6), Value::Int(1699999998), bulk("b"), bulk("e1")]),
            ]),
            Value::Int(0),
        ]);
        assert_eq!(
            Snapshot {
                exists: true,
                epoch: "e1".into(),
                low: 5,
                latest: 7,
                entries: vec![entry(6, 1699999998, "b"), entry(7, 1699999999, "{\"x\":1}")],
                stale: false,
            },
            parse_snapshot(&reply).unwrap()
        );
        let empty = || Value::Array(vec![]);
        let missing = Value::Array(vec![Value::Int(0), bulk(""), Value::Int(0), Value::Int(0), empty(), Value::Int(0)]);
        assert_eq!(Snapshot::default(), parse_snapshot(&missing).unwrap());
        let stale = Value::Array(vec![Value::Int(1), bulk("e1"), Value::Int(0), Value::Int(0), empty(), Value::Int(1)]);
        assert!(parse_snapshot(&stale).unwrap().stale);
        let bad =
            Value::Array(vec![Value::Int(1), bulk("e"), Value::Int(0), Value::Int(0), Value::Int(3), Value::Int(0)]);
        assert!(parse_snapshot(&bad).is_err());
    }

    #[test]
    fn instances_come_from_info() {
        let info = "# Server\r\nredis_version:7.4.12\r\nrun_id:6dd4\r\n# Replication\r\nrole:master\r\nmaster_replid:7fd9\r\nmaster_replid2:0000\r\n";
        assert_eq!(("6dd4:7fd9".to_owned(), true), parse_instance(info));
        assert_eq!((":7fd9".to_owned(), false), parse_instance("master_replid:7fd9\r\n"), "no run_id");
        assert_eq!((":".to_owned(), false), parse_instance(""));
    }

    #[test]
    fn broadcast_tokens_are_random_hex() {
        let (a, b) = (token(), token());
        assert_eq!(32, a.len());
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()), "{a}");
        assert_ne!(a, b);
    }
}
