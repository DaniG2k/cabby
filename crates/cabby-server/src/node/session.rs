//! A connected client (upstream `node/session.go`).
//!
//! # Design
//!
//! A session is shared (`Arc<Session>`) by the transport's reader, its writer,
//! the node, the hub's gate threads and the disconnect queue. Each piece of
//! mutable state has exactly one owner and lock, and no lock is held while
//! another component is called:
//!
//! * **Send path.** Producers (commands, the hub, timers) encode a frame into an
//!   `Arc<SentFrame>`. Broadcast frames are encoded once per encoder and shared
//!   between sessions (the hub's [`CachedEncodedMessage`]). On a non-blocking
//!   transport, when the writer is idle and nothing is queued, the frame goes
//!   straight to the transport without passing through the write queue; otherwise
//!   it is pushed into the session's mutex-guarded ring buffer so `send` never
//!   blocks on the connection. The queue accounts payload bytes; when the bytes
//!   still waiting reach `max_pending_size` the client is too slow: the queue is
//!   cleared and the session is disconnected with `too_slow` (upstream
//!   semantics).
//! * **Writer.** Upstream runs a writer goroutine per session. Here the writer
//!   is driven by an atomic state machine (`DETACHED`, `IDLE`, `BUSY`,
//!   `WAIT_WRITABLE`, `DONE`), so at most one write/drain runs per session,
//!   frames keep their FIFO order, and idle sessions cost no thread. Over a
//!   non-blocking transport (the WebSocket connection, which queues frames for
//!   its I/O task) writes run on the producer's thread without a thread switch;
//!   when a connection's queue is full the refused frame is parked and the writer
//!   waits in `WAIT_WRITABLE` until the connection signals space. Close frames,
//!   failed writes, and blocking transports use a drain on the node's task pool.
//!   Pings over a non-blocking transport are queued like other frames; over a
//!   blocking one they share a write lock with the drain.
//! * **Lifecycle.** `closed` (with the close token) and the Disconnect interest
//!   live under one small mutex; `connected` is an atomic that only changes
//!   under the registration lock, which serializes hub registration against
//!   disconnects (a session closing while it authenticates never stays in the hub).
//! * **Timers.** Upstream's single-timer scheme: the ping, pong, handshake, cache
//!   and presence deadlines share one cancellable timer from the injected
//!   [`Clock`], armed for the earliest deadline. A fired timer only acts if it is
//!   still the current one (a generation check), and a closed session never
//!   re-arms. Callbacks hold a weak reference, so timers never keep a session alive.
//! * **Env and subscriptions** have their own locks; controller calls get a
//!   snapshot of the env, never a guard.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use bytes::Bytes;
use cabby_core::broker::{Broker, BrokerError, Cacheable};
use cabby_core::common::{
    DisconnectMessage, Message, NO_PONG_REASON, PONG_TYPE, PingMessage, REMOTE_DISCONNECT_REASON, Reply,
    SERVER_RESTART_REASON, SLOW_CLIENT_REASON, SessionEnv, StringMap,
};
use cabby_core::encoders::{EncodedMessageJson, Encoder, FrameType, Json, SentFrame};
use cabby_core::gojson::deep::DeepNestingError;
use cabby_core::logger::{LogValue, LogValuer, Logger, compact_value, compact_value_bytes};
use cabby_core::utils::{Item, PushResult, Queue};
use cabby_core::{log_debug, log_error, log_warn};
use serde::Serialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::{
    CLOSE_ABNORMAL_CLOSURE, CLOSE_GOING_AWAY, CLOSE_NORMAL_CLOSURE, Clock, METRICS_DATA_RECEIVED, METRICS_DATA_SENT,
    METRICS_DISCONNECTED_SLOW_CLIENTS, METRICS_FAILED_COMMAND_RECEIVED, METRICS_FAILED_SENT, METRICS_RECEIVED_MSG,
    METRICS_SENT_MSG, METRICS_WRITE_TIMEOUT, Node, NodeError, SubscriptionState, SystemClock, Timer,
};
use crate::metrics::{Counter, Instrumenter, NoopMetrics};

/// A connection write or read failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ConnectionError(pub String);

impl ConnectionError {
    /// Whether the peer closed the connection normally (upstream `ws.IsCloseError`:
    /// a WebSocket close with status 1000, 1001 or 1005). Transports report such
    /// closes with gorilla/websocket's texts, `websocket: close <code> ...`.
    pub fn is_close(&self) -> bool {
        ["websocket: close 1000", "websocket: close 1001", "websocket: close 1005"]
            .iter()
            .any(|p| self.0.starts_with(p))
    }

    /// Whether the failure is a write deadline (Go `net.Error.Timeout()`; Go
    /// network errors read `i/o timeout`).
    pub fn is_timeout(&self) -> bool {
        self.0.contains("timeout")
    }
}

/// The underlying transport (upstream `node.Connection`). `deadline` bounds a write.
pub trait Connection: Send + Sync {
    fn write(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError>;
    fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError>;
    fn write_ping(&self, deadline: Instant) -> Result<(), ConnectionError>;
    fn read(&self) -> Result<Vec<u8>, ConnectionError>;
    fn close(&self, code: i32, reason: &str);

    /// Writes a shared frame: text, binary, or close (with its code and reason).
    /// Transports that can reference the payload instead of copying it (the hub
    /// encodes a broadcast once for every session) override this.
    fn write_frame(&self, frame: &Arc<SentFrame>, deadline: Instant) -> Result<(), ConnectionError> {
        match frame.frame_type {
            FrameType::Text => self.write(&frame.payload, deadline),
            FrameType::Binary => self.write_binary(&frame.payload, deadline),
            FrameType::Close => {
                self.close(frame.close_code, &frame.close_reason);
                Ok(())
            }
        }
    }

    /// Whether the transport queues writes without blocking: it implements
    /// [`Connection::try_write_frame`] and calls the writable handler. Sessions
    /// over such transports write from the producer's thread instead of a pool
    /// thread (see the module docs).
    fn nonblocking(&self) -> bool {
        false
    }

    /// Queues a text or binary frame without blocking: `Ok(false)` when the
    /// transport cannot take it now, in which case it calls the writable handler
    /// once it can (or once it has failed). `timeout` bounds the write, from when the
    /// transport starts it (producers do not read the clock).
    fn try_write_frame(&self, frame: &Arc<SentFrame>, timeout: Duration) -> Result<bool, ConnectionError> {
        self.write_frame(frame, deadline_after(timeout)).map(|()| true)
    }

    /// Sets the handler [`Connection::try_write_frame`] promises to call after
    /// refusing a frame.
    fn set_writable_handler(&self, _handler: Box<dyn Fn() + Send + Sync>) {}
}

/// Handles decoded client commands for sessions (upstream `node.Executor`).
pub trait Executor: Send + Sync {
    fn handle_command(&self, session: &Arc<Session>, msg: &Message) -> Result<(), NodeError>;
    fn disconnect(&self, session: &Arc<Session>) -> Result<(), NodeError>;

    /// Whether `msg` only touches the session itself (never the controller, the
    /// broker or anything else that may block), so it can run on the transport's
    /// thread when the session has nothing else queued (client pings).
    fn is_inline(&self, _msg: &Message) -> bool {
        false
    }

    /// The time source sessions of this executor schedule their timers on (the
    /// node's clock). `None` keeps the session's own clock.
    fn clock(&self) -> Option<Arc<dyn Clock>> {
        None
    }
}

/// Session construction options (upstream `SessionOption` functions `With*`).
pub enum SessionOption {
    /// `WithPingInterval` (zero disables pings).
    PingInterval(Duration),
    /// `WithPingPrecision`.
    PingPrecision(String),
    /// `WithEncoder`.
    Encoder(Arc<dyn Encoder + Send + Sync>),
    /// `WithExecutor`.
    Executor(Arc<dyn Executor>),
    /// `WithHandshakeMessageDeadline`: also means "do not authenticate on connect".
    HandshakeMessageDeadline(Instant),
    /// `WithMetrics`.
    Metrics(Arc<dyn Instrumenter>),
    /// `WithKeepaliveIntervals(sessionsTTL, presenceTTL)`: how often to touch the
    /// session cache and the presence records while connected.
    KeepaliveIntervals { sessions_ttl: Duration, presence_ttl: Duration },
    /// `WithPrevSID`: the session to restore from.
    PrevSid(String),
    /// `WithPongTimeout`.
    PongTimeout(Duration),
    /// `WithWriteTimeout`.
    WriteTimeout(Duration),
    /// `WithMaxPendingSize` (bytes of queued frames; 0 is unlimited).
    MaxPendingSize(u64),
    /// `InactiveSession`: no timers, no writer, not registered in the hub.
    Inactive,
}

// Writer states.
/// No writer attached yet: frames wait in the queue.
const WRITER_DETACHED: u8 = 0;
/// Attached, `refused` is empty and no drain is running.
const WRITER_IDLE: u8 = 1;
/// A drain (or fast-path direct write) is running.
const WRITER_BUSY: u8 = 2;
/// Stopped after a close frame or a failed write (upstream's writer returned).
const WRITER_DONE: u8 = 3;
/// A non-blocking write was refused (`refused` holds the frame); waiting for
/// the transport's writable callback to resume draining.
const WRITER_WAIT_WRITABLE: u8 = 4;

/// Outcome of one non-blocking write attempt while the caller holds `WRITER_BUSY`.
enum NonblockingSend {
    /// Accepted by the transport; caller still owns `WRITER_BUSY`.
    Sent,
    /// Refused, then the transport immediately became writable again and this
    /// caller re-acquired `WRITER_BUSY` with the frame parked in `refused`.
    RetryNow,
    /// Ownership handed off (`WRITER_WAIT_WRITABLE` or pool drain spawned).
    HandedOff,
}

/// Initial capacity of the write queue (upstream `NewQueue(16)`).
const WRITE_QUEUE_CAPACITY: usize = 16;

/// Default write deadline (upstream `BuildSession`).
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest timer interval a session keeps (about ten years): longer ping,
/// pong, keepalive and write intervals are clamped (D-054). Clients are
/// limited much lower (`cli::MAX_CLIENT_PING_INTERVAL_SECS`); this bounds
/// operator settings and library callers.
pub const MAX_TIMER_INTERVAL: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

/// The instant `timeout` from now. A timeout beyond what the clock can represent is
/// none in practice (about 136 years).
pub(crate) fn deadline_after(timeout: Duration) -> Instant {
    let now = Instant::now();
    now.checked_add(timeout).unwrap_or_else(|| now + Duration::from_secs(u64::from(u32::MAX)))
}

fn bounded(d: Duration) -> Duration {
    d.min(MAX_TIMER_INTERVAL)
}

fn lock<T: ?Sized>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T: ?Sized>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T: ?Sized>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(PoisonError::into_inner)
}

/// Collaborators and settings: written while the session is set up, read on
/// every operation (an uncontended read lock).
struct Settings {
    uid: String,
    encoder: Arc<dyn Encoder + Send + Sync>,
    executor: Option<Arc<dyn Executor>>,
    broker: Option<Arc<dyn Broker>>,
    metrics: Arc<dyn Instrumenter>,
    clock: Arc<dyn Clock>,
    write_timeout: Duration,
    ping_interval: Duration,
    ping_precision: String,
    native_ping: bool,
    pong_timeout: Duration,
    presence_interval: Duration,
    resume_interval: Duration,
    prev_sid: String,
    inactive: bool,
    /// The session's logger (upstream `Session.Log`: the node's with `sid`).
    log: Logger,
}

/// Lifecycle state (upstream `mu`).
#[derive(Default)]
struct Lifecycle {
    closed: bool,
    disconnect_interest: bool,
    /// An undecodable message ended the reading of commands (D-074).
    discarding: bool,
    internal_state: Option<HashMap<String, Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerEvent {
    Handshake,
    Presence,
    Expire,
    Ping,
    Pong,
}

/// Upstream `SessionTimers`: deadlines sharing one timer.
#[derive(Default)]
struct Timers {
    ping: Option<Instant>,
    handshake: Option<Instant>,
    pong: Option<Instant>,
    resume: Option<Instant>,
    presence: Option<Instant>,
    /// `Start` was called: scheduling arms the timer from now on.
    started: bool,
    /// The session closed: nothing is armed again.
    stopped: bool,
    next_event: Option<TimerEvent>,
    timer: Option<Box<dyn Timer>>,
    generation: u64,
}

impl Timers {
    /// The earliest deadline, ties resolved in upstream's order (ping, handshake,
    /// pong, cache, presence; a later one wins only when strictly earlier).
    fn earliest(&self) -> Option<(TimerEvent, Instant)> {
        let mut best: Option<(TimerEvent, Instant)> = None;
        for (ev, at) in [
            (TimerEvent::Ping, self.ping),
            (TimerEvent::Handshake, self.handshake),
            (TimerEvent::Pong, self.pong),
            (TimerEvent::Expire, self.resume),
            (TimerEvent::Presence, self.presence),
        ] {
            if let Some(at) = at
                && best.is_none_or(|(_, b)| at < b)
            {
                best = Some((ev, at));
            }
        }
        best
    }
}

/// Hot-path collaborators and settings read on every delivered broadcast frame
/// through a lock-free [`ArcSwap`]: avoids taking the `settings` RwLock, cloning
/// `Arc`s, or looking up hot metric names in the registry map.
struct HotConfig {
    encoder: Arc<dyn Encoder + Send + Sync>,
    metrics: Arc<dyn Instrumenter>,
    sent_msg: Option<Arc<Counter>>,
    data_sent: Option<Arc<Counter>>,
    write_timeout: Duration,
}

impl HotConfig {
    fn from_settings(s: &Settings) -> Self {
        Self {
            encoder: Arc::clone(&s.encoder),
            metrics: Arc::clone(&s.metrics),
            sent_msg: s.metrics.counter_handle(METRICS_SENT_MSG),
            data_sent: s.metrics.counter_handle(METRICS_DATA_SENT),
            write_timeout: s.write_timeout,
        }
    }

    #[inline]
    fn record_sent(&self, bytes: u64) {
        if let Some(c) = &self.data_sent {
            c.add(bytes);
        } else {
            self.metrics.counter_add(METRICS_DATA_SENT, bytes);
        }
        if let Some(c) = &self.sent_msg {
            c.inc();
        } else {
            self.metrics.counter_increment(METRICS_SENT_MSG);
        }
    }
}

/// A client session (upstream `node.Session`). Shared as `Arc<Session>` between
/// the transport, the node, the hub and the disconnect queue.
pub struct Session {
    me: Weak<Session>,
    conn: Arc<dyn Connection>,
    settings: RwLock<Settings>,
    hot: ArcSwap<HotConfig>,
    env: Mutex<SessionEnv>,
    subscriptions: SubscriptionState,
    lifecycle: Mutex<Lifecycle>,
    close_token: CancellationToken,
    connected: AtomicBool,
    /// Serializes hub registration with disconnects (upstream `rmu`).
    registration: Mutex<()>,
    /// Serializes connection writes (upstream `wmu`).
    write_lock: Mutex<()>,
    write_queue: Queue<Arc<SentFrame>>,
    writer: AtomicU8,
    max_pending_size: AtomicU64,
    timers: Mutex<Timers>,
    /// Commands go through the Pusher executor: only such sessions may
    /// subscribe to `$pusher`.
    pusher: AtomicBool,
    /// The transport queues writes without blocking ([`Connection::nonblocking`]).
    nonblocking: bool,
    /// A frame the non-blocking transport refused: written first once it is writable.
    refused: Mutex<Option<Arc<SentFrame>>>,
    /// The transport signalled that it is writable again (see `drain_nonblocking`).
    writable: AtomicBool,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session").field("uid", &read(&self.settings).uid).finish_non_exhaustive()
    }
}

/// The cache entry (upstream unexported `cacheEntry`), in Go's field order.
#[derive(Serialize)]
struct CacheEntryOut<'a> {
    ids: &'a str,
    subs: BTreeMap<String, Vec<String>>,
    cstate: Option<BTreeMap<&'a str, &'a str>>,
    istate: Option<BTreeMap<&'a str, BTreeMap<&'a str, &'a str>>>,
    #[serde(rename = "Disconnectable")]
    disconnectable: bool,
}

#[derive(Default)]
struct CacheEntryIn {
    ids: String,
    subs: HashMap<String, Vec<String>>,
    cstate: Option<StringMap>,
    istate: HashMap<String, StringMap>,
    disconnectable: bool,
}

fn json_error(e: impl std::fmt::Display) -> NodeError {
    NodeError(e.to_string())
}

/// Decodes a cache entry like Go's `json.Unmarshal` into `cacheEntry`: field
/// names match exactly or case-insensitively, `null` leaves a field unset,
/// unknown fields are ignored.
fn decode_cache_entry(cached: &[u8]) -> Result<CacheEntryIn, NodeError> {
    const FIELDS: [&str; 5] = ["ids", "subs", "cstate", "istate", "disconnectable"];
    let value: Value = serde_json::from_slice(cached).map_err(json_error)?;
    let mut entry = CacheEntryIn::default();
    let Value::Object(map) = value else {
        if value.is_null() {
            return Ok(entry);
        }
        return Err(NodeError("json: cannot unmarshal into Go value of type node.cacheEntry".into()));
    };
    for (key, v) in map {
        if v.is_null() {
            continue;
        }
        match cabby_core::gojson::field_index(&key, &FIELDS) {
            Some(0) => entry.ids = serde_json::from_value(v).map_err(json_error)?,
            Some(1) => {
                let subs: HashMap<String, Option<Vec<String>>> = serde_json::from_value(v).map_err(json_error)?;
                entry.subs = subs.into_iter().map(|(k, v)| (k, v.unwrap_or_default())).collect();
            }
            Some(2) => entry.cstate = Some(serde_json::from_value(v).map_err(json_error)?),
            Some(3) => {
                let istate: HashMap<String, Option<StringMap>> = serde_json::from_value(v).map_err(json_error)?;
                entry.istate = istate.into_iter().map(|(k, v)| (k, v.unwrap_or_default())).collect();
            }
            Some(4) => entry.disconnectable = serde_json::from_value(v).map_err(json_error)?,
            _ => {}
        }
    }
    Ok(entry)
}

/// The value printed by Go's `%v` for decoded JSON (`interface{}`).
pub(super) fn go_value_text(v: &Value) -> String {
    match v {
        Value::Null => "<nil>".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(items) => format!("[{}]", items.iter().map(go_value_text).collect::<Vec<_>>().join(" ")),
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            let parts: Vec<String> = keys.iter().map(|k| format!("{k}:{}", go_value_text(&map[k.as_str()]))).collect();
            format!("map[{}]", parts.join(" "))
        }
    }
}

impl Session {
    /// The bare session with defaults: JSON encoder, no-op metrics, empty
    /// subscriptions, a 16-frame write queue, 2s write timeout (upstream
    /// `BuildSession`). Neither the writer nor the timers are started.
    pub fn build(conn: Arc<dyn Connection>, env: SessionEnv) -> Arc<Session> {
        let nonblocking = conn.nonblocking();
        let initial_settings = Settings {
            uid: String::new(),
            encoder: Arc::new(Json),
            executor: None,
            broker: None,
            metrics: Arc::new(NoopMetrics),
            clock: Arc::new(SystemClock),
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            ping_interval: Duration::ZERO,
            ping_precision: String::new(),
            native_ping: false,
            pong_timeout: Duration::ZERO,
            presence_interval: Duration::ZERO,
            resume_interval: Duration::ZERO,
            prev_sid: String::new(),
            inactive: false,
            log: Logger::default(),
        };
        let hot = ArcSwap::from_pointee(HotConfig::from_settings(&initial_settings));
        let session = Arc::new_cyclic(|me| Session {
            me: me.clone(),
            nonblocking,
            refused: Mutex::new(None),
            writable: AtomicBool::new(false),
            conn,
            settings: RwLock::new(initial_settings),
            hot,
            env: Mutex::new(env),
            subscriptions: SubscriptionState::new(),
            lifecycle: Mutex::new(Lifecycle::default()),
            close_token: CancellationToken::new(),
            connected: AtomicBool::new(false),
            registration: Mutex::new(()),
            write_lock: Mutex::new(()),
            write_queue: Queue::new(WRITE_QUEUE_CAPACITY),
            writer: AtomicU8::new(WRITER_DETACHED),
            max_pending_size: AtomicU64::new(0),
            timers: Mutex::new(Timers::default()),
            pusher: AtomicBool::new(false),
        });
        if nonblocking {
            let me = Arc::downgrade(&session);
            session.conn.set_writable_handler(Box::new(move || {
                if let Some(session) = me.upgrade() {
                    session.writable.store(true, Ordering::SeqCst);
                    if session
                        .writer
                        .compare_exchange(WRITER_WAIT_WRITABLE, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        session.drain_nonblocking();
                    }
                }
            }));
        }
        session
    }

    /// Upstream `NewSession`: a session bound to `node` (its metrics, broker and
    /// ping settings); starts the writer and the timers unless inactive.
    pub fn new(
        node: &Arc<Node>,
        conn: Arc<dyn Connection>,
        url: &str,
        headers: Option<StringMap>,
        uid: &str,
        opts: Vec<SessionOption>,
    ) -> Arc<Session> {
        let session = Session::build(conn, SessionEnv::new(url, headers));
        {
            let config = node.config();
            let mut s = write(&session.settings);
            s.metrics = node.instrumenter();
            s.executor = Some(node.clone() as Arc<dyn Executor>);
            s.broker = Some(node.broker());
            s.clock = node.clock();
            s.ping_interval = bounded(Duration::from_secs(config.ping_interval.max(0) as u64));
            s.ping_precision.clone_from(&config.ping_timestamp_precision);
            s.native_ping = config.enable_native_ping;
            s.uid = uid.to_owned();
            s.log = node.logger().with("sid", uid);
            session.hot.store(Arc::new(HotConfig::from_settings(&s)));
        }
        session.apply(opts);

        if !session.is_inactive() {
            session.send_messages();
            session.start_timers();
        }
        session
    }

    fn me(&self) -> Arc<Session> {
        // Every method runs through an `Arc<Session>` held by the caller.
        self.me.upgrade().expect("a live session")
    }

    fn settings(&self) -> RwLockReadGuard<'_, Settings> {
        read(&self.settings)
    }

    fn metrics(&self) -> Arc<dyn Instrumenter> {
        self.settings().metrics.clone()
    }

    /// The session's logger (upstream `Session.Log`): the node's logger with
    /// `sid` for sessions built by [`Session::new`].
    pub fn logger(&self) -> Logger {
        self.settings().log.clone()
    }

    /// Replaces the session's logger (upstream assigns `Session.Log`, e.g.
    /// `session.Log = session.Log.With("ds", path)`).
    pub fn set_logger(&self, log: Logger) {
        write(&self.settings).log = log;
    }

    fn encoder(&self) -> Arc<dyn Encoder + Send + Sync> {
        self.settings().encoder.clone()
    }

    fn executor(&self) -> Option<Arc<dyn Executor>> {
        self.settings().executor.clone()
    }

    fn adopt_executor(s: &mut Settings, executor: Arc<dyn Executor>) {
        if let Some(clock) = executor.clock() {
            s.clock = clock;
        }
        s.executor = Some(executor);
    }

    /// Applies options after construction (what upstream test helpers do by
    /// calling the option functions on a built session).
    pub fn apply(&self, opts: Vec<SessionOption>) {
        for opt in opts {
            match opt {
                SessionOption::HandshakeMessageDeadline(deadline) => lock(&self.timers).handshake = Some(deadline),
                SessionOption::MaxPendingSize(size) => self.max_pending_size.store(size, Ordering::Release),
                opt => {
                    let mut s = write(&self.settings);
                    let mut hot_changed = false;
                    match opt {
                        SessionOption::PingInterval(d) => s.ping_interval = bounded(d),
                        SessionOption::PingPrecision(p) => s.ping_precision = p,
                        SessionOption::Encoder(e) => {
                            s.encoder = e;
                            hot_changed = true;
                        }
                        SessionOption::Executor(e) => Self::adopt_executor(&mut s, e),
                        SessionOption::Metrics(m) => {
                            s.metrics = m;
                            hot_changed = true;
                        }
                        SessionOption::KeepaliveIntervals { sessions_ttl, presence_ttl } => {
                            s.resume_interval = bounded(sessions_ttl);
                            s.presence_interval = bounded(presence_ttl);
                        }
                        SessionOption::PrevSid(sid) => s.prev_sid = sid,
                        SessionOption::PongTimeout(d) => s.pong_timeout = bounded(d),
                        SessionOption::WriteTimeout(d) => {
                            s.write_timeout = bounded(d);
                            hot_changed = true;
                        }
                        SessionOption::Inactive => s.inactive = true,
                        SessionOption::HandshakeMessageDeadline(_) | SessionOption::MaxPendingSize(_) => {
                            unreachable!()
                        }
                    }
                    if hot_changed {
                        self.hot.store(Arc::new(HotConfig::from_settings(&s)));
                    }
                }
            }
        }
    }

    /// Sets the executor (upstream unexported field `executor`, assigned by test helpers).
    pub fn set_executor(&self, executor: Arc<dyn Executor>) {
        Self::adopt_executor(&mut write(&self.settings), executor);
    }

    /// Sets the broker (upstream unexported field `broker`, assigned by test helpers).
    pub fn set_broker(&self, broker: Arc<dyn Broker>) {
        write(&self.settings).broker = Some(broker);
    }

    pub(super) fn is_inactive(&self) -> bool {
        self.settings().inactive
    }

    /// Attaches the writer (upstream `SendMessages`, which callers run on its own
    /// goroutine): from now on queued frames are written in order until the
    /// queue closes, a close frame is written or a write fails. Frames queued
    /// before are written first. Returns immediately: the writing happens on
    /// the node's task pool (see the module documentation).
    pub fn send_messages(&self) {
        if self.writer.compare_exchange(WRITER_DETACHED, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            if self.nonblocking {
                self.drain_nonblocking();
            } else {
                self.spawn_drain();
            }
        }
    }

    /// Starts a drain unless one is running (or no writer is attached).
    fn kick_writer(&self) {
        if self.writer.compare_exchange(WRITER_IDLE, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            if self.nonblocking {
                self.drain_nonblocking();
            } else {
                self.spawn_drain();
            }
        }
    }

    /// Runs the (blocking) drain on the node's task pool; the writer is busy.
    fn spawn_drain(&self) {
        let session = self.me();
        super::tasks::spawn(Box::new(move || session.drain()));
    }

    /// The next frame to write: a refused one first, then the queue's head.
    fn next_frame(&self) -> Option<Arc<SentFrame>> {
        if let Some(frame) = lock(&self.refused).take() {
            return Some(frame);
        }
        self.write_queue.remove().map(|item| item.data)
    }

    /// Parks a refused frame and transitions the writer to wait for the
    /// transport's writable callback, returning `true` if it already became
    /// writable and this caller re-acquired `WRITER_BUSY`.
    fn park_refused(&self, frame: Arc<SentFrame>) -> bool {
        *lock(&self.refused) = Some(frame);
        self.writer.store(WRITER_WAIT_WRITABLE, Ordering::SeqCst);
        #[cfg(test)]
        tests::pause_park_refused();
        // The transport may have become writable since it refused the frame,
        // while the writer was still busy: take it back if so. Use `load`
        // (never `swap`) so a losing racer cannot consume a wakeup meant for
        // the current `WRITER_BUSY` owner; only the winner clears `writable`
        // before calling `try_write_frame`.
        self.writable.load(Ordering::SeqCst)
            && self
                .writer
                .compare_exchange(WRITER_WAIT_WRITABLE, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
    }

    /// Writes one frame to a non-blocking transport while holding `WRITER_BUSY`.
    /// Close frames and failures hand off to the pool drain so disconnecting
    /// never runs on a producer's thread (a hub gate, a command).
    fn try_send_nonblocking(&self, frame: Arc<SentFrame>, hot: &HotConfig) -> NonblockingSend {
        if frame.frame_type == FrameType::Close {
            *lock(&self.refused) = Some(frame);
            self.spawn_drain();
            return NonblockingSend::HandedOff;
        }

        self.writable.store(false, Ordering::SeqCst);
        match self.conn.try_write_frame(&frame, hot.write_timeout) {
            Ok(true) => {
                hot.record_sent(frame.payload.len() as u64);
                NonblockingSend::Sent
            }
            Ok(false) => {
                if self.park_refused(frame) {
                    NonblockingSend::RetryNow
                } else {
                    NonblockingSend::HandedOff
                }
            }
            Err(_) => {
                // The pool drain writes it again and handles the failure.
                *lock(&self.refused) = Some(frame);
                self.spawn_drain();
                NonblockingSend::HandedOff
            }
        }
    }

    /// Hands frames to a non-blocking transport on the producer's thread while
    /// it takes them; the writer is busy. When the transport is full, the frame
    /// is kept and the drain stops: the transport's writable handler resumes it.
    fn drain_nonblocking(&self) {
        let hot = self.hot.load();
        loop {
            let Some(frame) = self.next_frame() else {
                self.writer.store(WRITER_IDLE, Ordering::SeqCst);
                if self.write_queue.is_empty_atomic()
                    || self
                        .writer
                        .compare_exchange(WRITER_IDLE, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                {
                    return;
                }
                continue;
            };
            match self.try_send_nonblocking(frame, &hot) {
                NonblockingSend::Sent | NonblockingSend::RetryNow => {}
                NonblockingSend::HandedOff => return,
            }
        }
    }

    /// Writes queued frames until the queue is empty (one iteration of
    /// upstream's writer loop per frame).
    fn drain(self: Arc<Self>) {
        let metrics = self.metrics();
        loop {
            let Some(frame) = self.next_frame() else {
                self.writer.store(WRITER_IDLE, Ordering::SeqCst);
                // A frame queued after the queue looked empty may have seen the
                // drain as busy: take it over unless another drain started.
                if self.write_queue.is_empty()
                    || self
                        .writer
                        .compare_exchange(WRITER_IDLE, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                {
                    return;
                }
                continue;
            };

            let res = self.write_frame(&frame);

            if frame.frame_type == FrameType::Close {
                self.writer.store(WRITER_DONE, Ordering::SeqCst);
                self.disconnect_now("Close frame sent", CLOSE_NORMAL_CLOSURE);
                return;
            }

            if let Err(e) = res {
                self.writer.store(WRITER_DONE, Ordering::SeqCst);
                if e.is_timeout() {
                    log_debug!(self.logger(), "write timed out",
                        "error" => e.to_string(), "msg" => compact_value_bytes(&frame.payload).log_value());
                    metrics.counter_increment(METRICS_WRITE_TIMEOUT);
                }
                metrics.counter_increment(METRICS_FAILED_SENT);
                self.disconnect_now("Write Failed", CLOSE_ABNORMAL_CLOSURE);
                return;
            }

            metrics.counter_increment(METRICS_SENT_MSG);
        }
    }

    /// Schedules the ping, keepalive and presence timers (upstream unexported
    /// `startTimers`).
    pub fn start_timers(&self) {
        let (ping_interval, pong_timeout, resume_interval, presence_interval, clock) = {
            let s = self.settings();
            (s.ping_interval, s.pong_timeout, s.resume_interval, s.presence_interval, s.clock.clone())
        };
        let now = clock.now();
        let mut t = lock(&self.timers);

        if !ping_interval.is_zero() {
            // Spread the first pings of sessions connecting together over
            // [interval/2, interval*3/2), as upstream.
            let min = ping_interval / 2;
            let span = (ping_interval * 3 / 2).saturating_sub(min);
            let initial = min + random_below(span);
            t.ping = Some(now + initial);
            if !pong_timeout.is_zero() {
                t.pong = Some(now + pong_timeout + initial);
            }
        }
        if !resume_interval.is_zero() {
            t.resume = Some(now + resume_interval);
        }
        if !presence_interval.is_zero() {
            t.presence = Some(now + presence_interval);
        }

        t.started = true;
        self.arm(&mut t, &clock);
    }

    fn clock_now(&self) -> Instant {
        self.settings().clock.now()
    }

    /// Arms the shared timer for the earliest deadline (upstream `schedule`).
    fn arm(&self, t: &mut Timers, clock: &Arc<dyn Clock>) {
        if let Some(timer) = t.timer.take() {
            timer.stop();
        }
        if !t.started || t.stopped {
            return;
        }
        let Some((ev, at)) = t.earliest() else { return };
        t.next_event = Some(ev);
        t.generation += 1;
        let generation = t.generation;
        let me = self.me.clone();
        let delay = at.saturating_duration_since(clock.now());
        t.timer = Some(clock.after(
            delay,
            Box::new(move || {
                if let Some(session) = me.upgrade() {
                    session.on_timer(generation);
                }
            }),
        ));
    }

    /// Re-arms after a deadline changed (upstream `timers.Schedule()`).
    fn schedule(&self) {
        let clock = self.settings().clock.clone();
        let mut t = lock(&self.timers);
        self.arm(&mut t, &clock);
    }

    fn on_timer(self: &Arc<Self>, generation: u64) {
        let ev = {
            let t = lock(&self.timers);
            if t.generation != generation || t.stopped {
                return;
            }
            t.next_event
        };
        if let Some(ev) = ev {
            self.handle_timer_event(ev);
        }
    }

    fn handle_timer_event(self: &Arc<Self>, ev: TimerEvent) {
        if self.is_closed() {
            return;
        }
        match ev {
            TimerEvent::Handshake => self.maybe_disconnect_idle(),
            TimerEvent::Presence => {
                let (broker, interval) = {
                    let s = self.settings();
                    (s.broker.clone(), s.presence_interval)
                };
                if let Some(b) = broker {
                    let _ = b.touch_presence(&self.get_id());
                }
                let now = self.clock_now();
                {
                    let mut t = lock(&self.timers);
                    if !interval.is_zero() {
                        t.presence = Some(now + interval);
                    }
                }
                self.schedule();
            }
            TimerEvent::Expire => {
                let (broker, interval) = {
                    let s = self.settings();
                    (s.broker.clone(), s.resume_interval)
                };
                if let Some(b) = broker {
                    let _ = b.touch_session(&self.get_id());
                }
                let now = self.clock_now();
                {
                    let mut t = lock(&self.timers);
                    if !interval.is_zero() {
                        t.resume = Some(now + interval);
                    }
                }
                self.schedule();
            }
            TimerEvent::Ping => self.send_ping(),
            TimerEvent::Pong => self.handle_no_pong(),
        }
    }

    fn maybe_disconnect_idle(self: &Arc<Self>) {
        lock(&self.timers).handshake = None;

        if self.is_connected() {
            self.schedule();
            return;
        }

        self.logger().log(cabby_core::logger::Level::Warn, "disconnecting idle session", &[]);

        self.send(&DisconnectMessage::new(cabby_core::common::IDLE_TIMEOUT_REASON, false));
        self.disconnect("Idle Timeout", CLOSE_NORMAL_CLOSURE);
    }

    fn send_ping(self: &Arc<Self>) {
        if self.is_closed() {
            return;
        }
        let (interval, precision, native) = {
            let s = self.settings();
            (s.ping_interval, s.ping_precision.clone(), s.native_ping)
        };

        let deadline = Instant::now() + interval / 2;
        let mut res = match self.encoder().encode(&new_ping_message(&precision)) {
            // Behind the frames already queued: a non-blocking transport is never
            // waited for (a dead connection fails the queued writes instead).
            Ok(frame) if self.nonblocking => {
                self.push_frame(Arc::new(frame));
                Ok(())
            }
            Ok(frame) => self.write_frame_with_deadline(&Arc::new(frame), deadline),
            Err(e) => {
                log_error!(self.logger(), "failed to encode ping message", "error" => e.0.clone());
                Err(ConnectionError(e.0))
            }
        };

        if native {
            res = self.conn.write_ping(Instant::now() + interval / 2);
        }

        if res.is_err() {
            self.disconnect("Ping failed", CLOSE_ABNORMAL_CLOSURE);
            return;
        }

        self.schedule_ping();
    }

    fn schedule_ping(&self) {
        let now = self.clock_now();
        let (interval, pong_timeout, clock) = {
            let s = self.settings();
            (s.ping_interval, s.pong_timeout, s.clock.clone())
        };
        let mut t = lock(&self.timers);
        t.ping = Some(now + interval);
        if !pong_timeout.is_zero() && t.pong.is_none() {
            t.pong = Some(now + pong_timeout);
        }
        self.arm(&mut t, &clock);
    }

    /// A client message arrived: the client is alive (upstream `keepalive`).
    pub(super) fn keepalive(&self) {
        let clock = self.settings().clock.clone();
        let mut t = lock(&self.timers);
        if t.pong.take().is_some() {
            self.arm(&mut t, &clock);
        }
    }

    /// Pushes the pong deadline by one timeout (upstream `resetPong`).
    pub(super) fn reset_pong(&self) {
        let (pong_timeout, clock) = {
            let s = self.settings();
            (s.pong_timeout, s.clock.clone())
        };
        let mut t = lock(&self.timers);
        let Some(pong) = t.pong else { return };
        t.pong = Some(pong.checked_add(pong_timeout).unwrap_or(pong));
        self.arm(&mut t, &clock);
    }

    fn handle_no_pong(self: &Arc<Self>) {
        if !self.is_connected() {
            return;
        }
        self.logger().log(cabby_core::logger::Level::Warn, "disconnecting session due to no pongs", &[]);
        self.send(&DisconnectMessage::new(NO_PONG_REASON, true));
        self.disconnect("No Pong", CLOSE_NORMAL_CLOSURE);
    }

    /// Answers a client ping (upstream `handleClientPing`).
    pub(super) fn handle_client_ping(&self) {
        log_debug!(self.logger(), "ping received");
        self.send(&Reply { kind: PONG_TYPE.to_owned(), ..Default::default() });
    }

    /// Marks the session as served by the Pusher executor.
    pub fn mark_pusher(&self) {
        self.pusher.store(true, Ordering::Release);
    }

    /// Whether the session's commands go through the Pusher executor.
    pub fn is_pusher(&self) -> bool {
        self.pusher.load(Ordering::Acquire)
    }

    /// Overrides the ping interval (upstream unexported field `pingInterval`).
    pub fn set_ping_interval(&self, interval: Duration) {
        write(&self.settings).ping_interval = bounded(interval);
    }

    /// Overrides the cache keepalive interval (upstream unexported field `resumeInterval`).
    pub fn set_resume_interval(&self, interval: Duration) {
        write(&self.settings).resume_interval = bounded(interval);
    }

    /// Overrides the pending bytes limit (upstream unexported `maxPendingSize.Store`).
    pub fn set_max_pending_size(&self, size: u64) {
        self.max_pending_size.store(size, Ordering::Release);
    }

    /// Overrides the connected flag (upstream unexported `connected.Store`).
    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Release);
    }

    /// Overrides the Disconnect interest without the sticky semantics of
    /// [`Session::mark_disconnectable`] (upstream unexported field `disconnectInterest`).
    pub fn set_disconnect_interest(&self, interest: bool) {
        lock(&self.lifecycle).disconnect_interest = interest;
    }

    /// The session's subscriptions (upstream unexported field `subscriptions`).
    pub fn subscriptions(&self) -> &SubscriptionState {
        &self.subscriptions
    }

    /// Cancelled once the session is closed (upstream unexported `closeCtx`).
    pub fn close_context(&self) -> CancellationToken {
        self.close_token.clone()
    }

    /// A snapshot of the session env (upstream `GetEnv`).
    pub fn get_env(&self) -> SessionEnv {
        lock(&self.env).clone()
    }

    /// Replaces the session env (upstream `SetEnv`).
    pub fn set_env(&self, env: SessionEnv) {
        *lock(&self.env) = env;
    }

    /// Mutates the env in place (upstream tests write `session.env.MergeChannelState(...)`).
    pub fn update_env(&self, f: &mut dyn FnMut(&mut SessionEnv)) {
        f(&mut lock(&self.env));
    }

    /// Reads a channel state field (upstream `env.GetChannelStateField`).
    pub(super) fn channel_state_field(&self, id: &str, field: &str) -> String {
        lock(&self.env).get_channel_state_field(id, field)
    }

    /// Merges connection and channel states into the env, atomically with
    /// respect to other merges (upstream `MergeEnv`).
    pub fn merge_env(&self, env: &SessionEnv) {
        let mut cur = lock(&self.env);
        if let Some(state) = &env.connection_state {
            cur.merge_connection_state(state);
        }
        if let Some(states) = &env.channel_states {
            for (id, state) in states {
                cur.merge_channel_state(id, state);
            }
        }
    }

    pub fn authenticate_on_connect(&self) -> bool {
        lock(&self.timers).handshake.is_none()
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    pub fn is_resumeable(&self) -> bool {
        let s = self.settings();
        !s.resume_interval.is_zero() && !s.inactive
    }

    pub fn get_id(&self) -> String {
        self.settings().uid.clone()
    }

    pub fn set_id(&self, id: &str) {
        write(&self.settings).uid = id.to_owned();
    }

    pub fn get_identifiers(&self) -> String {
        lock(&self.env).identifiers.clone()
    }

    pub fn set_identifiers(&self, ids: &str) {
        lock(&self.env).identifiers = ids.to_owned();
    }

    pub fn is_disconnectable(&self) -> bool {
        lock(&self.lifecycle).disconnect_interest
    }

    /// Sticky: once marked disconnectable, `false` does not reset it.
    pub fn mark_disconnectable(&self, val: bool) {
        let mut l = lock(&self.lifecycle);
        l.disconnect_interest = l.disconnect_interest || val;
    }

    /// Holds the registration lock (upstream `rmu`) for the node.
    pub(super) fn registration(&self) -> MutexGuard<'_, ()> {
        lock(&self.registration)
    }

    /// Starts the reader loop; `callback` runs when it ends (upstream `Serve`).
    pub fn serve(self: &Arc<Self>, callback: Box<dyn FnOnce() + Send>) -> Result<(), NodeError> {
        struct Finally(Option<Box<dyn FnOnce() + Send>>);
        impl Drop for Finally {
            fn drop(&mut self) {
                if let Some(f) = self.0.take() {
                    f();
                }
            }
        }

        let session = self.clone();
        std::thread::Builder::new()
            .name("cabby-session-reader".into())
            .spawn(move || {
                let _finally = Finally(Some(callback));
                loop {
                    if session.is_closed() {
                        return;
                    }
                    match session.conn.read() {
                        Err(e) => {
                            session.read_ended(&e);
                            return;
                        }
                        Ok(message) => {
                            if let Err(e) = session.read_message(&message) {
                                log_debug!(session.logger(), "WebSocket read failed", "error" => e.0);
                                return;
                            }
                        }
                    }
                }
            })
            .map_err(|e| NodeError(e.to_string()))?;
        Ok(())
    }

    /// The connection's read failed or it was closed (upstream `Serve` on a
    /// failed `Read`).
    fn read_ended(self: &Arc<Self>, e: &ConnectionError) {
        if e.is_close() {
            log_debug!(self.logger(), "WebSocket closed", "error" => e.to_string());
            self.disconnect_now("Read closed", CLOSE_NORMAL_CLOSURE);
        } else {
            log_debug!(self.logger(), "WebSocket close error", "error" => e.to_string());
            self.disconnect_now("Read failed", CLOSE_ABNORMAL_CLOSURE);
        }
    }

    /// The push-model counterpart of [`Session::serve`]: instead of a reader
    /// thread blocking on [`Connection::read`], the transport hands every client
    /// message (and the end of the connection) to the returned reader, which
    /// executes them in order on the node's command pool. `callback` runs when
    /// reading ends, as with `serve`.
    pub fn reader(self: &Arc<Self>, callback: Box<dyn FnOnce() + Send>) -> SessionReader {
        let reader = SessionReader {
            inner: Arc::new(ReaderInner {
                session: self.clone(),
                state: Mutex::new(ReaderState {
                    events: VecDeque::new(),
                    running: false,
                    discarding: false,
                    finished: false,
                    callback: Some(callback),
                    parked: None,
                }),
            }),
        };
        // Upstream's reader checks whether the session closed before its first
        // read: a session closed already (a rejected connection) reads nothing,
        // and the callback runs at once.
        if self.is_closed() {
            reader.inner.finish(None);
        }
        reader
    }

    /// Decodes and executes one client message (upstream `ReadMessage`).
    pub fn read_message(self: &Arc<Self>, message: &[u8]) -> Result<(), NodeError> {
        let metrics = self.metrics();
        metrics.counter_add(METRICS_DATA_RECEIVED, message.len() as u64);

        let command = match self.encoder().decode(message) {
            Ok(c) => c,
            Err(e) if cabby_core::gojson::deep::hit_default_limit(&e.0) => {
                // Nested deeper than serde_json's default limit, perhaps within
                // Go's: decoded, handled and dropped on a thread whose stack fits it.
                return match cabby_core::gojson::deep::with_deep_nesting(|| self.read_deep_message(message)) {
                    Ok(res) => res,
                    Err(DeepNestingError::Busy) => {
                        // Deviation D-056: too many deep messages at once; this
                        // one is dropped and the session keeps reading.
                        metrics.counter_increment(METRICS_FAILED_COMMAND_RECEIVED);
                        log_warn!(self.logger(), "failed to handle incoming message",
                            "data" => compact_value_bytes(message).log_value(),
                            "error" => DeepNestingError::Busy.to_string());
                        Ok(())
                    }
                    Err(DeepNestingError::Spawn) => {
                        metrics.counter_increment(METRICS_FAILED_COMMAND_RECEIVED);
                        Err(NodeError(e.0))
                    }
                };
            }
            Err(e) => {
                metrics.counter_increment(METRICS_FAILED_COMMAND_RECEIVED);
                return Err(NodeError(e.0));
            }
        };

        self.execute_decoded(message, &command, false);
        Ok(())
    }

    /// [`Session::read_message`] after its data was counted, on a deep-nesting thread.
    fn read_deep_message(self: &Arc<Self>, message: &[u8]) -> Result<(), NodeError> {
        let command = self.encoder().decode(message).map_err(|e| {
            self.metrics().counter_increment(METRICS_FAILED_COMMAND_RECEIVED);
            NodeError(e.0)
        })?;
        self.execute_decoded(message, &command, false);
        Ok(())
    }

    /// Decodes `message` and executes it right away if the executor says it
    /// never blocks ([`Executor::is_inline`]); `false` (nothing done) otherwise.
    fn try_read_inline(self: &Arc<Self>, message: &[u8]) -> bool {
        let Ok(command) = self.encoder().decode(message) else {
            return false;
        };
        match self.executor() {
            Some(executor) if executor.is_inline(&command) => {
                self.execute_decoded(message, &command, true);
                true
            }
            _ => false,
        }
    }

    fn execute_decoded(self: &Arc<Self>, message: &[u8], command: &Message, count_data: bool) {
        let metrics = self.metrics();
        if count_data {
            metrics.counter_add(METRICS_DATA_RECEIVED, message.len() as u64);
        }
        metrics.counter_increment(METRICS_RECEIVED_MSG);

        if let Some(executor) = self.executor()
            && let Err(e) = executor.handle_command(self, command)
        {
            metrics.counter_increment(METRICS_FAILED_COMMAND_RECEIVED);
            log_warn!(self.logger(), "failed to handle incoming message",
                "data" => compact_value_bytes(message).log_value(), "error" => e.0);
        }
    }

    /// Encodes and queues a message (upstream `Send`).
    pub fn send(&self, msg: &dyn EncodedMessageJson) {
        match self.encoder().encode(msg) {
            Ok(frame) => self.push_frame(Arc::new(frame)),
            Err(e) => {
                log_warn!(self.logger(), "failed to encode message",
                    "data" => encoded_log_value(msg), "type" => msg.get_type(), "error" => e.0);
            }
        }
    }

    /// Queues a raw transmission from an RPC result (upstream `SendJSONTransmission`).
    pub fn send_json_transmission(&self, msg: &str) {
        match self.encoder().encode_transmission(msg) {
            Ok(frame) => self.push_frame(Arc::new(frame)),
            Err(e) => {
                log_warn!(self.logger(), "failed to encode transmission",
                    "data" => compact_value(msg).log_value(), "error" => e.0);
            }
        }
    }

    /// Queues a frame, disconnecting slow clients over the pending size limit
    /// (upstream unexported `sendFrame`).
    pub fn send_frame(&self, frame: SentFrame) {
        self.push_frame(Arc::new(frame));
    }

    fn push_frame(&self, frame: Arc<SentFrame>) {
        let hot = self.hot.load();
        self.push_frame_hot(frame, &hot);
    }

    fn push_frame_hot(&self, frame: Arc<SentFrame>, hot: &HotConfig) {
        // Fast path for non-blocking transports (WebSocket): when the writer is
        // idle and nothing is queued, hand the frame straight to the transport
        // without touching `write_queue` or `refused`.
        if self.nonblocking
            && frame.frame_type != FrameType::Close
            && self.writer.compare_exchange(WRITER_IDLE, WRITER_BUSY, Ordering::SeqCst, Ordering::SeqCst).is_ok()
        {
            if self.write_queue.is_closed_atomic() {
                self.writer.store(WRITER_DONE, Ordering::SeqCst);
                return;
            }
            if !self.write_queue.is_empty_atomic() {
                // Another thread enqueued before releasing the writer; preserve
                // FIFO order by queuing behind it and draining while we hold
                // `WRITER_BUSY`.
                if self.enqueue_frame(frame) {
                    self.drain_nonblocking();
                } else {
                    self.writer.store(WRITER_IDLE, Ordering::SeqCst);
                    if !self.write_queue.is_empty_atomic() {
                        self.kick_writer();
                    }
                }
                return;
            }

            match self.try_send_nonblocking(frame, hot) {
                NonblockingSend::Sent => {
                    if !self.write_queue.is_empty_atomic() {
                        self.drain_nonblocking();
                        return;
                    }
                    self.writer.store(WRITER_IDLE, Ordering::SeqCst);
                    if !self.write_queue.is_empty_atomic() {
                        self.kick_writer();
                    }
                }
                NonblockingSend::RetryNow => self.drain_nonblocking(),
                NonblockingSend::HandedOff => {}
            }
            return;
        }

        if self.enqueue_frame(frame) {
            self.kick_writer();
        }
    }

    fn enqueue_frame(&self, frame: Arc<SentFrame>) -> bool {
        let max_pending = self.max_pending_size.load(Ordering::Acquire);
        let size = frame.payload.len() as u64;
        match self.write_queue.push(Item { size, data: frame }, max_pending) {
            PushResult::Ok => true,
            PushResult::Closed => false,
            PushResult::Overflow => {
                self.max_pending_size.store(0, Ordering::Release);
                log_debug!(self.logger(), "slow client detected, disconnecting", "max" => max_pending);
                self.metrics().counter_increment(METRICS_DISCONNECTED_SLOW_CLIENTS);
                self.me()
                    .disconnect_with_message(&DisconnectMessage::new(SLOW_CLIENT_REASON, true), SLOW_CLIENT_REASON);
                false
            }
        }
    }

    /// Queues a close frame, closes the session and detaches it from the node
    /// (upstream `Disconnect`).
    pub fn disconnect(self: &Arc<Self>, reason: &str, code: i32) {
        self.send_close(reason, code);
        self.close();
        self.disconnect_from_node();
    }

    /// Sends `msg`, then disconnects with the WebSocket code matching `code`
    /// (upstream `DisconnectWithMessage`).
    pub fn disconnect_with_message(self: &Arc<Self>, msg: &dyn EncodedMessageJson, code: &str) {
        // Closed before the message is queued, so whoever reads the message
        // sees a closed session (upstream closes right after queuing).
        self.close();
        self.send(msg);
        self.finish_disconnect_with_message(code);
    }

    fn finish_disconnect_with_message(self: &Arc<Self>, code: &str) {
        let (reason, ws_code) = match code {
            SERVER_RESTART_REASON => ("Server restart", CLOSE_GOING_AWAY),
            REMOTE_DISCONNECT_REASON => ("Closed remotely", CLOSE_NORMAL_CLOSURE),
            _ => ("", CLOSE_NORMAL_CLOSURE),
        };
        self.disconnect(reason, ws_code);
    }

    /// Closes first, then detaches from the node and writes the close frame
    /// directly (upstream `DisconnectNow`).
    pub fn disconnect_now(self: &Arc<Self>, reason: &str, code: i32) {
        if !self.is_connected() {
            return;
        }

        // Close first: detaching from the node may block for a long time (a
        // full disconnect queue), and in-flight commands must see the session
        // as closed meanwhile.
        self.close();

        self.disconnect_from_node();
        let _ = self.write_frame(&Arc::new(SentFrame::close(code, reason)));

        self.write_queue.close();
    }

    fn disconnect_from_node(self: &Arc<Self>) {
        let was_connected = {
            let _reg = self.registration();
            self.connected.swap(false, Ordering::AcqRel)
        };

        if was_connected && let Some(executor) = self.executor() {
            let _ = executor.disconnect(self);
        }
    }

    fn close(&self) {
        {
            let mut l = lock(&self.lifecycle);
            if l.closed {
                return;
            }
            l.closed = true;
        }

        {
            let mut t = lock(&self.timers);
            t.stopped = true;
            if let Some(timer) = t.timer.take() {
                timer.stop();
            }
        }

        self.close_token.cancel();
    }

    pub fn is_closed(&self) -> bool {
        lock(&self.lifecycle).closed
    }

    fn send_close(&self, reason: &str, code: i32) {
        self.send_frame(SentFrame::close(code, reason));
    }

    fn write_frame(&self, frame: &Arc<SentFrame>) -> Result<(), ConnectionError> {
        let timeout = self.settings().write_timeout;
        self.write_frame_with_deadline(frame, Instant::now() + timeout)
    }

    fn write_frame_with_deadline(&self, frame: &Arc<SentFrame>, deadline: Instant) -> Result<(), ConnectionError> {
        match frame.frame_type {
            FrameType::Text | FrameType::Binary => {
                let res = {
                    let _w = lock(&self.write_lock);
                    self.conn.write_frame(frame, deadline)
                };
                if res.is_ok() {
                    self.metrics().counter_add(METRICS_DATA_SENT, frame.payload.len() as u64);
                }
                res
            }
            FrameType::Close => {
                self.conn.close(frame.close_code, &frame.close_reason);
                Err(ConnectionError("closed".into()))
            }
        }
    }

    /// Restores identifiers, states, subscriptions and the Disconnect interest
    /// from a cache entry (upstream `RestoreFromCache`).
    pub fn restore_from_cache(&self, cached: &[u8]) -> Result<(), NodeError> {
        let entry = decode_cache_entry(cached)?;

        self.mark_disconnectable(entry.disconnectable);
        {
            let mut env = lock(&self.env);
            env.identifiers = entry.ids;
            if let Some(cstate) = &entry.cstate {
                env.merge_connection_state(cstate);
            }
            for (id, state) in &entry.istate {
                env.merge_channel_state(id, state);
            }
        }

        for (id, streams) in &entry.subs {
            self.subscriptions.add_channel(id);
            for stream in streams {
                self.subscriptions.add_channel_stream(id, stream);
            }
        }

        Ok(())
    }

    pub fn prev_sid(&self) -> String {
        self.settings().prev_sid.clone()
    }

    /// Native pong received (upstream `HandleNativePong`). As upstream, where
    /// nothing reads them, they no longer count after an undecodable message.
    pub fn handle_native_pong(&self) {
        if !lock(&self.lifecycle).discarding {
            self.reset_pong();
        }
    }

    pub fn write_internal_state(&self, key: &str, val: Value) {
        lock(&self.lifecycle).internal_state.get_or_insert_with(HashMap::new).insert(key.to_owned(), val);
    }

    pub fn read_internal_state(&self, key: &str) -> Option<Value> {
        lock(&self.lifecycle).internal_state.as_ref().and_then(|m| m.get(key).cloned())
    }
}

/// Holds the transport's permission to read the next message: dropping it lets
/// the transport read on (see [`SessionReader::message`]).
pub type ReadPermit = Box<dyn Send>;

enum ReadEvent {
    Message(Bytes, ReadPermit),
    End(ConnectionError),
}

struct ReaderState {
    events: VecDeque<ReadEvent>,
    /// A drain is scheduled or running on the command pool.
    running: bool,
    /// A message could not be decoded: later messages are read and dropped, so
    /// the end of the connection is still noticed (D-074).
    discarding: bool,
    /// Reading ended (upstream's reader goroutine returned): later events are dropped.
    finished: bool,
    callback: Option<Box<dyn FnOnce() + Send>>,
    /// The permit of a message that found the session closed: kept, so the
    /// transport reads no further (as nothing reads after upstream's reader returned).
    parked: Option<ReadPermit>,
}

struct ReaderInner {
    session: Arc<Session>,
    state: Mutex<ReaderState>,
}

/// Executes a session's client messages pushed by its transport (see
/// [`Session::reader`]): upstream's reader loop (`Serve`) without a thread.
///
/// Messages of one session run one at a time, in arrival order, on the node's
/// command pool; a session with nothing to execute holds no thread. Each
/// message comes with a permit the transport hands over: it is released once
/// the message has been executed, and the transport reads the next message only
/// then, so a client that sends faster than its commands execute is slowed down
/// through TCP, as with upstream's blocking reads.
#[derive(Clone)]
pub struct SessionReader {
    inner: Arc<ReaderInner>,
}

impl std::fmt::Debug for SessionReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionReader").field("session", &self.inner.session).finish_non_exhaustive()
    }
}

impl SessionReader {
    /// A client message; `permit` is dropped once it has been executed.
    ///
    /// A client ping that arrives while the session has nothing queued is answered
    /// on the calling (transport) thread: it only queues a pong, and running it
    /// here keeps it from waiting behind other sessions' RPC-bound commands on a
    /// busy command pool. Order is kept, as nothing of this session is pending.
    pub fn message(&self, msg: Bytes, permit: ReadPermit) {
        if may_be_inline(&msg) && self.try_inline(&msg) {
            drop(permit);
            return;
        }
        self.push(ReadEvent::Message(msg, permit));
    }

    /// Runs `msg` now if the session is idle and the command never blocks.
    fn try_inline(&self, msg: &[u8]) -> bool {
        {
            let mut state = lock(&self.inner.state);
            if state.running || state.discarding || state.finished || !state.events.is_empty() {
                return false;
            }
            state.running = true;
        }
        let session = &self.inner.session;
        let done = !session.is_closed() && session.try_read_inline(msg);

        let mut state = lock(&self.inner.state);
        if state.events.is_empty() {
            state.running = false;
            return done;
        }
        // Messages arrived meanwhile: hand them to the command pool (still `running`).
        drop(state);
        let inner = self.inner.clone();
        super::tasks::execute(Box::new(move || inner.drain()));
        done
    }

    /// The connection ended with `err` (a read failure or a close): the session
    /// is disconnected after the messages received before (upstream `Serve` on a
    /// failed `Read`).
    pub fn end(&self, err: ConnectionError) {
        self.push(ReadEvent::End(err));
    }

    fn push(&self, event: ReadEvent) {
        let mut state = lock(&self.inner.state);
        if state.finished || (state.discarding && matches!(event, ReadEvent::Message(..))) {
            return;
        }
        state.events.push_back(event);
        if state.running {
            return;
        }
        state.running = true;
        drop(state);
        let inner = self.inner.clone();
        super::tasks::execute(Box::new(move || inner.drain()));
    }
}

impl ReaderInner {
    fn drain(self: Arc<Self>) {
        loop {
            if super::tasks::should_yield() {
                // Back from an RPC call with the command pool over its seat limit:
                // the rest of this session's messages wait for a seat (in order,
                // as the reader stays `running`).
                super::tasks::execute(Box::new(move || self.drain()));
                return;
            }
            let (event, discarding) = {
                let mut state = lock(&self.state);
                match state.events.pop_front() {
                    Some(event) if !state.finished => (event, state.discarding),
                    _ => {
                        state.events.clear();
                        state.running = false;
                        return;
                    }
                }
            };
            let session = &self.session;
            match event {
                ReadEvent::Message(msg, permit) => {
                    // Upstream checks before every read whether the session closed.
                    if session.is_closed() {
                        self.finish(Some(permit));
                        continue;
                    }
                    if let Err(e) = session.read_message(&msg) {
                        log_debug!(session.logger(), "WebSocket read failed", "error" => e.0);
                        self.discard();
                    }
                    drop(permit);
                }
                ReadEvent::End(err) => {
                    if !discarding || !session.is_closed() {
                        session.read_ended(&err);
                    }
                    self.finish(None);
                }
            }
        }
    }

    /// A message could not be decoded: no later message is executed (upstream's
    /// reader returns) and the callback runs, but the transport keeps reading, so
    /// a closed connection still ends the session (D-074).
    fn discard(&self) {
        lock(&self.session.lifecycle).discarding = true;
        let (callback, dropped) = {
            let mut state = lock(&self.state);
            state.discarding = true;
            let (messages, ends) =
                std::mem::take(&mut state.events).into_iter().partition(|e| matches!(e, ReadEvent::Message(..)));
            state.events = ends;
            let dropped: VecDeque<ReadEvent> = messages;
            (state.callback.take(), dropped)
        };
        drop(dropped);
        if let Some(callback) = callback {
            callback();
        }
    }

    /// Reading ended: later events are dropped and the callback runs (once).
    fn finish(&self, permit: Option<ReadPermit>) {
        let callback = {
            let mut state = lock(&self.state);
            state.finished = true;
            if permit.is_some() {
                state.parked = permit;
            }
            state.callback.take()
        };
        if let Some(callback) = callback {
            callback();
        }
    }
}

/// Cheap filter for [`SessionReader::try_inline`]: short frames that mention a
/// ping or pong (the only inline commands; other frames skip the extra decode).
fn may_be_inline(msg: &[u8]) -> bool {
    msg.len() <= 256 && msg.windows(4).any(|w| w == b"ping" || w == b"pong")
}

/// The `data` of upstream's "failed to encode message" (`slog.Any("data", msg)`:
/// the message's `LogValue`).
fn encoded_log_value(msg: &dyn EncodedMessageJson) -> LogValue {
    if let Some(reply) = msg.as_reply() {
        return reply.log_value();
    }
    if let Some(disconnect) = msg.as_disconnect() {
        return disconnect.log_value();
    }
    compact_value_bytes(&cabby_core::gojson::to_vec(&msg.to_json_value())).log_value()
}

/// A uniformly distributed duration in `[0, span)` (`ZERO` for an empty span).
fn random_below(span: Duration) -> Duration {
    let nanos = span.as_nanos() as u64;
    if nanos == 0 {
        return Duration::ZERO;
    }
    let mut bytes = [0u8; 8];
    let random = if getrandom::fill(&mut bytes).is_ok() {
        u64::from_le_bytes(bytes)
    } else {
        // No entropy source: the clock's sub-second digits still spread sessions.
        u64::from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos())
    };
    Duration::from_nanos(random % nanos)
}

/// Upstream `newPingMessage(format)`: the current time in seconds, or in
/// milliseconds / nanoseconds for `ms` / `ns`.
fn new_ping_message(format: &str) -> PingMessage {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let ts: u128 = match format {
        "ns" => now.as_nanos(),
        "ms" => now.as_millis(),
        _ => u128::from(now.as_secs()),
    };
    PingMessage { kind: "ping".to_owned(), message: Some(Value::from(ts as u64)) }
}

/// The session as a hub participant. The hub calls `send` and
/// `disconnect_with_message` from its gate threads with a
/// [`CachedEncodedMessage`](cabby_core::hub::CachedEncodedMessage): fetch the
/// frame for the session's encoder with `fetch_with` (encoded once per broadcast
/// and encoder, shared as `Arc<SentFrame>`) and queue it without blocking.
impl cabby_core::hub::HubSession for Session {
    fn get_id(&self) -> String {
        Session::get_id(self)
    }

    fn get_identifiers(&self) -> String {
        Session::get_identifiers(self)
    }

    fn send(&self, msg: &cabby_core::hub::EncodedMessage) {
        let hot = self.hot.load();
        if let Ok(frame) = msg.fetch_with(hot.encoder.as_ref()) {
            self.push_frame_hot(frame, &hot);
        }
    }

    fn disconnect_with_message(&self, msg: &cabby_core::hub::EncodedMessage, code: &str) {
        let me = self.me();
        me.close();
        cabby_core::hub::HubSession::send(self, msg);
        me.finish_disconnect_with_message(code);
    }
}

/// The cache entry: JSON with `ids`, `subs`, `cstate`, `istate` and
/// `Disconnectable` (upstream `ToCacheEntry`).
impl Cacheable for Session {
    fn to_cache_entry(&self) -> Result<Vec<u8>, BrokerError> {
        let disconnectable = self.is_disconnectable();
        let subs: BTreeMap<String, Vec<String>> = self.subscriptions.to_map().into_iter().collect();
        let env = lock(&self.env);
        let entry = CacheEntryOut {
            ids: &env.identifiers,
            subs,
            cstate: env.connection_state.as_ref().map(|m| m.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()),
            istate: env.channel_states.as_ref().map(|states| {
                states
                    .iter()
                    .map(|(id, m)| (id.as_str(), m.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect()))
                    .collect()
            }),
            disconnectable,
        };
        Ok(cabby_core::gojson::to_vec(&entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{Receiver, Sender, channel};

    use crate::node::Ticker;

    thread_local! {
        /// Runs once when this thread next parks a refused frame, before it checks
        /// whether the transport became writable (see `park_refused`).
        static PARK_PAUSE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    }

    /// The pause point in [`Session::park_refused`].
    pub(super) fn pause_park_refused() {
        if let Some(pause) = PARK_PAUSE.take() {
            pause();
        }
    }

    type Callback = Box<dyn FnOnce() + Send>;
    /// Elapsed time, last timer id, pending timers (id, due, callback).
    type ClockState = (Duration, u64, Vec<(u64, Duration, Callback)>);

    /// A clock that moves only when the test advances it; callbacks run on the
    /// advancing thread.
    #[derive(Clone)]
    struct TestClock {
        base: Instant,
        state: Arc<Mutex<ClockState>>,
    }

    struct TestTimer {
        state: Arc<Mutex<ClockState>>,
        id: u64,
    }

    impl Timer for TestTimer {
        fn stop(&self) -> bool {
            let mut s = lock(&self.state);
            let before = s.2.len();
            s.2.retain(|(id, _, _)| *id != self.id);
            before != s.2.len()
        }
    }

    impl TestClock {
        fn new() -> Self {
            Self { base: Instant::now(), state: Arc::new(Mutex::new((Duration::ZERO, 0, Vec::new()))) }
        }

        fn advance(&self, d: Duration) {
            let target = lock(&self.state).0 + d;
            loop {
                let cb = {
                    let mut s = lock(&self.state);
                    let next = s.2.iter().enumerate().filter(|(_, t)| t.1 <= target).min_by_key(|(_, t)| (t.1, t.0));
                    match next.map(|(i, _)| i) {
                        Some(i) => {
                            let (_, due, cb) = s.2.remove(i);
                            s.0 = due;
                            cb
                        }
                        None => {
                            s.0 = target;
                            return;
                        }
                    }
                };
                cb();
            }
        }

        fn pending(&self) -> usize {
            lock(&self.state).2.len()
        }
    }

    impl Clock for TestClock {
        fn now(&self) -> Instant {
            self.base + lock(&self.state).0
        }
        fn after(&self, delay: Duration, f: Callback) -> Box<dyn Timer> {
            let mut s = lock(&self.state);
            s.1 += 1;
            let (id, due) = (s.1, s.0 + delay);
            s.2.push((id, due, f));
            Box::new(TestTimer { state: self.state.clone(), id })
        }
        fn ticker(&self, _period: Duration) -> Arc<dyn Ticker> {
            unimplemented!("not used by sessions")
        }
    }

    /// Records writes (as text) and closes (as `close:<code>`).
    struct Recorder {
        tx: Mutex<Sender<String>>,
    }

    impl Connection for Recorder {
        fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
            let _ = lock(&self.tx).send(String::from_utf8_lossy(msg).into_owned());
            Ok(())
        }
        fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
            self.write(msg, deadline)
        }
        fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn read(&self) -> Result<Vec<u8>, ConnectionError> {
            Err(ConnectionError("websocket: close 1000 (normal)".into()))
        }
        fn close(&self, code: i32, _reason: &str) {
            let _ = lock(&self.tx).send(format!("close:{code}"));
        }
    }

    fn session(clock: &TestClock, opts: Vec<SessionOption>) -> (Arc<Session>, Receiver<String>) {
        let (tx, rx) = channel();
        let s = Session::build(Arc::new(Recorder { tx: Mutex::new(tx) }), SessionEnv::new("/cable", None));
        write(&s.settings).clock = Arc::new(clock.clone());
        s.apply(opts);
        s.send_messages();
        (s, rx)
    }

    fn next(rx: &Receiver<String>) -> String {
        rx.recv_timeout(Duration::from_secs(5)).expect("a frame")
    }

    #[test]
    fn pings_on_schedule() {
        let clock = TestClock::new();
        let (s, rx) = session(&clock, vec![SessionOption::PingInterval(Duration::from_secs(3))]);
        s.start_timers();

        // The first ping comes within [1.5s, 4.5s), then every 3s.
        clock.advance(Duration::from_millis(4500));
        assert!(next(&rx).starts_with(r#"{"type":"ping","message":"#));
        clock.advance(Duration::from_secs(3));
        assert!(next(&rx).starts_with(r#"{"type":"ping","message":"#));
        assert_eq!(1, clock.pending(), "one timer armed for the next ping");
    }

    /// Deviation D-054: huge intervals are clamped.
    #[test]
    fn timer_intervals_are_capped() {
        let clock = TestClock::new();
        let huge = Duration::from_secs(u64::MAX);
        let (s, _rx) = session(
            &clock,
            vec![
                SessionOption::PingInterval(huge),
                SessionOption::PongTimeout(huge),
                SessionOption::KeepaliveIntervals { sessions_ttl: huge, presence_ttl: huge },
                SessionOption::WriteTimeout(huge),
            ],
        );
        s.set_connected(true);
        s.start_timers();
        s.schedule_ping();
        s.reset_pong();
        assert_eq!(MAX_TIMER_INTERVAL, s.settings().ping_interval);
        assert_eq!(1, clock.pending(), "one timer armed");
        assert!(!s.is_closed());
    }

    #[test]
    fn disconnects_without_pongs_and_keepalive_resets_the_deadline() {
        let clock = TestClock::new();
        let (s, rx) = session(
            &clock,
            vec![
                SessionOption::PingInterval(Duration::from_secs(100)),
                SessionOption::PongTimeout(Duration::from_secs(6)),
            ],
        );
        s.set_connected(true);
        s.start_timers();

        // A client message clears the pong deadline: no disconnect at the first one.
        clock.advance(Duration::from_secs(5));
        s.keepalive();
        clock.advance(Duration::from_secs(40));
        assert!(!s.is_closed());

        // After the first ping the pong deadline is set again and nothing arrives.
        clock.advance(Duration::from_secs(150));
        let mut frames = Vec::new();
        while let Ok(f) = rx.recv_timeout(Duration::from_secs(5)) {
            frames.push(f.clone());
            if f.starts_with("close:") {
                break;
            }
        }
        assert!(
            frames.contains(&r#"{"type":"disconnect","reason":"no_pong","reconnect":true}"#.to_owned()),
            "{frames:?}"
        );
        assert_eq!(Some(&format!("close:{CLOSE_NORMAL_CLOSURE}")), frames.last());
        assert!(s.is_closed());
        assert_eq!(0, clock.pending(), "a closed session arms no timer");
    }

    #[test]
    fn handshake_deadline_disconnects_idle_sessions() {
        let clock = TestClock::new();
        let deadline = clock.now() + Duration::from_secs(1);
        let (s, rx) = session(&clock, vec![SessionOption::HandshakeMessageDeadline(deadline)]);
        assert!(!s.authenticate_on_connect());
        s.start_timers();

        clock.advance(Duration::from_secs(1));
        assert_eq!(r#"{"type":"disconnect","reason":"idle_timeout","reconnect":false}"#, next(&rx));
        assert_eq!(format!("close:{CLOSE_NORMAL_CLOSURE}"), next(&rx));
        assert!(s.is_closed());
    }

    #[test]
    fn handshake_deadline_keeps_connected_sessions() {
        let clock = TestClock::new();
        let deadline = clock.now() + Duration::from_secs(1);
        let (s, rx) = session(&clock, vec![SessionOption::HandshakeMessageDeadline(deadline)]);
        s.set_connected(true);
        s.start_timers();

        clock.advance(Duration::from_secs(2));
        assert!(!s.is_closed());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn cache_entry_is_go_json() {
        let clock = TestClock::new();
        let (s, _rx) = session(&clock, vec![]);
        s.set_identifiers("user:1");
        s.subscriptions().add_channel("chat");
        s.subscriptions().add_channel_stream("chat", "s1");
        s.update_env(&mut |env| {
            env.merge_connection_state(&[("b".into(), "2".into()), ("a".into(), "1".into())].into())
        });
        let entry = String::from_utf8(s.to_cache_entry().unwrap()).unwrap();
        assert_eq!(
            r#"{"ids":"user:1","subs":{"chat":["s1"]},"cstate":{"a":"1","b":"2"},"istate":{},"Disconnectable":false}"#,
            entry
        );
    }

    #[test]
    fn cache_entry_fields_match_case_insensitively() {
        let clock = TestClock::new();
        let (s, _rx) = session(&clock, vec![]);
        s.restore_from_cache(br#"{"IDS":"x","Subs":{"c":null},"disconnectable":true,"cstate":null,"other":1}"#)
            .unwrap();
        assert_eq!("x", s.get_identifiers());
        assert_eq!(Some(Vec::<String>::new()), s.subscriptions().streams_for("c"));
        assert!(s.is_disconnectable());
        assert!(s.restore_from_cache(b"[1]").is_err());
    }

    /// Records the commands it executes.
    struct Commands(Mutex<Sender<String>>);

    impl Executor for Commands {
        fn handle_command(&self, _session: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
            let _ = lock(&self.0).send(msg.identifier.clone());
            Ok(())
        }
        fn disconnect(&self, _session: &Arc<Session>) -> Result<(), NodeError> {
            Ok(())
        }
    }

    /// Counts released read permits.
    struct Permit(Arc<AtomicU64>);

    impl Drop for Permit {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn reader_executes_messages_in_order_then_disconnects() {
        let clock = TestClock::new();
        let (s, frames) = session(&clock, vec![]);
        let (tx, commands) = channel();
        s.set_executor(Arc::new(Commands(Mutex::new(tx))));
        s.set_connected(true);
        let (done_tx, done) = channel();
        let reader = s.reader(Box::new(move || done_tx.send(()).unwrap()));
        let released = Arc::new(AtomicU64::new(0));

        for i in 0..50 {
            let msg = format!(r#"{{"command":"message","identifier":"{i}","data":"{{}}"}}"#);
            reader.message(Bytes::from(msg), Box::new(Permit(released.clone())));
        }
        reader.end(ConnectionError("websocket: close 1000 (normal)".into()));

        let got: Vec<String> = (0..50).map(|_| next(&commands)).collect();
        assert_eq!((0..50).map(|i| i.to_string()).collect::<Vec<_>>(), got);
        done.recv_timeout(Duration::from_secs(5)).expect("the callback runs when reading ends");
        assert_eq!(50, released.load(Ordering::SeqCst), "every permit is released after its message");
        assert!(s.is_closed());
        assert_eq!(format!("close:{CLOSE_NORMAL_CLOSURE}"), next(&frames));

        // Nothing is executed after the end.
        reader.message(Bytes::from_static(br#"{"command":"message","identifier":"late"}"#), Box::new(()));
        assert!(commands.recv_timeout(Duration::from_millis(100)).is_err());
    }

    /// Deviation D-074: after an undecodable message later ones are read and
    /// dropped, and the end of the connection ends the session.
    #[test]
    fn deviation_d074_reader_discards_after_an_undecodable_message() {
        let clock = TestClock::new();
        let (s, frames) = session(&clock, vec![]);
        let (tx, commands) = channel();
        s.set_executor(Arc::new(Commands(Mutex::new(tx))));
        s.set_connected(true);
        let (done_tx, done) = channel();
        let reader = s.reader(Box::new(move || done_tx.send(()).unwrap()));
        let released = Arc::new(AtomicU64::new(0));

        reader.message(Bytes::from_static(b"not json"), Box::new(Permit(released.clone())));
        reader.message(
            Bytes::from_static(br#"{"command":"message","identifier":"1"}"#),
            Box::new(Permit(released.clone())),
        );

        done.recv_timeout(Duration::from_secs(5)).expect("the callback runs (upstream's reader returns)");
        reader.message(Bytes::from_static(br#"{"command":"ping"}"#), Box::new(Permit(released.clone())));
        assert!(commands.recv_timeout(Duration::from_millis(100)).is_err(), "nothing is executed");
        let deadline = Instant::now() + Duration::from_secs(5);
        while released.load(Ordering::SeqCst) < 3 {
            assert!(Instant::now() < deadline, "the transport reads on");
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!s.is_closed(), "the session stays");

        reader.end(ConnectionError("websocket: close 1006 (abnormal closure): unexpected EOF".into()));
        assert!(next(&frames).starts_with("close:"), "the end of the connection ends the session");
        assert!(s.is_closed());
        assert!(!s.is_connected());
    }

    /// Deviation D-074: native pongs do not count after an undecodable message.
    #[test]
    fn deviation_d074_native_pongs_do_not_count_after_an_undecodable_message() {
        for (message, count) in [(&br#"{"command":"message","identifier":"1"}"#[..], true), (b"not json", false)] {
            let clock = TestClock::new();
            let opts = vec![
                SessionOption::PingInterval(Duration::from_secs(1)),
                SessionOption::PongTimeout(Duration::from_secs(4)),
            ];
            let (s, _frames) = session(&clock, opts);
            s.start_timers();
            let deadline = lock(&s.timers).pong.expect("a pong deadline");
            let reader = s.reader(Box::new(|| {}));
            let (released_tx, released) = channel();
            struct Signal(Sender<()>);
            impl Drop for Signal {
                fn drop(&mut self) {
                    let _ = self.0.send(());
                }
            }
            reader.message(Bytes::from_static(message), Box::new(Signal(released_tx)));
            released.recv_timeout(Duration::from_secs(5)).expect("the message is handled");

            s.handle_native_pong();
            assert_eq!(count, lock(&s.timers).pong.expect("a pong deadline") > deadline);
        }
    }

    /// A non-blocking transport that takes `capacity` frames, then refuses until
    /// released.
    #[derive(Default)]
    struct Valve {
        frames: Mutex<Vec<String>>,
        capacity: Mutex<usize>,
        refused: AtomicBool,
        refusals: AtomicU64,
        handler: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
        /// Runs once when the transport next refuses a frame, before the writer
        /// learns about it.
        on_refusal: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        /// The last write's.
        timeout: Mutex<Duration>,
    }

    impl Valve {
        fn frames(&self) -> Vec<String> {
            lock(&self.frames).clone()
        }

        fn is_refused(&self) -> bool {
            self.refused.load(Ordering::SeqCst)
        }

        fn refusals(&self) -> u64 {
            self.refusals.load(Ordering::SeqCst)
        }

        fn release(&self, n: usize) {
            *lock(&self.capacity) += n;
            if self.refused.swap(false, Ordering::SeqCst)
                && let Some(handler) = lock(&self.handler).as_ref()
            {
                handler();
            }
        }
    }

    impl Connection for Valve {
        fn write(&self, msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
            lock(&self.frames).push(String::from_utf8_lossy(msg).into_owned());
            Ok(())
        }
        fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
            self.write(msg, deadline)
        }
        fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn read(&self) -> Result<Vec<u8>, ConnectionError> {
            Err(ConnectionError("websocket: close 1000 (normal)".into()))
        }
        fn close(&self, code: i32, _reason: &str) {
            lock(&self.frames).push(format!("close:{code}"));
        }
        fn nonblocking(&self) -> bool {
            true
        }
        fn try_write_frame(&self, frame: &Arc<SentFrame>, timeout: Duration) -> Result<bool, ConnectionError> {
            *lock(&self.timeout) = timeout;
            let mut capacity = lock(&self.capacity);
            if *capacity == 0 {
                drop(capacity);
                self.refusals.fetch_add(1, Ordering::SeqCst);
                self.refused.store(true, Ordering::SeqCst);
                let on_refusal = lock(&self.on_refusal).take();
                if let Some(f) = on_refusal {
                    f();
                }
                return Ok(false);
            }
            *capacity -= 1;
            lock(&self.frames).push(String::from_utf8_lossy(&frame.payload).into_owned());
            Ok(true)
        }
        fn set_writable_handler(&self, handler: Box<dyn Fn() + Send + Sync>) {
            *lock(&self.handler) = Some(handler);
        }
    }

    #[test]
    fn nonblocking_transports_are_written_by_the_producer_and_resumed_when_writable() {
        let valve = Arc::new(Valve { capacity: Mutex::new(2), ..Default::default() });
        let s = Session::build(valve.clone(), SessionEnv::new("/cable", None));
        s.send_messages();
        s.set_connected(true);

        for i in 0..5 {
            s.send_json_transmission(&format!("{{\"n\":{i}}}"));
        }
        // Written synchronously while the transport takes frames; the rest waits.
        assert_eq!(vec![r#"{"n":0}"#, r#"{"n":1}"#], valve.frames());

        valve.release(10);
        assert_eq!(vec![r#"{"n":0}"#, r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#, r#"{"n":4}"#], valve.frames());

        // Close frames go through the pool drain.
        s.disconnect("bye", CLOSE_NORMAL_CLOSURE);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !valve.frames().last().is_some_and(|f| f.starts_with("close:")) {
            assert!(Instant::now() < deadline, "no close: {:?}", valve.frames());
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(Some(&format!("close:{CLOSE_NORMAL_CLOSURE}")), valve.frames().last());
    }

    /// Records the deadline of each write.
    #[derive(Default)]
    struct Deadlines(Mutex<Vec<Instant>>);

    impl Connection for Deadlines {
        fn write(&self, _msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
            lock(&self.0).push(deadline);
            Ok(())
        }
        fn write_binary(&self, msg: &[u8], deadline: Instant) -> Result<(), ConnectionError> {
            self.write(msg, deadline)
        }
        fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn read(&self) -> Result<Vec<u8>, ConnectionError> {
            Err(ConnectionError("websocket: close 1000 (normal)".into()))
        }
        fn close(&self, _code: i32, _reason: &str) {}
    }

    /// `Connection::try_write_frame`, as transports that do not override it have it.
    #[test]
    fn the_default_nonblocking_write_honours_the_timeout() {
        let conn = Deadlines::default();
        let frame = Arc::new(SentFrame::text(b"x".to_vec()));
        for timeout in [Duration::ZERO, Duration::from_secs(7), MAX_TIMER_INTERVAL] {
            let before = Instant::now();
            assert_eq!(Ok(true), conn.try_write_frame(&frame, timeout));
            let after = Instant::now();
            let deadline = lock(&conn.0).pop().unwrap();
            assert!(before + timeout <= deadline && deadline <= after + timeout, "{timeout:?}");
        }
        // Too long for the clock: no overflow, and no early deadline either.
        assert_eq!(Ok(true), conn.try_write_frame(&frame, Duration::MAX));
        assert!(lock(&conn.0).pop().unwrap() > Instant::now() + MAX_TIMER_INTERVAL);
    }

    #[test]
    fn nonblocking_transports_get_the_write_timeout() {
        let valve = Arc::new(Valve { capacity: Mutex::new(3), ..Default::default() });
        let s = Session::build(valve.clone(), SessionEnv::new("/cable", None));
        s.send_json_transmission("queued before the writer is attached");
        s.send_messages();
        assert_eq!(DEFAULT_WRITE_TIMEOUT, *lock(&valve.timeout));
        s.apply(vec![SessionOption::WriteTimeout(Duration::from_secs(7))]);
        s.send_json_transmission("written straight");
        assert_eq!(Duration::from_secs(7), *lock(&valve.timeout));
        s.apply(vec![SessionOption::WriteTimeout(Duration::MAX)]);
        s.send_json_transmission("clamped");
        assert_eq!(MAX_TIMER_INTERVAL, *lock(&valve.timeout));
    }

    #[test]
    fn nonblocking_fast_path_preserves_order_and_metrics_under_concurrent_backpressure() {
        let metrics = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
        metrics.register_counter(METRICS_SENT_MSG, "sent");
        metrics.register_counter(METRICS_DATA_SENT, "bytes");

        let valve = Arc::new(Valve { capacity: Mutex::new(1), ..Default::default() });
        let s = Session::build(valve.clone(), SessionEnv::new("/cable", None));
        s.apply(vec![SessionOption::Metrics(metrics.clone())]);
        s.send_messages();
        s.set_connected(true);

        const THREADS: usize = 4;
        const PER_THREAD: usize = 50;
        const TOTAL: usize = THREADS * PER_THREAD;
        let expected_bytes: u64 =
            (0..THREADS).flat_map(|t| (0..PER_THREAD).map(move |i| format!("{t}:{i}").len() as u64)).sum();

        let releaser = {
            let valve = valve.clone();
            std::thread::spawn(move || {
                let mut n = 1usize;
                let deadline = Instant::now() + Duration::from_secs(5);
                while valve.frames().len() < TOTAL && Instant::now() < deadline {
                    if valve.is_refused() {
                        valve.release(n);
                        n = (n % 3) + 1;
                    } else {
                        std::thread::yield_now();
                    }
                }
            })
        };

        let producers: Vec<_> = (0..THREADS)
            .map(|t| {
                let s = s.clone();
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        s.send_json_transmission(&format!("{t}:{i}"));
                    }
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        releaser.join().unwrap();

        let frames = valve.frames();
        assert!(valve.refusals() > 0, "transport must refuse frames to exercise backpressure/resume");
        assert_eq!(TOTAL, frames.len(), "writer stalled after {} refusals", valve.refusals());
        let mut last = [None::<usize>; THREADS];
        for f in &frames {
            let (t, i) = f.split_once(':').unwrap();
            let (t, i): (usize, usize) = (t.parse().unwrap(), i.parse().unwrap());
            assert_eq!(last[t].map_or(0, |prev| prev + 1), i, "out-of-order frame for producer {t}: {frames:?}");
            last[t] = Some(i);
        }
        assert_eq!(last, [Some(PER_THREAD - 1); THREADS]);
        assert_eq!(TOTAL as u64, metrics.counter(METRICS_SENT_MSG).unwrap().value());
        assert_eq!(expected_bytes, metrics.counter(METRICS_DATA_SENT).unwrap().value());
    }

    /// The interleaving in which a parking thread that lost the writer must not
    /// consume a wakeup meant for its current owner (`load`, never `swap`, in
    /// [`Session::park_refused`]): with `swap` the second frame stays parked
    /// although the transport is writable.
    #[test]
    fn a_parking_thread_that_lost_the_writer_leaves_the_wakeup_to_its_owner() {
        let valve = Arc::new(Valve::default());
        let s = Session::build(valve.clone(), SessionEnv::new("/cable", None));
        s.send_messages();
        s.set_connected(true);

        let (refused_tx, refused) = channel::<()>();
        let (resume_tx, resume) = channel::<()>();
        let (producer_tx, producer) = channel();
        // This thread (T) is paused after it parked "a"; everything below happens
        // before it checks the flag.
        let (valve2, s2, s3) = (valve.clone(), s.clone(), s.clone());
        PARK_PAUSE.set(Some(Box::new(move || {
            // The io loop signals writable: its handler takes the writer over, sends
            // "a" and goes idle.
            valve2.release(1);
            assert_eq!(vec!["a"], valve2.frames());

            // Another producer (P) takes the writer and is refused; it is held
            // before it can park "b".
            *lock(&valve2.on_refusal) = Some(Box::new(move || {
                refused_tx.send(()).unwrap();
                resume.recv_timeout(Duration::from_secs(5)).unwrap();
            }));
            let p = std::thread::spawn(move || s2.send_json_transmission("b"));
            refused.recv_timeout(Duration::from_secs(5)).expect("P is refused");

            // The io loop signals writable again. P still owns the writer, so the
            // handler only sets the flag.
            valve2.release(1);
            assert_eq!(WRITER_BUSY, s3.writer.load(Ordering::SeqCst));
            producer_tx.send(p).unwrap();
        })));
        s.send_json_transmission("a");

        // T resumed, looked at the flag and lost the race. Now P parks "b": it must
        // find the flag and write it, as no other signal is coming.
        resume_tx.send(()).unwrap();
        producer.recv_timeout(Duration::from_secs(5)).unwrap().join().unwrap();
        assert_eq!(vec!["a", "b"], valve.frames(), "the wakeup was consumed by the thread that lost the writer");
        assert_eq!(WRITER_IDLE, s.writer.load(Ordering::SeqCst));
    }

    #[test]
    fn nonblocking_slow_client_overflows_while_waiting_for_writable() {
        let metrics = Arc::new(crate::metrics::Metrics::new(Vec::new(), 0));
        metrics.register_counter(METRICS_DISCONNECTED_SLOW_CLIENTS, "slow");

        let valve = Arc::new(Valve { capacity: Mutex::new(1), ..Default::default() });
        let s = Session::build(valve.clone(), SessionEnv::new("/cable", None));
        s.apply(vec![SessionOption::Metrics(metrics.clone()), SessionOption::MaxPendingSize(20)]);
        s.send_messages();
        s.set_connected(true);

        // First frame passes straight through; second parks in `refused`;
        // subsequent frames fill `write_queue` past 20 bytes and trigger overflow.
        for _ in 0..5 {
            s.send_json_transmission("0123456789");
        }
        assert!(s.is_closed(), "slow client must be closed on pending overflow");
        assert_eq!(1, metrics.counter(METRICS_DISCONNECTED_SLOW_CLIENTS).unwrap().value());
    }

    #[test]
    fn close_errors() {
        assert!(ConnectionError("websocket: close 1000 (normal)".into()).is_close());
        assert!(ConnectionError("websocket: close 1001 (going away)".into()).is_close());
        assert!(ConnectionError("websocket: close 1005 (no status)".into()).is_close());
        assert!(!ConnectionError("websocket: close 1006 (abnormal closure)".into()).is_close());
        assert!(ConnectionError("write tcp: i/o timeout".into()).is_timeout());
    }
}
