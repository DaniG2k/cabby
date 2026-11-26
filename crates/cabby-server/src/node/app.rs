//! The node itself (upstream `node/node.go` and `node/controller.go`) and its
//! time abstraction.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cabby_core::broker::{Broker, BrokerError, Cacheable, PresenceInfoOptions, Presenter};
use cabby_core::common::{
    CommandResult, ConnectResult, ControllerError, DisconnectMessage, ERROR_TYPE, HISTORY_CONFIRMED_TYPE,
    HISTORY_REJECTED_TYPE, HistoryRequest, Message, MessageData, Outcome, PRESENCE_JOIN_TYPE, PRESENCE_LEAVE_TYPE,
    PRESENCE_STREAM_STATE, PRESENCE_TYPE, PresenceEvent, PresenceInfo, PubSubMessage, RemoteCommandMessage,
    RemoteDisconnectMessage, Reply, SERVER_RESTART_REASON, SessionEnv, Status, StreamMessage, StreamMessageMetadata,
    StringMap, UNAUTHORIZED_REASON, WELCOME_TYPE, WHISPER_STREAM_STATE, disconnection_message, filtered_identifier,
    pub_sub_message_from_json, rejection_message,
};
use cabby_core::encoders::{EncodedMessage, EncodedMessageJson};
use cabby_core::logger::{LogValue, LogValuer, Logger, compact_value_bytes, error_value, filtered_map, filtered_url};
use cabby_core::pubsub::Activations;
use cabby_core::utils::GoPool;
use cabby_core::{log_debug, log_error, log_info, log_warn};

use super::call_context::with_close_context;
use super::disconnect_queue::context_done;
use super::revocations::Revocations;
use super::session::{decode_cache_entry, go_value_text};
use super::*;
use crate::cli::SubscriberHandler;
use crate::metrics::{Instrumenter, NoopMetrics};
use crate::rpc::Context;

// --- Time -------------------------------------------------------------------

/// A scheduled callback (upstream `*time.Timer`).
pub trait Timer: Send + Sync {
    /// Cancels the callback; `false` if it already ran or was stopped.
    fn stop(&self) -> bool;
}

/// A periodic tick source shared by several receivers (upstream `*time.Ticker`:
/// one tick is buffered, further ticks are dropped while nobody receives).
pub trait Ticker: Send + Sync {
    /// Blocks until the next tick.
    fn recv(&self);
    fn stop(&self);
}

/// Time source of sessions and the disconnect queue (upstream reads `time.Now`
/// and uses `time.AfterFunc` / `time.NewTicker`).
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
    /// Runs `f` once after `delay` unless the returned timer is stopped first.
    fn after(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer>;
    fn ticker(&self, period: Duration) -> Arc<dyn Ticker>;
    /// Wall-clock time in microseconds since the Unix epoch: when sessions are
    /// authenticated and remote disconnects arrive (D-084). Defaults to the
    /// system clock.
    fn unix_micros(&self) -> i64 {
        system_unix_micros()
    }
}

fn system_unix_micros() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
}

/// The operating system clock: one timer thread for the whole process, fired
/// callbacks on a worker pool (see the `clock` module).
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn after(&self, delay: Duration, f: Box<dyn FnOnce() + Send>) -> Box<dyn Timer> {
        super::clock::after(delay, f)
    }
    fn ticker(&self, period: Duration) -> Arc<dyn Ticker> {
        super::clock::ticker(period)
    }
}

// --- Node -------------------------------------------------------------------

/// Node construction options (upstream `NodeOption` functions `With*`).
pub enum NodeOption {
    /// `WithController`.
    Controller(Arc<dyn Controller>),
    /// `WithInstrumenter`.
    Instrumenter(Arc<dyn Instrumenter>),
    /// `WithID`.
    Id(String),
    /// The time source for sessions and the disconnect queue (default: [`SystemClock`]).
    Clock(Arc<dyn Clock>),
}

/// Authentication options (upstream `AuthOption` functions).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthOption {
    /// `WithDisconnectOnFailure` (default `true`).
    DisconnectOnFailure(bool),
}

// Process-wide gauges upstream registers alongside the node's (Go runtime stats).
const METRICS_GOROUTINES: &str = "goroutines_num";
const METRICS_MEM_SYS: &str = "mem_sys_bytes";

/// Upstream nodes start without a broker (a nil field) until `SetBroker`; this
/// stands in for it and does nothing.
struct UnsetBroker;

impl Broker for UnsetBroker {
    fn start(&self) -> Result<(), BrokerError> {
        Ok(())
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        Ok(())
    }
    fn announce(&self) -> String {
        String::new()
    }
    fn handle_broadcast(&self, _msg: &StreamMessage) -> Result<(), BrokerError> {
        Ok(())
    }
    fn handle_command(&self, _msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        Ok(())
    }
    fn subscribe(&self, stream: &str) -> String {
        stream.to_owned()
    }
    fn unsubscribe(&self, stream: &str) -> String {
        stream.to_owned()
    }
    fn history_from(&self, _stream: &str, _epoch: &str, _offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        Ok(Vec::new())
    }
    fn history_since(&self, _stream: &str, _ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        Ok(None)
    }
    fn commit_session(&self, _sid: &str, _session: &dyn Cacheable) -> Result<(), BrokerError> {
        Ok(())
    }
    fn restore_session(&self, _sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        Ok(None)
    }
    fn touch_session(&self, _sid: &str) -> Result<(), BrokerError> {
        Ok(())
    }
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
        Ok(None)
    }
    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        Ok(None)
    }
    fn presence_info(&self, _stream: &str, _opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        Ok(PresenceInfo::default())
    }
    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        Ok(())
    }
    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        Ok(None)
    }
}

fn read<T: Clone>(l: &RwLock<T>) -> T {
    l.read().unwrap_or_else(PoisonError::into_inner).clone()
}

fn replace<T>(l: &RwLock<T>, v: T) {
    *l.write().unwrap_or_else(PoisonError::into_inner) = v;
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn broker_error(e: BrokerError) -> NodeError {
    NodeError(e.message)
}

/// `errorx.Decorate(err, msg)`.
fn decorate(err: &ControllerError, msg: String) -> NodeError {
    NodeError(format!("{msg}, cause: {err}"))
}

fn filtered(identifier: &str) -> String {
    filtered_identifier(identifier).filtered()
}

/// `utils.ToJSON(msg.Data)`.
fn data_json(data: &Option<MessageData>) -> String {
    match data {
        Some(MessageData::Json(v)) => cabby_core::gojson::to_string(v),
        Some(MessageData::Typed(_)) | None => "null".to_owned(),
    }
}

/// Go's `%v` of a message's `Data`.
fn data_text(data: &Option<MessageData>) -> String {
    match data {
        Some(MessageData::Json(v)) => go_value_text(v),
        Some(MessageData::Typed(_)) => "<typed>".to_owned(),
        None => "<nil>".to_owned(),
    }
}

/// Go's `slog.Any("response", res)` for a nil-able result pointer (its
/// `LogValue` reads `nil` for a nil receiver).
fn result_value<T: LogValuer>(res: Option<&T>) -> LogValue {
    res.map_or_else(|| LogValue::String("nil".to_owned()), LogValuer::log_value)
}

/// Upstream logs `&v` of the decoded batch (`*[]*StreamMessage`), which slog
/// prints with `%+v` as element addresses; Cabby lists the messages instead.
fn batch_log_value(batch: &[StreamMessage]) -> LogValue {
    LogValue::List(batch.iter().map(LogValuer::log_value).collect())
}

/// The session's request headers as `logger.FilteredMap` takes them (`None`:
/// a session without headers).
fn sorted_headers(env: &SessionEnv) -> Option<std::collections::BTreeMap<String, String>> {
    env.headers.as_ref().map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// A `presence` reply (upstream `&common.Reply{Type: PresenceType, Identifier,
/// Message: info}`). A dedicated message rather than a [`Reply`], whose message
/// is a JSON value with sorted keys: the JSON encoder writes the info in Go's
/// struct order (`type`, `total`, `records`), as upstream does.
struct PresenceReply<'a> {
    identifier: &'a str,
    info: &'a PresenceInfo,
}

impl serde::Serialize for PresenceReply<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Reply", 3)?;
        st.serialize_field("type", PRESENCE_TYPE)?;
        if self.identifier.is_empty() {
            st.skip_field("identifier")?;
        } else {
            st.serialize_field("identifier", self.identifier)?;
        }
        st.serialize_field("message", self.info)?;
        st.end()
    }
}

impl EncodedMessage for PresenceReply<'_> {
    fn get_type(&self) -> String {
        PRESENCE_TYPE.to_owned()
    }
}

impl EncodedMessageJson for PresenceReply<'_> {
    fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("presence reply JSON")
    }

    fn write_json(&self, out: &mut Vec<u8>) {
        cabby_core::gojson::write(out, self);
    }
}

#[derive(Default)]
struct Lifecycle {
    closed: bool,
}

/// The whole application: hub, broker, controller and disconnector (upstream
/// `node.Node`).
pub struct Node {
    id: String,
    /// Upstream `n.log`: the runner's logger with `context=node`.
    log: Logger,
    config: Config,
    metrics: Arc<dyn Instrumenter>,
    clock: Arc<dyn Clock>,
    hub: Hub,
    broker: RwLock<Arc<dyn Broker>>,
    controller: RwLock<Arc<dyn Controller>>,
    disconnector: RwLock<Option<Arc<dyn Disconnector>>>,
    lifecycle: Mutex<Lifecycle>,
    lifecycle_changed: Condvar,
    /// `$pusher` is routed to the Pusher controller: subscriptions to it are
    /// refused for sessions outside the Pusher endpoint.
    pusher_channel_reserved: AtomicBool,
    /// Pub/sub subscriptions not active yet (D-105).
    activations: Arc<Activations>,
    /// Identifiers disconnected remotely: their earlier sessions are not
    /// restored or written to the cache (D-084).
    revocations: Revocations,
    /// How long cache entries live (milliseconds; see [`Node::set_sessions_ttl`]).
    sessions_ttl_ms: AtomicU64,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node").field("id", &self.id).finish_non_exhaustive()
    }
}

impl Node {
    /// Upstream `NewNode(config, opts...)`: builds the hub and registers metrics.
    pub fn new(config: Config, opts: Vec<NodeOption>) -> Arc<Node> {
        let mut id = String::new();
        let mut controller: Arc<dyn Controller> = Arc::new(NullController);
        let mut metrics: Option<Arc<dyn Instrumenter>> = None;
        let mut clock: Arc<dyn Clock> = Arc::new(SystemClock);
        for opt in opts {
            match opt {
                NodeOption::Controller(c) => controller = c,
                NodeOption::Instrumenter(i) => metrics = Some(i),
                NodeOption::Id(v) => id = v,
                NodeOption::Clock(c) => clock = c,
            }
        }

        let log = Logger::default().with("context", "node");
        let hub = Hub::with_logger(config.hub_gopool_size.max(0) as usize, &log);

        let registered = metrics.is_some();
        let node = Arc::new(Node {
            id,
            log,
            config,
            metrics: metrics.unwrap_or_else(|| Arc::new(NoopMetrics)),
            clock,
            hub,
            broker: RwLock::new(Arc::new(UnsetBroker)),
            controller: RwLock::new(controller),
            disconnector: RwLock::new(None),
            lifecycle: Mutex::new(Lifecycle::default()),
            lifecycle_changed: Condvar::new(),
            pusher_channel_reserved: AtomicBool::new(false),
            activations: Arc::new(Activations::new()),
            revocations: Revocations::new(),
            sessions_ttl_ms: AtomicU64::new(
                u64::try_from(cabby_core::broker::Config::new().sessions_ttl).unwrap_or(0).saturating_mul(1000),
            ),
        });

        if registered {
            node.register_metrics();
        }
        node
    }

    /// Starts the hub and the stats collector (upstream `Start`).
    pub fn start(self: &Arc<Self>) -> Result<(), NodeError> {
        let node = self.clone();
        std::thread::Builder::new()
            .name("cabby-hub".into())
            .spawn(move || node.hub.run())
            .map_err(|e| NodeError(e.to_string()))?;

        if self.config.stats_refresh_interval > 0 {
            let node = Arc::downgrade(self);
            let interval = Duration::from_secs(self.config.stats_refresh_interval as u64);
            std::thread::Builder::new()
                .name("cabby-node-stats".into())
                .spawn(move || Self::collect_stats(&node, interval))
                .map_err(|e| NodeError(e.to_string()))?;
        }

        Ok(())
    }

    /// Refreshes the gauges every `interval` until shutdown (upstream unexported
    /// `collectStats`).
    fn collect_stats(node: &Weak<Node>, interval: Duration) {
        loop {
            let Some(n) = node.upgrade() else { return };
            let deadline = Instant::now() + interval;
            {
                let mut life = lock(&n.lifecycle);
                loop {
                    if life.closed {
                        return;
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    life = n
                        .lifecycle_changed
                        .wait_timeout(life, deadline - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            }
            n.collect_stats_once();
        }
    }

    fn collect_stats_once(&self) {
        let (threads, memory) = process_stats();
        self.metrics.gauge_set(METRICS_GOROUTINES, threads);
        self.metrics.gauge_set(METRICS_MEM_SYS, memory);
        self.metrics.gauge_set(METRICS_CLIENTS_NUM, self.hub.size() as u64);
        self.metrics.gauge_set(METRICS_UNIQ_CLIENTS_NUM, self.hub.uniq_size() as u64);
        self.metrics.gauge_set(METRICS_STREAMS_NUM, self.hub.streams_size() as u64);
        let queued = read(&self.disconnector).map_or(0, |d| d.size());
        self.metrics.gauge_set(METRICS_DISCONNECT_QUEUE, queued as u64);
    }

    fn register_metrics(&self) {
        let m = &self.metrics;
        m.register_gauge(METRICS_GOROUTINES, "The number of Go routines");
        m.register_gauge(METRICS_MEM_SYS, "The total bytes of memory obtained from the OS");

        m.register_gauge(METRICS_CLIENTS_NUM, "The number of active clients");
        m.register_gauge(
            METRICS_UNIQ_CLIENTS_NUM,
            "The number of unique clients (with respect to connection identifiers)",
        );
        m.register_gauge(METRICS_STREAMS_NUM, "The number of active broadcasting streams");
        m.register_gauge(METRICS_DISCONNECT_QUEUE, "The size of delayed disconnect");

        m.register_counter(METRICS_FAILED_AUTHS, "The total number of failed authentication attempts");
        m.register_counter(
            METRICS_REJECTED_CLIENTS,
            "The total number of client connections rejected because the server was overloaded",
        );
        m.register_counter(METRICS_RECEIVED_MSG, "The total number of received messages from clients");
        m.register_counter(METRICS_DISCONNECTED_SLOW_CLIENTS, "The total number of disconnected slow clients");
        m.register_counter(
            METRICS_DISCONNECT_DISCARDED,
            "The total number of Disconnect calls discarded because the disconnect queue was full",
        );
        m.register_counter(
            METRICS_FAILED_COMMAND_RECEIVED,
            "The total number of unrecognized messages received from clients",
        );
        m.register_counter(METRICS_PUBLICATIONS, "The total number of publications received (from the application)");
        m.register_counter(
            METRICS_BROADCAST_MSG,
            "The total number of messages received through PubSub (for broadcast)",
        );
        m.register_counter(METRICS_REMOTE_COMMANDS, "The total number of remote commands received through PubSub");
        m.register_counter(
            METRICS_UNKNOWN_BROADCAST,
            "The total number of unrecognized publications received from the application",
        );

        m.register_counter(METRICS_SENT_MSG, "The total number of messages sent to clients");
        m.register_counter(METRICS_FAILED_SENT, "The total number of messages failed to send to clients");
        m.register_counter(
            METRICS_WRITE_TIMEOUT,
            "The total number of messages failed to send to clients due to write timeouts",
        );

        m.register_counter(METRICS_DATA_SENT, "The total amount of bytes sent to clients");
        m.register_counter(METRICS_DATA_RECEIVED, "The total amount of bytes received from clients");
    }

    /// Disconnects active sessions, then stops the hub, the disconnector and the
    /// controller (upstream `Shutdown(ctx)`); fails when called twice.
    pub fn shutdown(&self, ctx: &Context) -> Result<(), NodeError> {
        {
            let mut life = lock(&self.lifecycle);
            if life.closed {
                return Err(NodeError("already shut down".into()));
            }
            life.closed = true;
        }
        self.lifecycle_changed.notify_all();

        let active = self.hub.size();
        if active > 0 {
            log_info!(self.log, "closing active connections", "num" => active);
            self.disconnect_all(ctx);
        }
        self.hub.shutdown();

        if let Some(d) = read(&self.disconnector)
            && let Err(e) = d.shutdown(ctx)
        {
            log_warn!(self.log, "failed to shutdown disconnector gracefully", "error" => e.0);
        }

        if let Err(e) = self.controller().shutdown() {
            log_warn!(self.log, "failed to shutdown controller gracefully", "error" => e.to_string());
        }

        Ok(())
    }

    /// Sends every connected session a `server_restart` disconnect, in parallel
    /// on a pool of `shutdown_disconnect_pool_size` workers, until all are done or
    /// `ctx` ends (upstream unexported `disconnectAll`).
    fn disconnect_all(&self, ctx: &Context) {
        /// Counts a session done when dropped, also when its disconnect panics
        /// (the pool catches the panic): the wait below must not last until
        /// the deadline for it.
        struct Done(Arc<(Mutex<usize>, Condvar)>);
        impl Drop for Done {
            fn drop(&mut self) {
                let (count, done) = &*self.0;
                *lock(count) -= 1;
                done.notify_all();
            }
        }

        let sessions = self.hub.sessions();
        let pending = Arc::new((Mutex::new(sessions.len()), Condvar::new()));
        let pool = Arc::new(GoPool::new("disconnect", self.config.shutdown_disconnect_pool_size.max(1) as usize));

        // Scheduled from a separate thread so a saturated pool never blocks the wait.
        {
            let (pool, pending, ctx) = (pool.clone(), pending.clone(), ctx.clone());
            std::thread::spawn(move || {
                for s in sessions {
                    let (pending, ctx) = (pending.clone(), ctx.clone());
                    pool.schedule(Box::new(move || {
                        let _done = Done(pending);
                        if !context_done(&ctx) && s.is_connected() {
                            s.disconnect_with_message(
                                &DisconnectMessage::new(SERVER_RESTART_REASON, true),
                                SERVER_RESTART_REASON,
                            );
                        }
                    }));
                }
            });
        }

        // The context has no blocking notification, so the wait re-checks it at
        // a short interval (only while sessions are still being disconnected).
        const CONTEXT_CHECK: Duration = Duration::from_millis(20);
        let (count, done) = &*pending;
        let mut left = lock(count);
        while *left > 0 && !context_done(ctx) {
            left = done.wait_timeout(left, CONTEXT_CHECK).unwrap_or_else(PoisonError::into_inner).0;
        }
        let finished = *left == 0;
        drop(left);
        if finished {
            log_info!(self.log, "all active connections closed");
        } else {
            log_warn!(self.log, "terminated while disconnecting active sessions", "num" => self.hub.size());
        }
    }

    /// The node's logger (upstream unexported field `log`).
    pub fn logger(&self) -> Logger {
        self.log.clone()
    }

    pub fn id(&self) -> String {
        self.id.clone()
    }

    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    fn controller(&self) -> Arc<dyn Controller> {
        read(&self.controller)
    }

    pub fn set_disconnector(&self, d: Arc<dyn Disconnector>) {
        replace(&self.disconnector, Some(d));
    }

    pub fn set_broker(&self, b: Arc<dyn Broker>) {
        replace(&self.broker, b);
    }

    /// Replaces the controller (upstream tests assign the unexported `controller` field).
    pub fn set_controller(&self, c: Arc<dyn Controller>) {
        replace(&self.controller, c);
    }

    pub fn instrumenter(&self) -> Arc<dyn Instrumenter> {
        self.metrics.clone()
    }

    /// The node's broker (upstream unexported field `broker`).
    pub fn broker(&self) -> Arc<dyn Broker> {
        read(&self.broker)
    }

    /// The node's hub (upstream unexported field `hub`).
    pub fn hub(&self) -> &Hub {
        &self.hub
    }

    /// The node's disconnector (upstream unexported field `disconnector`); a
    /// no-op one until [`Node::set_disconnector`].
    pub fn disconnector(&self) -> Arc<dyn Disconnector> {
        read(&self.disconnector).unwrap_or_else(|| Arc::new(NoopDisconnectQueue))
    }

    pub fn is_shutting_down(&self) -> bool {
        lock(&self.lifecycle).closed
    }

    pub fn admit_new_client(&self) -> bool {
        true
    }

    /// Parses an application broadcast and passes it to the broker (upstream
    /// `HandleBroadcast`). Unparseable payloads are counted and ignored.
    pub fn handle_broadcast(&self, raw: &[u8]) -> Result<(), NodeError> {
        let msg = match pub_sub_message_from_json(raw) {
            Ok(msg) => msg,
            Err(e) => {
                self.metrics.counter_increment(METRICS_UNKNOWN_BROADCAST);
                log_warn!(self.log, "failed to parse pubsub message",
                    "data" => compact_value_bytes(raw).log_value(), "error" => e.to_string());
                return Ok(());
            }
        };

        self.metrics.counter_increment(METRICS_PUBLICATIONS);

        let broker = self.broker();
        match msg {
            PubSubMessage::Stream(m) => {
                log_debug!(self.log, "handle broadcast message", "payload" => m.log_value());
                broker.handle_broadcast(&m).map_err(broker_error)
            }
            PubSubMessage::Batch(batch) => {
                log_debug!(self.log, "handle batch-broadcast message", "payload" => batch_log_value(&batch));
                for el in &batch {
                    if let Err(e) = broker.handle_broadcast(el) {
                        return Err(NodeError(format!("failed to broadcast batch, cause: {}", e.message)));
                    }
                }
                Ok(())
            }
            PubSubMessage::RemoteCommand(c) => {
                log_debug!(self.log, "handle remote command", "command" => c.log_value());
                broker.handle_command(&c).map_err(broker_error)
            }
        }
    }

    /// Parses a pub/sub message and delivers it locally, bypassing the broker
    /// (upstream `HandlePubSub`).
    pub fn handle_pub_sub(&self, raw: &[u8]) {
        match pub_sub_message_from_json(raw) {
            Err(e) => {
                self.metrics.counter_increment(METRICS_UNKNOWN_BROADCAST);
                log_warn!(self.log, "failed to parse pubsub message",
                    "data" => compact_value_bytes(raw).log_value(), "error" => e.to_string());
            }
            Ok(PubSubMessage::Stream(m)) => self.broadcast(&m),
            Ok(PubSubMessage::Batch(batch)) => batch.iter().for_each(|m| self.broadcast(m)),
            Ok(PubSubMessage::RemoteCommand(c)) => self.execute_remote_command(&c),
        }
    }

    /// How long the broker keeps session cache entries (its `sessions_ttl`;
    /// default: the broker default, 5 minutes). A remote disconnect makes the
    /// sessions cached before it unusable for this long (D-084).
    pub fn set_sessions_ttl(&self, ttl: Duration) {
        self.sessions_ttl_ms.store(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    fn sessions_ttl(&self) -> Duration {
        Duration::from_millis(self.sessions_ttl_ms.load(Ordering::Relaxed))
    }

    pub fn lookup_session(&self, identifiers: &str) -> Option<Arc<Session>> {
        self.hub.find_by_identifier(identifiers)
    }

    /// Runs `write` (a commit or touch of the session's cache entry) only for
    /// sessions that the application authenticated, or that were restored
    /// (D-083; Go writes the session after every authentication result). A
    /// session authenticated before a remote disconnect of its identifiers
    /// leaves its entry as it is (D-084).
    pub(super) fn write_session_cache(&self, s: &Session, write: impl FnOnce()) {
        if !s.is_resumeable() || !s.is_authenticated() {
            return;
        }
        let (identifiers, authenticated_at) = (s.get_identifiers(), s.authenticated_at());
        if self.revocations.refuses(&identifiers, authenticated_at, self.clock.now()) {
            return;
        }
        write();
        // A remote disconnect recorded meanwhile outlives the entry just written.
        self.revocations.extend(&identifiers, authenticated_at, self.clock.now(), self.sessions_ttl());
    }

    fn commit_session(&self, s: &Arc<Session>) {
        self.write_session_cache(s, || {
            if let Err(e) = self.broker().commit_session(&s.get_id(), s.as_ref()) {
                log_error!(s.logger(), "failed to persist session in cache", "error" => e.message);
            }
        });
    }

    /// Restores the session from the broker cache or calls the controller; on
    /// success registers the session with the hub (upstream `Authenticate`).
    /// `Ok(None)` is Go's `(nil, nil)` (e.g. the session closed meanwhile).
    pub fn authenticate(&self, s: &Arc<Session>, opts: &[AuthOption]) -> Result<Option<ConnectResult>, NodeError> {
        let disconnect_on_failure = opts.iter().fold(true, |_, o| match o {
            AuthOption::DisconnectOnFailure(v) => *v,
        });

        if s.is_resumeable() {
            if self.try_restore_session(s) {
                return Ok(Some(ConnectResult { status: Status::Success, ..Default::default() }));
            }
            if s.is_closed() {
                return Ok(None);
            }
        }

        // A session counts as authenticated when its authentication started
        // (D-084: a remote disconnect meanwhile makes it an earlier session).
        let started = self.clock.unix_micros();
        let env = s.get_env();
        let Outcome { result, error } =
            with_close_context(s.close_context(), || self.controller().authenticate(&s.get_id(), Some(&env)));

        if s.is_closed() {
            log_debug!(s.logger(), "skip authenticate result: closed");
            return Ok(None);
        }

        if result.is_none() && error.is_none() {
            return Ok(None);
        }

        log_debug!(s.logger(), "controller authenticate",
            "response" => result_value(result.as_ref()), "err" => error_value(error.as_ref()));

        if let Some(err) = error {
            s.disconnect("Auth Error", CLOSE_INTERNAL_SERVER_ERR);
            return Err(decorate(&err, "failed to authenticate".into()));
        }
        let Some(res) = result else { return Ok(None) };

        let mut disconnect_after = false;
        if res.status == Status::Success {
            self.authenticated_since(s, &res.identifier, started);
        } else {
            if res.status == Status::Failure {
                self.metrics.counter_increment(METRICS_FAILED_AUTHS);
            }
            disconnect_after = disconnect_on_failure;
        }

        self.handle_call_reply(s, res.cstate.as_ref(), &res.broadcasts, &res.transmissions);
        self.mark_disconnectable(s, res.disconnect_interest);
        self.commit_session(s);

        if disconnect_after {
            s.disconnect("Auth Failed", CLOSE_NORMAL_CLOSURE);
        }

        Ok(Some(res))
    }

    /// Marks the session authenticated and registers it with the hub unless it
    /// is already closed (upstream `Authenticated`).
    pub fn authenticated(&self, s: &Arc<Session>, identifiers: &str) {
        self.authenticated_since(s, identifiers, self.clock.unix_micros());
    }

    fn authenticated_since(&self, s: &Arc<Session>, identifiers: &str, at: i64) {
        let _reg = s.registration();
        if s.is_closed() {
            return;
        }
        s.set_identifiers(identifiers);
        s.mark_authenticated(Some(at));
        if !s.is_inactive() {
            self.hub.add_session(s.clone());
        }
        s.set_connected(true);
    }

    pub fn try_restore_session(&self, s: &Arc<Session>) -> bool {
        let sid = s.get_id();
        let prev_sid = s.prev_sid();
        if prev_sid.is_empty() {
            return false;
        }

        let broker = self.broker();
        let cached = match broker.restore_session(&prev_sid) {
            Ok(Some(cached)) => cached,
            Ok(None) => {
                log_debug!(s.logger(), "session not found in cache", "old_sid" => &prev_sid);
                return false;
            }
            Err(e) => {
                log_error!(s.logger(), "failed to fetch session cache", "old_sid" => &prev_sid, "error" => e.message);
                return false;
            }
        };

        if s.is_closed() {
            log_debug!(s.logger(), "skip restore result: closed");
            return false;
        }

        let entry = match decode_cache_entry(&cached) {
            Ok(entry) => entry,
            Err(e) => {
                log_error!(s.logger(), "failed to restore session from cache", "old_sid" => &prev_sid, "error" => e.0);
                return false;
            }
        };

        {
            let _reg = s.registration();
            if s.is_closed() {
                return false;
            }
            // Registered before the remote disconnects are checked: one recorded
            // after the check finds the session in the hub.
            s.set_identifiers(&entry.ids);
            self.hub.add_session(s.clone());
            if self.revocations.refuses(&entry.ids, entry.authenticated_at, self.clock.now()) {
                // Disconnected remotely after this snapshot (D-084): the
                // application authenticates the session instead.
                self.hub.remove_session(s);
                s.set_identifiers("");
                log_debug!(s.logger(), "session not restored: disconnected remotely", "old_sid" => &prev_sid);
                return false;
            }
            let authenticated_at = entry.authenticated_at;
            s.apply_cache_entry(entry);
            log_debug!(s.logger(), "session restored", "old_sid" => &prev_sid);
            s.set_connected(true);
            s.mark_authenticated(authenticated_at);

            // Resubscribe to streams
            for (identifier, streams) in s.subscriptions().to_map() {
                for stream in streams {
                    let stream_id = broker.subscribe(&stream);
                    self.hub.subscribe_session(s, &stream_id, &identifier);
                }
            }
        }

        s.send(&Reply {
            kind: WELCOME_TYPE.to_owned(),
            sid,
            restored: true,
            restored_ids: s.subscriptions().channels(),
            ..Default::default()
        });

        self.commit_session(s);

        true
    }

    pub fn subscribe(&self, s: &Arc<Session>, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        let id = &msg.identifier;
        if s.subscriptions().has_channel(id) {
            return Err(NodeError(format!("already subscribed to {}", filtered(id))));
        }

        let env = s.get_env();
        let Outcome { result, error } = if self.refuses_pusher_channel(s, id) {
            // The `$pusher` channel only accepts subscriptions from the Pusher endpoint.
            log_debug!(s.logger(), "the Pusher channel is only available at the Pusher endpoint",
                "identifier" => filtered_identifier(id).log_value());
            Outcome {
                result: Some(CommandResult {
                    status: Status::Failure,
                    transmissions: vec![rejection_message(id)],
                    ..Default::default()
                }),
                error: None,
            }
        } else {
            with_close_context(s.close_context(), || {
                self.controller().subscribe(&s.get_id(), Some(&env), &env.identifiers, id)
            })
        };

        if s.is_closed() {
            log_debug!(s.logger(), "skip subscribe result: closed");
            return Ok(None);
        }

        if result.is_none() && error.is_none() {
            return Ok(None);
        }

        log_debug!(s.logger(), "controller subscribe",
            "response" => result_value(result.as_ref()), "err" => error_value(error.as_ref()));

        let mut confirmed = false;
        match (&error, &result) {
            (Some(err), None) => return Err(decorate(err, format!("subscribe failed for {}", filtered(id)))),
            (Some(err), Some(res)) if res.status == Status::Error => {
                return Err(decorate(err, format!("subscribe failed for {}", filtered(id))));
            }
            (None, Some(res)) if res.status == Status::Success => {
                confirmed = true;
                s.subscriptions().add_channel(id);
                log_debug!(s.logger(), "subscribed", "identifier" => filtered_identifier(id).log_value());
            }
            (None, Some(_)) => {
                log_debug!(s.logger(), "subscription rejected", "identifier" => filtered_identifier(id).log_value());
            }
            _ => {}
        }

        // D-106: live messages wait until the requested history is sent.
        let _hold = (confirmed && (msg.history.since > 0 || !msg.history.streams.is_empty())).then(|| s.hold_live(id));

        if let Some(res) = &result {
            self.handle_command_reply(s, msg, res);
            self.mark_disconnectable(s, res.disconnect_interest);
        }

        if confirmed {
            self.commit_session(s);

            if (msg.history.since > 0 || !msg.history.streams.is_empty())
                && let Err(e) = self.history(s, msg)
            {
                log_warn!(s.logger(), "couldn't retrieve history",
                    "identifier" => filtered_identifier(id).log_value(), "error" => e.0);
            }

            if let Some(presence) = &msg.presence
                && let Err(e) = self.handle_presence_reply(s, id, PRESENCE_JOIN_TYPE, Some(presence))
            {
                log_warn!(s.logger(), "couldn't process presence join",
                    "identifier" => filtered_identifier(id).log_value(), "error" => e.0);
            }
        }

        Ok(result)
    }

    /// Reserves `$pusher` for the Pusher endpoint: once called (when
    /// Pusher compatibility is mounted), sessions that do not go through the
    /// Pusher executor get a rejection when they subscribe to it.
    pub fn reserve_pusher_channel(&self) {
        self.pusher_channel_reserved.store(true, Ordering::Release);
    }

    /// Whether subscribing `s` to `identifier` must be refused: the identifier's
    /// channel (decoded as the channels router does) is `$pusher`, it is
    /// reserved, and `s` is not a Pusher session.
    fn refuses_pusher_channel(&self, s: &Session, identifier: &str) -> bool {
        if !self.pusher_channel_reserved.load(Ordering::Acquire) || s.is_pusher() {
            return false;
        }
        matches!(
            cabby_core::gojson::decode_string_fields(identifier, "struct { Channel string }", &["channel"]),
            Ok([channel]) if channel == crate::pusher::CHANNEL_NAME
        )
    }

    pub fn unsubscribe(&self, s: &Arc<Session>, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        let id = &msg.identifier;
        if !s.subscriptions().has_channel(id) {
            return Err(NodeError(format!("unknown subscription: {}", filtered(id))));
        }

        let env = s.get_env();
        let Outcome { mut result, error } = with_close_context(s.close_context(), || {
            self.controller().unsubscribe(&s.get_id(), Some(&env), &env.identifiers, id)
        });

        if s.is_closed() {
            log_debug!(s.logger(), "skip unsubscribe result: closed");
            return Ok(None);
        }

        if result.is_none() && error.is_none() {
            return Ok(None);
        }

        log_debug!(s.logger(), "controller unsubscribe",
            "response" => result_value(result.as_ref()), "err" => error_value(error.as_ref()));

        let mut presence_stream = String::new();
        match (&error, result.as_mut()) {
            (Some(err), None) => return Err(decorate(err, format!("failed to unsubscribe from {}", filtered(id)))),
            (Some(err), Some(res)) if res.status == Status::Error => {
                return Err(decorate(err, format!("failed to unsubscribe from {}", filtered(id))));
            }
            (Some(_), Some(_)) => {}
            (None, res) => {
                // Make sure to remove all streams subscriptions
                if let Some(res) = res {
                    res.stop_all_streams = true;
                }
                s.update_env(&mut |env| {
                    presence_stream = env.get_channel_state_field(id, PRESENCE_STREAM_STATE);
                    env.remove_channel_state(id);
                });
                log_debug!(s.logger(), "unsubscribed", "identifier" => filtered_identifier(id).log_value());
            }
        }

        if let Some(res) = &result {
            self.handle_command_reply(s, msg, res);
        }

        // The closed session needs its channels to correctly unsubscribe pub/sub on disconnect
        if error.is_none() && !s.is_closed() {
            s.subscriptions().remove_channel(id);
        }

        self.commit_session(s);

        // Make sure presence is removed on explicit unsubscribe
        if !presence_stream.is_empty() {
            log_debug!(s.logger(), "remove presence", "stream" => &presence_stream);
            if let Err(e) = self.broker().presence_remove(&presence_stream, &s.get_id()) {
                log_error!(s.logger(), "failed to remove presence", "error" => e.message);
            }
        }

        Ok(result)
    }

    pub fn perform(&self, s: &Arc<Session>, msg: &Message) -> Result<Option<CommandResult>, NodeError> {
        let id = &msg.identifier;
        if !s.subscriptions().has_channel(id) {
            return Err(NodeError(format!("unknown subscription {}", filtered(id))));
        }

        let data = match &msg.data {
            Some(MessageData::Json(serde_json::Value::String(data))) => data,
            other => return Err(NodeError(format!("perform data must be a string, got {}", data_text(other)))),
        };

        let env = s.get_env();
        let Outcome { result, error } = with_close_context(s.close_context(), || {
            self.controller().perform(&s.get_id(), Some(&env), &env.identifiers, id, data)
        });

        if s.is_closed() {
            log_debug!(s.logger(), "skip perform result: closed");
            return Ok(None);
        }

        if result.is_none() && error.is_none() {
            return Ok(None);
        }

        log_debug!(s.logger(), "controller perform",
            "response" => result_value(result.as_ref()), "err" => error_value(error.as_ref()));

        if let Some(err) = &error
            && result.as_ref().is_none_or(|r| r.status == Status::Error)
        {
            return Err(decorate(err, format!("perform failed for {}", filtered(id))));
        }

        if let Some(res) = &result
            && self.handle_command_reply(s, msg, res)
        {
            self.commit_session(s);
        }

        Ok(result)
    }

    /// Sends the requested backlog followed by `confirm_history`, or
    /// `reject_history` on broker errors (upstream `History`).
    pub fn history(&self, s: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        let id = &msg.identifier;
        let Some(streams) = s.subscriptions().streams_for(id) else {
            return Err(NodeError(format!("unknown subscription {}", filtered(id))));
        };

        let history = &msg.history;
        if history.since == 0 && history.streams.is_empty() {
            return Err(NodeError(format!("history request is missing, got {msg:?}")));
        }

        let backlog = self.retrieve_history(history, &streams);

        if s.is_closed() {
            log_debug!(s.logger(), "skip history result: closed");
            return Ok(());
        }

        let backlog = match backlog {
            Ok(backlog) => backlog,
            Err(e) => {
                s.send(&Reply { kind: HISTORY_REJECTED_TYPE.to_owned(), identifier: id.clone(), ..Default::default() });
                return Err(broker_error(e));
            }
        };

        for el in &backlog {
            s.send(&el.to_reply_for(id));
        }

        s.send(&Reply { kind: HISTORY_CONFIRMED_TYPE.to_owned(), identifier: id.clone(), ..Default::default() });
        s.release_live(id, &backlog);

        Ok(())
    }

    fn retrieve_history(
        &self,
        history: &HistoryRequest,
        streams: &[String],
    ) -> Result<Vec<StreamMessage>, BrokerError> {
        let broker = self.broker();
        let mut backlog = Vec::new();

        for stream in streams {
            if let Some(pos) = history.streams.get(stream) {
                backlog.extend(broker.history_from(stream, &pos.epoch, pos.offset)?);
                continue;
            }

            if history.since > 0
                && let Some(messages) = broker.history_since(stream, history.since)?
            {
                backlog.extend(messages);
            }
        }

        Ok(backlog)
    }

    /// Broadcasts the message to the session's whisper stream (from the channel
    /// state), excluding the sender; ignored without a whisper stream (upstream
    /// `Whisper`).
    pub fn whisper(&self, s: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        let stream = s.channel_state_field(&msg.identifier, WHISPER_STREAM_STATE);
        if stream.is_empty() {
            log_debug!(s.logger(), "whisper stream not found",
                "identifier" => filtered_identifier(&msg.identifier).log_value());
            return Ok(());
        }

        let broadcast = StreamMessage {
            stream,
            data: data_json(&msg.data),
            meta: Some(StreamMessageMetadata { exclude_socket: s.get_id(), transient: true, ..Default::default() }),
            ..Default::default()
        };

        match self.broker().handle_broadcast(&broadcast) {
            Err(e) => log_error!(s.logger(), "failed to whisper", "stream" => &broadcast.stream, "err" => e.message),
            Ok(()) => log_debug!(s.logger(), "whispered", "stream" => &broadcast.stream),
        }

        Ok(())
    }

    pub fn presence(&self, s: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        let id = &msg.identifier;
        if !s.subscriptions().has_channel(id) {
            return Err(NodeError(format!("unknown subscription {}", filtered(id))));
        }

        // Check that the presence stream is configured (thus, the feature is enabled)
        let stream = s.channel_state_field(id, PRESENCE_STREAM_STATE);
        if stream.is_empty() {
            return Err(NodeError(format!("presence stream not found for identifier: {}", filtered(id))));
        }

        // Upstream means to read options from the message data, but only does so
        // when re-encoding the data fails: the defaults always apply.
        let info = self.broker().presence_info(&stream, Some(&PresenceInfoOptions::new()));

        if s.is_closed() {
            log_debug!(s.logger(), "skip presence result: closed");
            return Ok(());
        }

        let (info, res) = match info {
            Ok(info) => (info, Ok(())),
            Err(e) => (PresenceInfo { kind: ERROR_TYPE.to_owned(), ..Default::default() }, Err(broker_error(e))),
        };
        s.send(&PresenceReply { identifier: id, info: &info });
        res
    }

    pub fn presence_join(&self, s: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        if !s.subscriptions().has_channel(&msg.identifier) {
            return Err(NodeError(format!("unknown subscription {}", filtered(&msg.identifier))));
        }
        self.handle_presence_reply(s, &msg.identifier, PRESENCE_JOIN_TYPE, msg.presence.as_ref())
    }

    pub fn presence_leave(&self, s: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        if !s.subscriptions().has_channel(&msg.identifier) {
            return Err(NodeError(format!("unknown subscription {}", filtered(&msg.identifier))));
        }
        self.handle_presence_reply(s, &msg.identifier, PRESENCE_LEAVE_TYPE, msg.presence.as_ref())
    }

    /// Delivers a stream message to local sessions (upstream `Broadcast`).
    pub fn broadcast(&self, msg: &StreamMessage) {
        self.metrics.counter_increment(METRICS_BROADCAST_MSG);
        log_debug!(self.log, "incoming broadcast message", "payload" => msg.log_value());
        self.hub.broadcast_message(msg.clone());
    }

    pub fn execute_remote_command(&self, msg: &RemoteCommandMessage) {
        self.metrics.counter_increment(METRICS_REMOTE_COMMANDS);
        if msg.command == "disconnect" {
            match msg.to_remote_disconnect_message() {
                Ok(dmsg) => {
                    log_debug!(self.log, "incoming remote command", "command" => dmsg.log_value());
                    self.remote_disconnect(&dmsg);
                }
                Err(e) => {
                    log_warn!(self.log, "failed to parse remote disconnect command",
                        "data" => msg.log_value(), "error" => e.to_string());
                }
            }
        }
    }

    /// Unregisters the session (hub, broker streams) and enqueues its Disconnect
    /// call if it is disconnectable (upstream `Disconnect`).
    pub fn disconnect(&self, s: &Arc<Session>) -> Result<(), NodeError> {
        if s.is_inactive() {
            return Ok(());
        }

        let broker = self.broker();
        {
            let _reg = s.registration();
            // Unsubscribe from all streams to clean up broker subscriptions
            let all_streams = s.subscriptions().to_map();
            let channels = all_streams.len();
            for streams in all_streams.into_values() {
                for stream in streams {
                    broker.unsubscribe(&stream);
                }
            }
            if channels > 0 {
                log_debug!(s.logger(), "unsubscribed from broker streams", "channels" => channels);
            }

            // Make sure the session is removed from the hub, so we don't try to
            // send broadcast messages to it
            self.hub.remove_session(s);
        }

        let sid = s.get_id();
        self.write_session_cache(s, || {
            let _ = broker.touch_session(&sid);
        });
        let _ = broker.touch_presence(&sid);

        if !s.is_disconnectable() {
            return Ok(());
        }
        if self.is_shutting_down() {
            self.disconnect_now(s)
        } else {
            // Upstream has no disconnector until `SetDisconnector` (a nil field).
            read(&self.disconnector).map_or(Ok(()), |d| d.enqueue(s.clone()))
        }
    }

    /// Calls the controller's Disconnect now (upstream `DisconnectNow`).
    pub fn disconnect_now(&self, s: &Arc<Session>) -> Result<(), NodeError> {
        let subscriptions = s.subscriptions().channels();
        let env = s.get_env();

        log_debug!(s.logger(), "disconnect",
            "ids" => &env.identifiers,
            "url" => filtered_url(&env.url).log_value(),
            "headers" => filtered_map(sorted_headers(&env).as_ref()).log_value(),
            "subscriptions" => subscriptions.len());

        let res = self.controller().disconnect(&s.get_id(), Some(&env), &env.identifiers, &subscriptions);

        if let Err(e) = &res {
            log_error!(s.logger(), "controller disconnect failed", "error" => e.to_string());
        }

        log_debug!(s.logger(), "controller disconnect succeeded");

        res.map_err(|e| NodeError(e.message))
    }

    /// Disconnects the identifiers' sessions (upstream `RemoteDisconnect`).
    /// Recorded first: for the sessions TTL, the sessions of these identifiers
    /// authenticated before now, connected or cached, are authenticated by the
    /// application instead of restored (D-084).
    pub fn remote_disconnect(&self, msg: &RemoteDisconnectMessage) {
        self.metrics.counter_increment(METRICS_BROADCAST_MSG);
        log_debug!(self.log, "incoming pubsub command", "data" => msg.log_value());
        self.revocations.revoke(&msg.identifier, self.clock.unix_micros(), self.clock.now(), self.sessions_ttl());
        self.hub.remote_disconnect(msg.clone());
    }

    /// Interest is -1 (none), 0 (unknown) or 1 (interested); the disconnect mode
    /// decides what it means (upstream unexported `markDisconnectable`).
    fn mark_disconnectable(&self, s: &Session, interest: i32) {
        match self.config.disconnect_mode.as_str() {
            DISCONNECT_MODE_ALWAYS => s.mark_disconnectable(true),
            DISCONNECT_MODE_NEVER => s.mark_disconnectable(false),
            DISCONNECT_MODE_AUTO => s.mark_disconnectable(interest >= 0),
            _ => {}
        }
    }

    /// Sessions registered with the hub.
    pub fn size(&self) -> usize {
        self.hub.size()
    }

    /// Upstream `HandleJoin` / `HandleLeave` (also via [`Presenter`]).
    pub fn handle_join(&self, stream: &str, msg: &PresenceEvent) {
        self.broadcast_presence_event(stream, msg);
    }

    pub fn handle_leave(&self, stream: &str, msg: &PresenceEvent) {
        self.broadcast_presence_event(stream, msg);
    }

    fn broadcast_presence_event(&self, stream: &str, msg: &PresenceEvent) {
        let _ = self.broker().handle_broadcast(&StreamMessage {
            stream: stream.to_owned(),
            data: cabby_core::gojson::to_string(msg),
            meta: Some(StreamMessageMetadata {
                broadcast_type: PRESENCE_TYPE.to_owned(),
                transient: true,
                ..Default::default()
            }),
            ..Default::default()
        });
    }

    /// Applies a command result: stream changes (under the session's
    /// registration lock), channel and connection state, broadcasts,
    /// transmissions and presence. Returns whether any cached state changed
    /// (upstream unexported `handleCommandReply`).
    fn handle_command_reply(&self, s: &Arc<Session>, msg: &Message, reply: &CommandResult) -> bool {
        let id = &msg.identifier;
        let mut dirty = false;

        let closed = {
            let _reg = s.registration();
            if s.is_closed() {
                true
            } else {
                let broker = self.broker();
                if reply.stop_all_streams {
                    self.hub.unsubscribe_session_from_channel(s, id);
                    for stream in s.subscriptions().remove_channel_streams(id).unwrap_or_default() {
                        dirty = true;
                        broker.unsubscribe(&stream);
                    }
                } else if !reply.stopped_streams.is_empty() {
                    dirty = true;
                    for stream in &reply.stopped_streams {
                        let stream_id = broker.unsubscribe(stream);
                        self.hub.unsubscribe_session(s, &stream_id, id);
                        s.subscriptions().remove_channel_stream(id, &stream_id);
                    }
                }

                if !reply.streams.is_empty() {
                    dirty = true;
                    for stream in &reply.streams {
                        let stream_id = broker.subscribe(stream);
                        self.hub.subscribe_session(s, &stream_id, id);
                        s.subscriptions().add_channel_stream(id, &stream_id);
                    }
                }
                false
            }
        };

        // D-105: the transmissions (the confirmation) go once the streams' pub/sub subscriptions are active.
        if !closed {
            super::activation::wait_for_streams(&self.activations, s, &reply.streams);
        }

        if !closed {
            if let Some(istate) = &reply.istate {
                dirty = true;
                s.update_env(&mut |env| env.merge_channel_state(id, istate));
            }

            let connection_dirty =
                self.handle_call_reply(s, reply.cstate.as_ref(), &reply.broadcasts, &reply.transmissions);
            dirty = dirty || connection_dirty;

            if let Some(presence) = &reply.presence
                && let Err(e) = self.handle_presence_reply(s, id, &presence.kind, Some(presence))
            {
                log_warn!(s.logger(), "failed to process presence", "err" => e.0);
            }
        }

        if reply.disconnect {
            s.disconnect("Command Failed", CLOSE_ABNORMAL_CLOSURE);
        }

        dirty && !closed
    }

    /// Applies the connection-level part of a result (upstream unexported
    /// `handleCallReply`); returns whether the connection state changed.
    fn handle_call_reply(
        &self,
        s: &Arc<Session>,
        cstate: Option<&StringMap>,
        broadcasts: &[StreamMessage],
        transmissions: &[String],
    ) -> bool {
        let mut dirty = false;

        if let Some(cstate) = cstate {
            dirty = true;
            s.update_env(&mut |env| env.merge_connection_state(cstate));
        }

        if !broadcasts.is_empty() {
            let broker = self.broker();
            for broadcast in broadcasts {
                let _ = broker.handle_broadcast(broadcast);
            }
        }

        for msg in transmissions {
            s.send_json_transmission(msg);
        }

        dirty
    }

    fn handle_presence_reply(
        &self,
        s: &Arc<Session>,
        identifier: &str,
        event: &str,
        presence: Option<&PresenceEvent>,
    ) -> Result<(), NodeError> {
        // Check that the presence stream is configured (thus, the feature is enabled)
        let stream = s.channel_state_field(identifier, PRESENCE_STREAM_STATE);
        if stream.is_empty() {
            return Err(NodeError(format!("presence stream not found for identifier: {identifier}")));
        }

        let sid = s.get_id();
        let broker = self.broker();
        log_debug!(s.logger(), "presence event", "event" => event,
            "presence" => presence.map_or_else(|| LogValue::String("<nil>".to_owned()), LogValuer::log_value));
        match event {
            PRESENCE_JOIN_TYPE => {
                let Some(presence) = presence else {
                    return Err(NodeError("presence data is missing".into()));
                };
                // The broker keeps the info beyond this message.
                let info = presence.info.as_ref().map_or(serde_json::Value::Null, cabby_core::gojson::deep::bounded);
                broker.presence_add(&stream, &sid, &presence.id, info).map(|_| ()).map_err(broker_error)
            }
            PRESENCE_LEAVE_TYPE => broker.presence_remove(&stream, &sid).map(|_| ()).map_err(broker_error),
            other => Err(NodeError(format!("unknown presence event: {other}"))),
        }
    }
}

/// Thread count and resident memory of the process (the counterparts of Go's
/// goroutine count and `MemStats.Sys`); zero where the platform does not tell.
fn process_stats() -> (u64, u64) {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else { return (0, 0) };
    let field = |name: &str| {
        status
            .lines()
            .find_map(|l| l.strip_prefix(name))
            .and_then(|v| v.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (field("Threads:"), field("VmRSS:") * 1024)
}

/// Upstream `HandleCommand`: dispatches a client command; `pong` only keeps the
/// session alive.
impl Executor for Node {
    fn is_inline(&self, msg: &Message) -> bool {
        matches!(msg.command.as_str(), "ping" | "pong")
    }

    fn handle_command(&self, session: &Arc<Session>, msg: &Message) -> Result<(), NodeError> {
        log_debug!(session.logger(), "incoming message", "data" => msg.log_value());

        session.keepalive();

        let res = match msg.command.as_str() {
            "pong" => Ok(()),
            "ping" => {
                session.handle_client_ping();
                Ok(())
            }
            "subscribe" => self.subscribe(session, msg).map(|_| ()),
            "unsubscribe" => self.unsubscribe(session, msg).map(|_| ()),
            "message" => self.perform(session, msg).map(|_| ()),
            "history" => self.history(session, msg),
            "presence" => self.presence(session, msg),
            "join" => self.presence_join(session, msg),
            "leave" => self.presence_leave(session, msg),
            "whisper" => self.whisper(session, msg),
            other => Err(NodeError(format!("unknown command: {other}"))),
        };

        // Reset the pong timer after the message has been processed (so the
        // client is not disconnected when processing took too long)
        session.reset_pong();

        res
    }

    fn disconnect(&self, session: &Arc<Session>) -> Result<(), NodeError> {
        Node::disconnect(self, session)
    }

    fn clock(&self) -> Option<Arc<dyn Clock>> {
        Some(self.clock.clone())
    }
}

impl Presenter for Node {
    fn handle_join(&self, stream: &str, msg: &PresenceEvent) {
        Node::handle_join(self, stream, msg);
    }

    fn handle_leave(&self, stream: &str, msg: &PresenceEvent) {
        Node::handle_leave(self, stream, msg);
    }
}

/// The pub/sub side of the node (upstream `pubsub.Handler`, implemented by `*Node`).
impl SubscriberHandler for Node {
    fn broadcast(&self, msg: &StreamMessage) {
        Node::broadcast(self, msg);
    }

    fn execute_remote_command(&self, msg: &RemoteCommandMessage) {
        Node::execute_remote_command(self, msg);
    }

    fn activations(&self) -> Option<Arc<Activations>> {
        Some(self.activations.clone())
    }
}

/// Rejects every connection and subscription (upstream `NullController`, used
/// when no RPC is configured).
#[derive(Debug, Default, Clone, Copy)]
pub struct NullController;

/// Upstream `l.With("context", "rpc", "impl", "null")`.
fn null_controller_log() -> Logger {
    Logger::default().with("context", "rpc").with("impl", "null")
}

impl Controller for NullController {
    fn start(&self) -> Result<(), ControllerError> {
        log_info!(null_controller_log(), "no RPC configured");
        Ok(())
    }
    fn shutdown(&self) -> Result<(), ControllerError> {
        Ok(())
    }
    fn authenticate(&self, _sid: &str, _env: Option<&SessionEnv>) -> Outcome<ConnectResult> {
        log_debug!(null_controller_log(), "reject connection");
        Outcome {
            result: Some(ConnectResult {
                status: Status::Failure,
                transmissions: vec![disconnection_message(UNAUTHORIZED_REASON, false)],
                disconnect_interest: -1,
                ..Default::default()
            }),
            error: None,
        }
    }
    fn subscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, channel: &str) -> Outcome<CommandResult> {
        log_debug!(null_controller_log(), "reject subscription", "channel" => channel);
        Outcome {
            result: Some(CommandResult {
                status: Status::Failure,
                transmissions: vec![rejection_message(channel)],
                disconnect_interest: -1,
                ..Default::default()
            }),
            error: None,
        }
    }
    fn unsubscribe(&self, _sid: &str, _env: Option<&SessionEnv>, _ids: &str, _channel: &str) -> Outcome<CommandResult> {
        Outcome { result: None, error: Some(ControllerError::new("not implemented")) }
    }
    fn perform(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _channel: &str,
        _data: &str,
    ) -> Outcome<CommandResult> {
        Outcome { result: None, error: Some(ControllerError::new("not implemented")) }
    }
    fn disconnect(
        &self,
        _sid: &str,
        _env: Option<&SessionEnv>,
        _ids: &str,
        _subscriptions: &[String],
    ) -> Result<(), ControllerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Upstream encodes the presence info struct in field order, nested in the reply.
    #[test]
    fn presence_reply_keeps_go_field_order() {
        let info = PresenceInfo {
            kind: "info".into(),
            total: 1,
            records: vec![PresenceEvent { kind: String::new(), id: "42".into(), info: Some(json!({"name": "Sasha"})) }],
        };
        let mut out = Vec::new();
        PresenceReply { identifier: "chat_1", info: &info }.write_json(&mut out);
        assert_eq!(
            r#"{"type":"presence","identifier":"chat_1","message":{"type":"info","total":1,"records":[{"info":{"name":"Sasha"},"id":"42"}]}}"#,
            String::from_utf8(out).unwrap()
        );

        let mut out = Vec::new();
        let error = PresenceInfo { kind: ERROR_TYPE.into(), ..Default::default() };
        PresenceReply { identifier: "chat_1", info: &error }.write_json(&mut out);
        assert_eq!(
            r#"{"type":"presence","identifier":"chat_1","message":{"type":"error","total":0}}"#,
            String::from_utf8(out).unwrap()
        );
    }

    struct Discard;

    impl Connection for Discard {
        fn write(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn write_binary(&self, _msg: &[u8], _deadline: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn write_ping(&self, _deadline: Instant) -> Result<(), ConnectionError> {
            Ok(())
        }
        fn read(&self) -> Result<Vec<u8>, ConnectionError> {
            Err(ConnectionError("websocket: close 1000 (normal)".into()))
        }
        fn close(&self, _code: i32, _reason: &str) {}
    }

    /// Deviation D-073: presence info nested deeper than 126 levels is kept as
    /// its JSON text.
    #[test]
    fn deviation_d073_deep_presence_info_is_kept_as_text() {
        use cabby_core::broker::{Config as BrokerConfig, Memory, SystemClock as BrokerClock};

        let node = Node::new(Config::new(), Vec::new());
        let broker = Arc::new(Memory::new(None, None, &BrokerConfig::new(), Arc::new(BrokerClock)));
        node.set_broker(broker.clone());
        let s = Session::new(&node, Arc::new(Discard), "/cable", None, "sid", Vec::new());
        s.subscriptions().add_channel("chat");
        let state = StringMap::from([(PRESENCE_STREAM_STATE.to_owned(), "room".to_owned())]);
        s.update_env(&mut |env| env.merge_channel_state("chat", &state));

        let kept = |depth: usize| {
            let text = format!("{}{}", "[".repeat(depth), "]".repeat(depth));
            let join = format!(r#"{{"command":"join","identifier":"chat","presence":{{"id":"1","info":{text}}}}}"#);
            s.read_message(join.as_bytes()).expect("handled");
            let mut records = broker.presence_info("room", None).expect("presence info").records;
            (text, records.pop().expect("a record").info.expect("info"))
        };
        let (text, info) = kept(126);
        assert_eq!(text, info.to_string());
        for depth in [127, 500] {
            let (text, info) = kept(depth);
            assert_eq!(json!(text), info);
        }
    }

    /// Pins D-117: a session whose Disconnect call panics while the node shuts
    /// down is counted as done, so the shutdown does not wait for its deadline
    /// (10 s here). Before, the panic (caught by the disconnect pool) skipped
    /// the count, and `disconnect_all` waited until the deadline.
    #[test]
    fn deviation_d117_a_panicking_disconnect_does_not_hold_the_shutdown() {
        struct PanickingDisconnect;
        impl Controller for PanickingDisconnect {
            fn start(&self) -> Result<(), ControllerError> {
                Ok(())
            }
            fn shutdown(&self) -> Result<(), ControllerError> {
                Ok(())
            }
            fn authenticate(&self, _: &str, _: Option<&SessionEnv>) -> Outcome<ConnectResult> {
                Outcome { result: None, error: Some(ControllerError::new("not used")) }
            }
            fn subscribe(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str) -> Outcome<CommandResult> {
                Outcome { result: None, error: Some(ControllerError::new("not used")) }
            }
            fn unsubscribe(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str) -> Outcome<CommandResult> {
                Outcome { result: None, error: Some(ControllerError::new("not used")) }
            }
            fn perform(&self, _: &str, _: Option<&SessionEnv>, _: &str, _: &str, _: &str) -> Outcome<CommandResult> {
                Outcome { result: None, error: Some(ControllerError::new("not used")) }
            }
            fn disconnect(
                &self,
                _: &str,
                _: Option<&SessionEnv>,
                _: &str,
                _: &[String],
            ) -> Result<(), ControllerError> {
                panic!("disconnect failed");
            }
        }

        let node = Node::new(Config::new(), vec![NodeOption::Controller(Arc::new(PanickingDisconnect))]);
        let s = Session::new(&node, Arc::new(Discard), "/cable", None, "sid", Vec::new());
        // As authentication marks it with `--disconnect_mode=always`.
        s.mark_disconnectable(true);
        node.authenticated(&s, "ids");
        assert_eq!(1, node.hub.size());

        let ctx =
            Context { deadline: Some(tokio::time::Instant::now() + Duration::from_secs(10)), ..Context::default() };
        let started = std::time::Instant::now();
        node.shutdown(&ctx).expect("shut down");
        assert!(started.elapsed() < Duration::from_secs(5), "the shutdown took {:?}", started.elapsed());
    }

    /// Deviation D-016: `goroutines_num` and `mem_sys_bytes` report the process's
    /// OS threads and resident memory.
    #[test]
    fn deviation_d016_process_stats_report_threads_and_resident_memory() {
        let (threads, memory) = process_stats();
        if cfg!(target_os = "linux") {
            assert!(threads >= 1, "threads: {threads}");
            assert!(memory > 0, "memory: {memory}");
        }
    }

    /// Deviation D-051: the batch broadcast debug record lists the messages (Go
    /// prints the addresses of the decoded elements, `&[0xc000… 0xc000…]`).
    #[test]
    fn deviation_d051_batch_broadcast_record_lists_the_messages() {
        let batch = vec![
            StreamMessage { stream: "a".into(), data: "1".into(), ..Default::default() },
            StreamMessage { stream: "b".into(), data: "\"x\"".into(), ..Default::default() },
        ];
        assert_eq!("[[stream=a data=1] [stream=b data=\"x\"]]", batch_log_value(&batch).to_string());
    }

    #[test]
    fn null_controller_rejects() {
        let c = NullController;
        let auth = c.authenticate("sid", None).result.unwrap();
        assert_eq!(Status::Failure, auth.status);
        assert_eq!(
            vec![r#"{"type":"disconnect","reason":"unauthorized","reconnect":false}"#.to_owned()],
            auth.transmissions
        );
        assert_eq!(-1, auth.disconnect_interest);

        let sub = c.subscribe("sid", None, "", "chat").result.unwrap();
        assert_eq!(Status::Failure, sub.status);
        assert_eq!(vec![rejection_message("chat")], sub.transmissions);

        assert_eq!(Some(ControllerError::new("not implemented")), c.perform("sid", None, "", "chat", "").error);
        assert_eq!(Some(ControllerError::new("not implemented")), c.unsubscribe("sid", None, "", "chat").error);
    }
}
