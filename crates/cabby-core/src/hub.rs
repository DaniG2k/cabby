//! Hub routing (upstream package `hub`): sessions, stream subscriptions sharded
//! over gates, broadcast fan-out and remote disconnects.
//!
//! # Design
//!
//! This is the server's hot path. Behavior follows upstream; the internals differ:
//!
//! * **Sharding.** Streams are assigned to gates (shards) by FNV-1a 64 of the stream
//!   name, as upstream. Each gate owns one OS thread that performs its broadcasts,
//!   fed by a bounded FIFO queue of [`BROADCAST_QUEUE_SIZE`] messages; a full queue
//!   blocks the broadcaster (upstream's buffered channel back-pressure).
//! * **No central dispatcher.** Upstream funnels every broadcast through the hub's
//!   `Run` goroutine, which only forwards it to the stream's gate. Here
//!   [`Hub::broadcast_message`] routes to the gate queue directly from the caller's
//!   thread. Messages for one stream from one producer stay in order (one FIFO
//!   queue, one consumer), which is the ordering upstream guarantees; nothing is
//!   ordered across gates in either implementation. [`Hub::run`] only parks until
//!   [`Hub::shutdown`].
//! * **Cached snapshots.** Upstream copies a stream's (session, identifier) rows on
//!   every broadcast. A gate here keeps, per stream, an immutable snapshot grouped by
//!   identifier behind an `Arc`, built on the first broadcast after a subscription
//!   change. Broadcasting to an unchanged stream takes a read lock for an `Arc`
//!   clone; the lock is never held while sessions are called.
//! * **Encode once.** Each broadcast builds one [`CachedEncodedMessage`] per channel
//!   identifier (the stream data is parsed once even when several identifiers
//!   subscribe the stream), and sessions fetch their encoder's frame from it, so a
//!   frame is encoded once per encoder and shared (`Arc<SentFrame>`) across
//!   sessions.
//! * **Locks.** Hub-level bookkeeping (sessions, identifiers) is one `RwLock`; each
//!   session's list of subscriptions has its own mutex, so subscribing different
//!   sessions does not serialize on a hub-wide write lock. Lock order is
//!   session list, then gate. Session methods (`get_id`, `send`, ...) are never
//!   called while any hub or gate lock is held, so sessions may call into the
//!   hub while holding their own locks.
//!
//! The hub is generic over the session type so the node can use its concrete
//! session (`Hub<Session>`, static dispatch, `find_by_identifier` returning the
//! concrete type); the default is `dyn HubSession`.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{
    Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

use crate::common::{self, DisconnectMessage, RemoteDisconnectMessage, Reply, StreamMessage, filtered_identifier};
/// The messages a hub hands to sessions (upstream `encoders.EncodedMessage`, which
/// the hub always passes as an `encoders.CachedEncodedMessage`). Its `to_json` is
/// the `encoders.JSON` payload.
pub use crate::encoders::CachedEncodedMessage as EncodedMessage;
pub use crate::encoders::{CachedEncodedMessage, SharedFrame};
use crate::logger::{LogValuer, Logger};
use crate::utils::GoPool;
use crate::{log_debug, log_error, log_warn};

/// Capacity of each gate's broadcast queue (upstream `make(chan ..., 256)`).
pub const BROADCAST_QUEUE_SIZE: usize = 256;

/// Size of the worker pool running remote commands (upstream `NewGoPool("remote commands", 256)`).
pub const REMOTE_COMMANDS_POOL_SIZE: usize = 256;

/// A session as seen by the hub (upstream `hub.HubSession`).
///
/// `send` and `disconnect_with_message` are called from gate threads (and the
/// remote-commands pool); they must not block for long. A session typically
/// fetches its encoder's frame with [`CachedEncodedMessage::fetch_with`] and queues
/// the shared frame for its writer.
pub trait HubSession: Send + Sync {
    fn get_id(&self) -> String;
    fn get_identifiers(&self) -> String;
    fn send(&self, msg: &EncodedMessage);
    fn disconnect_with_message(&self, msg: &EncodedMessage, code: &str);
}

pub type SessionRef = Arc<dyn HubSession>;

// ------------------------------------------------------------- helpers ---

fn read<T: ?Sized>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T: ?Sized>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sessions are keyed by allocation address, as upstream keys gate maps by the
/// `HubSession` interface value (pointer identity).
fn session_key<S: ?Sized>(session: &Arc<S>) -> usize {
    Arc::as_ptr(session) as *const () as usize
}

/// Hasher for session keys (addresses, not attacker-controlled): one multiply and
/// a fold so that both the low bits (bucket index) and the high bits carry entropy.
#[derive(Default)]
struct AddrHasher(u64);

impl Hasher for AddrHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = (self.0.rotate_left(8) ^ u64::from(*b)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }

    fn write_usize(&mut self, n: usize) {
        let h = (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = h ^ (h >> 29);
    }
}

type BySession<V> = HashMap<usize, V, BuildHasherDefault<AddrHasher>>;

/// Upstream `index(stream, size)`: FNV-1a 64 of the stream modulo the gate count.
fn gate_index(stream: &str, size: usize) -> usize {
    if size <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in stream.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % size as u64) as usize
}

/// The reply for subscribers of `identifier` (upstream `buildMessage` before wrapping).
fn reply_for(msg: &StreamMessage, identifier: &str) -> Reply {
    let mut reply = msg.to_reply_for(identifier);
    if let Some(meta) = &msg.meta {
        reply.kind = meta.broadcast_type.clone();
    }
    reply
}

/// Builds the reply delivered to subscribers of `identifier` (upstream unexported
/// `buildMessage`).
pub fn build_message(msg: &StreamMessage, identifier: &str) -> EncodedMessage {
    CachedEncodedMessage::new(common::EncodedMessage::Reply(reply_for(msg, identifier)))
}

// ---------------------------------------------------------------- gate ---

enum GateCommand {
    Broadcast(StreamMessage),
    Stop,
}

type GateSender = SyncSender<GateCommand>;

struct ContextInner {
    cancelled: AtomicBool,
    /// Queues of the gates started with this context, woken on cancel.
    gates: Mutex<Vec<Weak<GateSender>>>,
}

/// Cancellation scope of gate broadcast loops (upstream passes a
/// `context.Context` to `hub.NewGate`). Clones share the scope.
#[derive(Clone)]
pub struct GateContext {
    inner: Arc<ContextInner>,
}

impl GateContext {
    pub fn new() -> Self {
        Self { inner: Arc::new(ContextInner { cancelled: AtomicBool::new(false), gates: Mutex::new(Vec::new()) }) }
    }

    /// Cancels the context. Gates stop delivering: a message dequeued after this
    /// returns is dropped, and idle loops exit.
    pub fn cancel(&self) {
        if self.inner.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        let gates = std::mem::take(&mut *lock(&self.inner.gates));
        for gate in gates.iter().filter_map(Weak::upgrade) {
            // A full queue needs no wake-up: the loop sees the flag on its next message.
            let _ = gate.try_send(GateCommand::Stop);
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    fn register(&self, gate: &Arc<GateSender>) {
        let mut gates = lock(&self.inner.gates);
        gates.retain(|g| g.strong_count() > 0);
        gates.push(Arc::downgrade(gate));
    }
}

impl Default for GateContext {
    fn default() -> Self {
        Self::new()
    }
}

struct Subscriber<S: ?Sized> {
    session: Arc<S>,
    sid: Arc<str>,
    identifiers: Vec<Arc<str>>,
}

struct Member<S: ?Sized> {
    sid: Arc<str>,
    session: Arc<S>,
}

/// Subscribers of one stream and channel identifier.
struct Group<S: ?Sized> {
    identifier: Arc<str>,
    members: Vec<Member<S>>,
}

/// Immutable delivery list of one stream (upstream `createStreamSessionsSnapshot`).
struct Snapshot<S: ?Sized> {
    groups: Vec<Group<S>>,
}

struct StreamSubscribers<S: ?Sized> {
    sessions: BySession<Subscriber<S>>,
    /// Built lazily by the broadcast loop; reset whenever `sessions` changes.
    snapshot: OnceLock<Arc<Snapshot<S>>>,
}

impl<S: ?Sized> StreamSubscribers<S> {
    fn new() -> Self {
        Self { sessions: BySession::default(), snapshot: OnceLock::new() }
    }

    fn snapshot(&self) -> Arc<Snapshot<S>> {
        Arc::clone(self.snapshot.get_or_init(|| {
            let mut index: HashMap<&str, usize> = HashMap::new();
            let mut groups: Vec<Group<S>> = Vec::new();
            for sub in self.sessions.values() {
                for id in &sub.identifiers {
                    let i = *index.entry(id).or_insert_with(|| {
                        groups.push(Group { identifier: Arc::clone(id), members: Vec::new() });
                        groups.len() - 1
                    });
                    groups[i].members.push(Member { sid: Arc::clone(&sub.sid), session: Arc::clone(&sub.session) });
                }
            }
            Arc::new(Snapshot { groups })
        }))
    }
}

struct GateShared<S: ?Sized> {
    /// stream -> session -> identifiers (upstream `streams`).
    streams: RwLock<HashMap<String, StreamSubscribers<S>>>,
}

/// Values removed under a gate lock, dropped after it is released (dropping may
/// release the last reference to a session).
type Garbage<S> = (Option<Subscriber<S>>, Option<StreamSubscribers<S>>, Option<Arc<Snapshot<S>>>);

impl<S: ?Sized + HubSession> GateShared<S> {
    fn perform_broadcast(&self, msg: &StreamMessage) {
        let snapshot = match read(&self.streams).get(&msg.stream) {
            Some(subs) => subs.snapshot(),
            None => return,
        };

        let exclude = msg.meta.as_ref().map_or("", |m| m.exclude_socket.as_str());
        let shared_template = snapshot.groups.len() > 1;
        let mut template: Option<Reply> = None;

        for group in &snapshot.groups {
            let mut encoded: Option<EncodedMessage> = None;
            for member in &group.members {
                if !exclude.is_empty() && *member.sid == *exclude {
                    continue;
                }
                let encoded = encoded.get_or_insert_with(|| {
                    // Parse the data once per broadcast; other identifiers reuse it.
                    let reply = match &template {
                        Some(t) => Reply { identifier: group.identifier.to_string(), ..t.clone() },
                        None => {
                            let reply = reply_for(msg, &group.identifier);
                            if shared_template {
                                template = Some(reply.clone());
                            }
                            reply
                        }
                    };
                    CachedEncodedMessage::new(common::EncodedMessage::Reply(reply))
                });
                member.session.send(encoded);
            }
        }
    }
}

fn gate_loop<S: ?Sized + HubSession>(shared: Arc<GateShared<S>>, queue: Receiver<GateCommand>, ctx: GateContext) {
    while let Ok(cmd) = queue.recv() {
        if ctx.is_cancelled() {
            return;
        }
        match cmd {
            // A panicking session must not end the gate's deliveries for every other
            // stream it serves (D-013; a Go panic would end the process).
            GateCommand::Broadcast(msg) => {
                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| shared.perform_broadcast(&msg)));
            }
            GateCommand::Stop => return,
        }
    }
}

/// One shard of the hub: subscriptions for some streams and their broadcast
/// queue and thread (upstream `hub.Gate`). Dropping the gate ends its thread
/// once the queue is drained.
pub struct Gate<S: ?Sized = dyn HubSession> {
    shared: Arc<GateShared<S>>,
    queue: Arc<GateSender>,
    log: Logger,
}

impl<S: ?Sized + HubSession + 'static> Gate<S> {
    /// Upstream `hub.NewGate(ctx, slog.Default())`: starts the broadcast loop.
    pub fn new(ctx: &GateContext) -> Self {
        Self::with_logger(ctx, Logger::default())
    }

    /// Upstream `hub.NewGate(ctx, logger)`: starts the broadcast loop.
    pub fn with_logger(ctx: &GateContext, log: Logger) -> Self {
        let shared = Arc::new(GateShared { streams: RwLock::new(HashMap::new()) });
        let (tx, rx) = mpsc::sync_channel(BROADCAST_QUEUE_SIZE);
        let queue = Arc::new(tx);
        ctx.register(&queue);

        let loop_shared = Arc::clone(&shared);
        let loop_ctx = ctx.clone();
        std::thread::Builder::new()
            .name("cabby-hub-gate".to_owned())
            .spawn(move || gate_loop(loop_shared, rx, loop_ctx))
            .expect("failed to spawn a hub gate thread");

        Self { shared, queue, log }
    }
}

impl<S: ?Sized + HubSession> Gate<S> {
    /// Queues `msg` for the stream's subscribers; a stream without subscribers is
    /// skipped. Blocks while the queue is full.
    pub fn broadcast(&self, msg: StreamMessage) {
        log_debug!(self.log, "schedule broadcast", "stream" => &msg.stream, "message" => msg.log_value());
        if !read(&self.shared.streams).contains_key(&msg.stream) {
            log_debug!(self.log, "no sessions", "stream" => &msg.stream);
            return;
        }
        // Fails only when the loop has ended (context cancelled): drop the message.
        let _ = self.queue.send(GateCommand::Broadcast(msg));
    }

    pub fn subscribe(&self, session: &Arc<S>, stream: &str, identifier: &str) {
        self.subscribe_as(session, &session.get_id(), stream, identifier);
    }

    /// [`Gate::subscribe`] with the session id already known (the hub calls it
    /// while holding the session's subscription list, where session methods must
    /// not be called).
    fn subscribe_as(&self, session: &Arc<S>, sid: &str, stream: &str, identifier: &str) {
        let key = session_key(session);

        let _garbage: Option<Arc<Snapshot<S>>>;
        let mut streams = write(&self.shared.streams);
        if !streams.contains_key(stream) {
            streams.insert(stream.to_owned(), StreamSubscribers::new());
        }
        let subs = streams.get_mut(stream).expect("stream entry");
        let sub = subs.sessions.entry(key).or_insert_with(|| Subscriber {
            session: Arc::clone(session),
            sid: sid.into(),
            identifiers: Vec::with_capacity(1),
        });
        if !sub.identifiers.iter().any(|id| **id == *identifier) {
            sub.identifiers.push(identifier.into());
            _garbage = subs.snapshot.take();
        }
    }

    pub fn unsubscribe(&self, session: &Arc<S>, stream: &str, identifier: &str) {
        let key = session_key(session);
        let garbage = self.remove(key, stream, identifier);
        drop(garbage);
    }

    fn remove(&self, key: usize, stream: &str, identifier: &str) -> Garbage<S> {
        let mut streams = write(&self.shared.streams);
        let Some(subs) = streams.get_mut(stream) else { return (None, None, None) };
        let Some(sub) = subs.sessions.get_mut(&key) else { return (None, None, None) };
        let Some(pos) = sub.identifiers.iter().position(|id| **id == *identifier) else { return (None, None, None) };

        sub.identifiers.swap_remove(pos);
        let removed = if sub.identifiers.is_empty() { subs.sessions.remove(&key) } else { None };
        let snapshot = subs.snapshot.take();
        let stream_entry = if subs.sessions.is_empty() { streams.remove(stream) } else { None };
        (removed, stream_entry, snapshot)
    }

    /// Number of unique streams.
    pub fn size(&self) -> usize {
        read(&self.shared.streams).len()
    }

    /// Number of sessions with at least one subscription in this gate (upstream
    /// tests read the unexported `sessionsStreams` map, which upstream declares
    /// but never fills; this is the index it describes).
    pub fn sessions_streams_size(&self) -> usize {
        let streams = read(&self.shared.streams);
        streams.values().flat_map(|s| s.sessions.keys()).collect::<HashSet<_>>().len()
    }
}

// ----------------------------------------------------------------- hub ---

#[derive(Default)]
struct SessionStreams {
    /// Set once the session is unregistered; later subscriptions are ignored.
    removed: bool,
    /// Stream-identifier pairs, in subscription order (upstream `HubSessionInfo.streams`).
    list: Vec<(String, String)>,
}

/// A registered session (upstream `HubSessionInfo`).
struct SessionInfo<S: ?Sized> {
    session: Arc<S>,
    /// The connection identifiers it is registered with.
    identifiers: Arc<str>,
    streams: Mutex<SessionStreams>,
}

struct HubState<S: ?Sized> {
    /// Keyed as in gates: several sessions may have the same id (D-077).
    sessions: BySession<Arc<SessionInfo<S>>>,
    /// Connection identifiers -> session keys.
    identifiers: HashMap<Arc<str>, HashSet<usize, BuildHasherDefault<AddrHasher>>>,
}

impl<S: ?Sized> HubState<S> {
    /// Registers the session's entry, returning the one it replaces.
    fn insert(&mut self, key: usize, info: Arc<SessionInfo<S>>) -> Option<Arc<SessionInfo<S>>> {
        let replaced = self.remove(key);
        self.identifiers.entry(Arc::clone(&info.identifiers)).or_default().insert(key);
        self.sessions.insert(key, info);
        replaced
    }

    /// Unregisters the session's entry.
    fn remove(&mut self, key: usize) -> Option<Arc<SessionInfo<S>>> {
        let info = self.sessions.remove(&key)?;
        if let Some(keys) = self.identifiers.get_mut(&info.identifiers) {
            keys.remove(&key);
            if keys.is_empty() {
                self.identifiers.remove(&info.identifiers);
            }
        }
        Some(info)
    }
}

#[derive(Default)]
struct Lifecycle {
    shutdown: bool,
    running: usize,
}

struct HubInner<S: ?Sized> {
    gates: Box<[Gate<S>]>,
    /// Upstream `l.With("component", "hub")`.
    log: Logger,
    ctx: GateContext,
    state: RwLock<HubState<S>>,
    pool: GoPool,
    lifecycle: Mutex<Lifecycle>,
    lifecycle_changed: Condvar,
}

impl<S: ?Sized + HubSession> HubInner<S> {
    fn gate(&self, stream: &str) -> &Gate<S> {
        &self.gates[gate_index(stream, self.gates.len())]
    }

    fn info(&self, session: &Arc<S>) -> Option<Arc<SessionInfo<S>>> {
        read(&self.state).sessions.get(&session_key(session)).cloned()
    }

    /// Upstream `disconnectSessions`' scheduled task: disconnects every session
    /// registered with `identifier`, as AnyCable 1.6.22 does (1.6.21 disconnected
    /// the first one it found). The sessions are collected under the lock and disconnected after
    /// it is released, each with the same message, so a session may unregister
    /// itself while it disconnects.
    fn disconnect_sessions(&self, identifier: &str, reconnect: bool) {
        let sessions: Vec<Arc<S>> = {
            let state = read(&self.state);
            let keys = state.identifiers.get(identifier).into_iter().flatten();
            keys.filter_map(|key| state.sessions.get(key)).map(|info| Arc::clone(&info.session)).collect()
        };
        if sessions.is_empty() {
            return;
        }
        let msg = CachedEncodedMessage::new(common::EncodedMessage::Disconnect(DisconnectMessage {
            kind: common::DISCONNECT_TYPE.to_owned(),
            reason: common::REMOTE_DISCONNECT_REASON.to_owned(),
            reconnect,
        }));
        for session in &sessions {
            // A panicking session must not keep the identifier's other sessions
            // connected (as D-013 for broadcasts).
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                session.disconnect_with_message(&msg, common::REMOTE_DISCONNECT_REASON);
            }));
        }
    }
}

/// Stores all sessions and their subscriptions; broadcasts are routed through
/// `pool_size` gates (shards) (upstream `hub.Hub`). See the module docs.
pub struct Hub<S: ?Sized = dyn HubSession> {
    inner: Arc<HubInner<S>>,
}

impl<S: ?Sized + HubSession + 'static> Hub<S> {
    /// Upstream `hub.NewHub(poolSize, logger)`. Starts the gates; `pool_size` 0 is
    /// treated as 1 (upstream would divide by zero on the first broadcast).
    pub fn new(pool_size: usize) -> Self {
        Self::with_logger(pool_size, &Logger::default())
    }

    /// Upstream `hub.NewHub(poolSize, logger)` with the caller's logger (the
    /// node passes its own): the hub logs with `component=hub`, gates also with
    /// `gate=<index>`.
    pub fn with_logger(pool_size: usize, log: &Logger) -> Self {
        let ctx = GateContext::new();
        let log = log.with("component", "hub");
        let gates = (0..pool_size.max(1)).map(|i| Gate::with_logger(&ctx, log.with("gate", i))).collect();
        Self {
            inner: Arc::new(HubInner {
                gates,
                log,
                ctx,
                state: RwLock::new(HubState { sessions: BySession::default(), identifiers: HashMap::new() }),
                // Created eagerly, as upstream: it is listed by `utils::all_pools()`
                // (the runner's "Go pools initialized" log line).
                pool: GoPool::new("remote commands", REMOTE_COMMANDS_POOL_SIZE),
                lifecycle: Mutex::new(Lifecycle::default()),
                lifecycle_changed: Condvar::new(),
            }),
        }
    }

    /// Enqueues a remote disconnect command: every session registered with
    /// `msg.identifier` receives a `remote` disconnect message, on the
    /// remote-commands pool.
    pub fn remote_disconnect(&self, msg: RemoteDisconnectMessage) {
        if !read(&self.inner.state).identifiers.contains_key(msg.identifier.as_str()) {
            log_debug!(self.inner.log, "cannot disconnect session",
                "identifier" => &msg.identifier, "reason" => "not found");
            return;
        }
        let inner = Arc::clone(&self.inner);
        self.inner.pool.schedule(Box::new(move || inner.disconnect_sessions(&msg.identifier, msg.reconnect)));
    }
}

impl<S: ?Sized + HubSession> Hub<S> {
    /// Blocks until [`Hub::shutdown`] (upstream `go hub.Run()`). Broadcasts do not
    /// depend on it: they are routed to the gates by the caller.
    pub fn run(&self) {
        let mut life = lock(&self.inner.lifecycle);
        if life.shutdown {
            return;
        }
        life.running += 1;
        while !life.shutdown {
            life = self.inner.lifecycle_changed.wait(life).unwrap_or_else(PoisonError::into_inner);
        }
        life.running -= 1;
        self.inner.lifecycle_changed.notify_all();
    }

    /// Stops the gates and [`Hub::run`], and waits for `run` to return.
    pub fn shutdown(&self) {
        self.inner.ctx.cancel();
        let mut life = lock(&self.inner.lifecycle);
        life.shutdown = true;
        self.inner.lifecycle_changed.notify_all();
        while life.running > 0 {
            life = self.inner.lifecycle_changed.wait(life).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Enqueues `data` for every subscriber of `stream`.
    pub fn broadcast(&self, stream: &str, data: &str) {
        self.broadcast_message(StreamMessage {
            stream: stream.to_owned(),
            data: data.to_owned(),
            ..Default::default()
        });
    }

    /// Enqueues a pre-built stream message.
    pub fn broadcast_message(&self, msg: StreamMessage) {
        self.inner.gate(&msg.stream).broadcast(msg);
    }

    /// Number of registered sessions.
    pub fn size(&self) -> usize {
        read(&self.inner.state).sessions.len()
    }

    /// Number of unique connection identifiers.
    pub fn uniq_size(&self) -> usize {
        read(&self.inner.state).identifiers.len()
    }

    /// Number of unique streams across gates.
    pub fn streams_size(&self) -> usize {
        self.inner.gates.iter().map(Gate::size).sum()
    }

    /// Registers a session.
    pub fn add_session(&self, session: Arc<S>) {
        let uid = session.get_id();
        let identifiers: Arc<str> = session.get_identifiers().into();
        let key = session_key(&session);
        let info = Arc::new(SessionInfo {
            session,
            identifiers: Arc::clone(&identifiers),
            streams: Mutex::new(SessionStreams::default()),
        });

        // Dropped outside the lock.
        let replaced = write(&self.inner.state).insert(key, info);
        drop(replaced);

        log_debug!(self.inner.log, "registered", "sid" => uid, "ids" => &*identifiers);
    }

    /// Unsubscribes the session from all its streams and unregisters it.
    pub fn remove_session(&self, session: &Arc<S>) {
        let uid = session.get_id();
        let removed = write(&self.inner.state).remove(session_key(session));
        let Some(info) = removed else {
            log_warn!(self.inner.log, "session hasn't been registered", "sid" => uid);
            return;
        };

        let streams = {
            let mut streams = lock(&info.streams);
            streams.removed = true;
            std::mem::take(&mut streams.list)
        };
        for (stream, identifier) in &streams {
            self.inner.gate(stream).unsubscribe(session, stream, identifier);
        }

        log_debug!(self.inner.log, "unregistered", "sid" => uid);
    }

    /// Upstream unexported `unsubscribeSessionFromAllChannels`: removes the gate
    /// subscriptions but keeps the session's bookkeeping, as upstream.
    pub fn unsubscribe_session_from_all_channels(&self, session: &Arc<S>) {
        let Some(info) = self.inner.info(session) else { return };
        let streams = lock(&info.streams);
        for (stream, identifier) in &streams.list {
            self.inner.gate(stream).unsubscribe(session, stream, identifier);
        }
    }

    /// Removes every stream subscribed under the channel `identifier`.
    pub fn unsubscribe_session_from_channel(&self, session: &Arc<S>, identifier: &str) {
        let sid = session.get_id();
        if let Some(info) = self.inner.info(session) {
            let mut streams = lock(&info.streams);
            streams.list.retain(|(stream, id)| {
                if id != identifier {
                    return true;
                }
                self.inner.gate(stream).unsubscribe(session, stream, id);
                false
            });
        }

        log_debug!(self.inner.log, "unsubscribed", "sid" => sid, "identifier" => filtered_identifier(identifier).log_value());
    }

    /// Subscribes a registered session to `stream` under the channel `identifier`.
    /// Unregistered sessions are ignored (upstream logs an error).
    pub fn subscribe_session(&self, session: &Arc<S>, stream: &str, identifier: &str) {
        let sid = session.get_id();
        let registered = self.inner.info(session).is_some_and(|info| {
            let mut streams = lock(&info.streams);
            if streams.removed {
                return false;
            }
            self.inner.gate(stream).subscribe_as(session, &sid, stream, identifier);
            streams.list.push((stream.to_owned(), identifier.to_owned()));
            true
        });

        if !registered {
            log_error!(self.inner.log, "attempted to subscribe a session without registration", "sid" => sid);
            return;
        }

        log_debug!(self.inner.log, "subscribed",
            "sid" => sid, "identifier" => filtered_identifier(identifier).log_value(), "stream" => stream);
    }

    pub fn unsubscribe_session(&self, session: &Arc<S>, stream: &str, identifier: &str) {
        self.inner.gate(stream).unsubscribe(session, stream, identifier);

        let sid = session.get_id();
        if let Some(info) = self.inner.info(session) {
            let mut streams = lock(&info.streams);
            if let Some(pos) = streams.list.iter().position(|(s, id)| s == stream && id == identifier) {
                streams.list.remove(pos);
            }
        }

        log_debug!(self.inner.log, "unsubscribed",
            "sid" => sid, "identifier" => filtered_identifier(identifier).log_value(), "stream" => stream);
    }

    /// A session registered with the connection `identifier`, if any.
    pub fn find_by_identifier(&self, identifier: &str) -> Option<Arc<S>> {
        let state = read(&self.inner.state);
        let keys = state.identifiers.get(identifier)?;
        keys.iter().find_map(|key| state.sessions.get(key)).map(|info| Arc::clone(&info.session))
    }

    /// All registered sessions.
    pub fn sessions(&self) -> Vec<Arc<S>> {
        read(&self.inner.state).sessions.values().map(|info| Arc::clone(&info.session)).collect()
    }
}

impl<S: ?Sized> Drop for Hub<S> {
    fn drop(&mut self) {
        // Pending remote-command tasks may still hold the inner state; the gates
        // must stop with the hub regardless.
        self.inner.ctx.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_index_is_fnv1a_64() {
        // FNV-1a 64 of "test" is 0xf9e6e6ef197c2b25.
        assert_eq!((0xf9e6_e6ef_197c_2b25u64 % 10) as usize, gate_index("test", 10));
        assert_eq!(0, gate_index("anything", 1));
    }

    struct Recorder {
        sid: String,
        frames: std::sync::mpsc::Sender<Vec<u8>>,
    }

    impl HubSession for Recorder {
        fn get_id(&self) -> String {
            self.sid.clone()
        }

        fn get_identifiers(&self) -> String {
            self.sid.clone()
        }

        fn send(&self, msg: &EncodedMessage) {
            let json = msg.to_json();
            assert!(!json.windows(5).any(|w| w == b"panic"), "session failure");
            let _ = self.frames.send(json);
        }

        fn disconnect_with_message(&self, msg: &EncodedMessage, _code: &str) {
            self.send(msg);
        }
    }

    /// D-013: a panic in `HubSession::send` does not stop the gate.
    #[test]
    fn deviation_d013_gate_survives_a_panicking_session() {
        let hub: Hub = Hub::new(1);
        let (tx, rx) = std::sync::mpsc::channel();
        let session: SessionRef = Arc::new(Recorder { sid: "1".into(), frames: tx });
        hub.add_session(Arc::clone(&session));
        hub.subscribe_session(&session, "s", "c");

        hub.broadcast("s", "\"panic\"");
        hub.broadcast("s", "\"ok\"");

        let frame = rx.recv_timeout(std::time::Duration::from_secs(5)).expect("delivered after the panic");
        assert_eq!(b"{\"identifier\":\"c\",\"message\":\"ok\"}".to_vec(), frame);
        hub.shutdown();
    }

    /// Deviation D-077: sessions with the same id are registered, subscribed and
    /// unregistered independently.
    #[test]
    fn deviation_d077_sessions_with_the_same_id_are_independent() {
        let hub: Hub = Hub::new(1);
        let connect = || {
            let (tx, rx) = std::sync::mpsc::channel();
            let session: SessionRef = Arc::new(Recorder { sid: "1".into(), frames: tx });
            hub.add_session(Arc::clone(&session));
            (session, rx)
        };
        let next = |rx: &std::sync::mpsc::Receiver<Vec<u8>>| {
            String::from_utf8(rx.recv_timeout(std::time::Duration::from_secs(5)).expect("a frame")).unwrap()
        };

        let (a, a_frames) = connect();
        hub.subscribe_session(&a, "a", "c");
        let (b, b_frames) = connect();
        hub.subscribe_session(&a, "late", "c");
        hub.subscribe_session(&b, "b", "c");
        assert_eq!((2, 1, 3), (hub.size(), hub.uniq_size(), hub.streams_size()));

        hub.remove_session(&a);
        assert_eq!((1, 1, 1), (hub.size(), hub.uniq_size(), hub.streams_size()));
        assert!(hub.find_by_identifier("1").is_some_and(|s| Arc::ptr_eq(&s, &b)));
        hub.subscribe_session(&b, "later", "c");
        hub.broadcast("a", "1");
        hub.broadcast("later", "2");
        assert_eq!(r#"{"identifier":"c","message":2}"#, next(&b_frames));
        assert!(a_frames.try_recv().is_err());

        hub.remove_session(&b);
        assert_eq!((0, 0, 0), (hub.size(), hub.uniq_size(), hub.streams_size()));
        hub.shutdown();
    }

    /// A session whose identifiers change between registration and removal.
    struct Renamed(Mutex<&'static str>);

    impl HubSession for Renamed {
        fn get_id(&self) -> String {
            "1".into()
        }

        fn get_identifiers(&self) -> String {
            lock(&self.0).to_string()
        }

        fn send(&self, _msg: &EncodedMessage) {}

        fn disconnect_with_message(&self, _msg: &EncodedMessage, _code: &str) {}
    }

    /// A session is unregistered from the identifiers it was registered with.
    #[test]
    fn registration_remembers_the_identifiers() {
        let hub: Hub = Hub::new(1);
        let renamed = Arc::new(Renamed(Mutex::new("a")));
        let session: SessionRef = renamed.clone();
        hub.add_session(Arc::clone(&session));
        *lock(&renamed.0) = "b";
        hub.remove_session(&session);
        assert_eq!((0, 0), (hub.size(), hub.uniq_size()));

        // Registered again with other identifiers: only those are kept.
        hub.add_session(Arc::clone(&session));
        *lock(&renamed.0) = "c";
        hub.add_session(Arc::clone(&session));
        assert_eq!((1, 1), (hub.size(), hub.uniq_size()));
        assert!(hub.find_by_identifier("b").is_none());
        assert!(hub.find_by_identifier("c").is_some());
        hub.remove_session(&session);
        assert_eq!((0, 0), (hub.size(), hub.uniq_size()));
        hub.shutdown();
    }

    /// A session for remote disconnects: records the disconnect frame it gets
    /// (with its name), closes, and unregisters itself from the hub while it
    /// disconnects, as a node's session does.
    struct Connection {
        name: &'static str,
        sid: &'static str,
        identifiers: &'static str,
        closed: AtomicBool,
        panics: bool,
        frames: FrameSender,
        hub: Weak<Hub>,
        me: Weak<Connection>,
    }

    impl HubSession for Connection {
        fn get_id(&self) -> String {
            self.sid.to_owned()
        }

        fn get_identifiers(&self) -> String {
            self.identifiers.to_owned()
        }

        fn send(&self, msg: &EncodedMessage) {
            let _ = self.frames.send((self.name, msg.to_json()));
        }

        fn disconnect_with_message(&self, msg: &EncodedMessage, code: &str) {
            assert_eq!(common::REMOTE_DISCONNECT_REASON, code);
            assert!(!self.panics, "session failure");
            if let (Some(hub), Some(me)) = (self.hub.upgrade(), self.me.upgrade()) {
                let me: SessionRef = me;
                hub.remove_session(&me);
            }
            self.closed.store(true, Ordering::SeqCst);
            self.send(msg);
        }
    }

    /// A frame and the name of the session it was sent to.
    type Frame = (&'static str, Vec<u8>);
    type FrameSender = std::sync::mpsc::Sender<Frame>;
    type Frames = std::sync::mpsc::Receiver<Frame>;

    /// A hub (shared with its sessions) and the channel their frames arrive on.
    fn remote_hub() -> (Arc<Hub>, FrameSender, Frames) {
        let (tx, rx) = std::sync::mpsc::channel();
        (Arc::new(Hub::new(1)), tx, rx)
    }

    fn connection(
        hub: &Arc<Hub>,
        frames: &FrameSender,
        name: &'static str,
        sid: &'static str,
        identifiers: &'static str,
        panics: bool,
    ) -> Arc<Connection> {
        let conn = Arc::new_cyclic(|me| Connection {
            name,
            sid,
            identifiers,
            closed: AtomicBool::new(false),
            panics,
            frames: frames.clone(),
            hub: Arc::downgrade(hub),
            me: me.clone(),
        });
        let session: SessionRef = conn.clone();
        hub.add_session(Arc::clone(&session));
        hub.subscribe_session(&session, &format!("stream-{name}"), "c");
        conn
    }

    /// The names of the sessions that received `n` frames, sorted; every frame is
    /// the remote disconnect message.
    fn disconnected(frames: &Frames, n: usize, reconnect: bool) -> Vec<&'static str> {
        let want = format!(r#"{{"type":"disconnect","reason":"remote","reconnect":{reconnect}}}"#);
        let mut names: Vec<_> = (0..n)
            .map(|_| {
                let (name, frame) = frames.recv_timeout(std::time::Duration::from_secs(5)).expect("a disconnect frame");
                assert_eq!(want, String::from_utf8(frame).unwrap(), "frame of {name}");
                name
            })
            .collect();
        names.sort_unstable();
        names
    }

    /// A remote disconnect closes every session registered with the identifier
    /// (two of them), as in AnyCable 1.6.22; a session with other identifiers
    /// stays connected.
    /// Each session unregisters itself while it disconnects, so the hub's lock is
    /// not held then (the frames arrive within a deadline instead of deadlocking).
    #[test]
    fn remote_disconnect_closes_every_session_of_the_identifier() {
        let (hub, tx, frames) = remote_hub();
        let a = connection(&hub, &tx, "a", "1", "user-1", false);
        let b = connection(&hub, &tx, "b", "2", "user-1", false);
        let other = connection(&hub, &tx, "other", "3", "user-2", false);

        hub.remote_disconnect(RemoteDisconnectMessage { identifier: "user-1".into(), reconnect: true });

        assert_eq!(vec!["a", "b"], disconnected(&frames, 2, true));
        assert!(a.closed.load(Ordering::SeqCst) && b.closed.load(Ordering::SeqCst));
        assert!(!other.closed.load(Ordering::SeqCst));
        assert_eq!((1, 1, 1), (hub.size(), hub.uniq_size(), hub.streams_size()), "only the other session is left");

        // The other session is still registered and subscribed.
        hub.broadcast("stream-other", "1");
        let (name, frame) = frames.recv_timeout(std::time::Duration::from_secs(5)).expect("a broadcast");
        assert_eq!(("other", r#"{"identifier":"c","message":1}"#), (name, String::from_utf8(frame).unwrap().as_str()));
        assert!(frames.try_recv().is_err());
        hub.shutdown();
    }

    /// Three sessions of one identifier, two of which share a session id (D-077):
    /// each of them gets the same message and is closed.
    #[test]
    fn deviation_d077_three_sessions_of_one_identifier_are_closed() {
        let (hub, tx, frames) = remote_hub();
        let conns =
            [("a", "1"), ("b", "1"), ("c", "2")].map(|(name, sid)| connection(&hub, &tx, name, sid, "user-1", false));
        let other = connection(&hub, &tx, "other", "1", "user-2", false);
        assert_eq!(4, hub.size());

        hub.remote_disconnect(RemoteDisconnectMessage { identifier: "user-1".into(), reconnect: false });

        assert_eq!(vec!["a", "b", "c"], disconnected(&frames, 3, false));
        assert!(conns.iter().all(|c| c.closed.load(Ordering::SeqCst)));
        assert!(!other.closed.load(Ordering::SeqCst));
        assert_eq!(1, hub.size());
        assert!(hub.find_by_identifier("user-1").is_none());
        assert!(hub.find_by_identifier("user-2").is_some_and(|s| s.get_id() == "1"));
        assert!(frames.try_recv().is_err());
        hub.shutdown();
    }

    /// A remote disconnect for an identifier no session has changes nothing.
    #[test]
    fn remote_disconnect_of_an_unknown_identifier_is_a_no_op() {
        let (hub, tx, frames) = remote_hub();
        let a = connection(&hub, &tx, "a", "1", "user-1", false);

        hub.inner.disconnect_sessions("user-2", true);
        hub.inner.disconnect_sessions("user", true);
        hub.inner.disconnect_sessions("", true);
        hub.remote_disconnect(RemoteDisconnectMessage { identifier: "user-2".into(), reconnect: true });

        assert!(frames.try_recv().is_err());
        assert!(!a.closed.load(Ordering::SeqCst));
        assert_eq!((1, 1, 1), (hub.size(), hub.uniq_size(), hub.streams_size()));
        hub.shutdown();
    }

    /// A session that panics while it disconnects does not keep the other
    /// sessions of the identifier connected (the pool task survives, D-009).
    #[test]
    fn deviation_d009_a_panicking_session_does_not_stop_the_others() {
        let (hub, tx, frames) = remote_hub();
        let failing = connection(&hub, &tx, "failing", "1", "user-1", true);
        let a = connection(&hub, &tx, "a", "2", "user-1", false);
        let b = connection(&hub, &tx, "b", "3", "user-1", false);

        hub.remote_disconnect(RemoteDisconnectMessage { identifier: "user-1".into(), reconnect: true });

        assert_eq!(vec!["a", "b"], disconnected(&frames, 2, true));
        assert!(a.closed.load(Ordering::SeqCst) && b.closed.load(Ordering::SeqCst));
        assert!(!failing.closed.load(Ordering::SeqCst));
        assert_eq!(1, hub.size(), "the failing session is still registered");
        hub.shutdown();
    }

    #[test]
    fn cached_message_encodes_once_per_encoder() {
        let msg = build_message(&StreamMessage { stream: "s".into(), data: "1".into(), ..Default::default() }, "c");
        let calls = std::cell::Cell::new(0);
        let encode = |m: &common::EncodedMessage| {
            calls.set(calls.get() + 1);
            Ok(Some(crate::encoders::SentFrame::text(m.to_json())))
        };
        let a = msg.fetch("json", encode).expect("frame");
        let b = msg.fetch("json", encode).expect("frame");
        assert!(Arc::ptr_eq(&a, &b));
        let _ = msg.fetch("other", encode).expect("frame");
        let _ = msg.fetch("other", encode).expect("frame");
        assert_eq!(2, calls.get());
        assert_eq!(b"{\"identifier\":\"c\",\"message\":1}".to_vec(), a.payload);
    }
}
