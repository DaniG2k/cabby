//! Facade for upstream package `hub`.
//!
//! [`MockSession`] is the translation of upstream's `MockSession`
//! (hub/hub_test.go): it queues every frame it is sent (JSON-encoded) and
//! [`MockSession::read`] waits up to 100 ms for the next one. On the Go backend
//! the hub, gates and sessions live in the bridge (`bridge/hub.go`).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cabby_core::common::{RemoteDisconnectMessage, StreamMessage};
use cabby_core::hub::{self as rs, EncodedMessage, HubSession, SessionRef};
use serde_json::json;

use super::msg_shapes::{GoParsed, stream_message_json};
use crate::{Backend, backend, bridge};

/// Upstream `MockSession.Read` waits this long (`time.After(100 * time.Millisecond)`).
pub const READ_TIMEOUT: Duration = Duration::from_millis(100);

/// Error text of a read that timed out (upstream message).
pub const NO_MESSAGES: &str = "Session hasn't received any messages";

// ------------------------------------------------------------ sessions ---

struct RustSession {
    sid: String,
    frames: Mutex<VecDeque<Vec<u8>>>,
    ready: Condvar,
    closed: Mutex<bool>,
}

impl RustSession {
    fn push(&self, payload: Vec<u8>) {
        self.frames.lock().unwrap_or_else(|e| e.into_inner()).push_back(payload);
        self.ready.notify_all();
    }

    fn pop(&self, timeout: Duration) -> Option<Vec<u8>> {
        let guard = self.frames.lock().unwrap_or_else(|e| e.into_inner());
        let (mut q, _) =
            self.ready.wait_timeout_while(guard, timeout, |q| q.is_empty()).unwrap_or_else(|e| e.into_inner());
        q.pop_front()
    }

    fn drain(&self, n: usize, deadline: Instant) -> Vec<Vec<u8>> {
        let guard = self.frames.lock().unwrap_or_else(|e| e.into_inner());
        let left = deadline.saturating_duration_since(Instant::now());
        let (mut q, _) = self.ready.wait_timeout_while(guard, left, |q| q.len() < n).unwrap_or_else(|e| e.into_inner());
        q.drain(..).collect()
    }
}

impl HubSession for RustSession {
    fn get_id(&self) -> String {
        self.sid.clone()
    }

    fn get_identifiers(&self) -> String {
        self.sid.clone()
    }

    fn send(&self, msg: &EncodedMessage) {
        self.push(msg.to_json());
    }

    fn disconnect_with_message(&self, msg: &EncodedMessage, _code: &str) {
        let mut closed = self.closed.lock().unwrap_or_else(|e| e.into_inner());
        if *closed {
            return;
        }
        self.send(msg);
        *closed = true;
    }
}

enum SessionImpl {
    Rust(Arc<RustSession>),
    Go(i64),
}

/// Upstream `NewMockSession(sid)`: identifiers equal the session id.
pub struct MockSession {
    inner: SessionImpl,
}

impl MockSession {
    pub fn new(sid: &str) -> Self {
        match backend() {
            Backend::Rust => Self::rust(sid),
            Backend::Go => Self { inner: SessionImpl::Go(bridge::call("hub.NewSession", json!({"Sid": sid}))) },
        }
    }

    /// Many sessions in one bridge round trip (benchmark configurations).
    pub fn new_many(sids: &[String]) -> Vec<Self> {
        match backend() {
            Backend::Rust => sids.iter().map(|s| Self::rust(s)).collect(),
            Backend::Go => {
                let ids: Vec<i64> = bridge::call("hub.NewSessions", json!({"Sids": sids}));
                ids.into_iter().map(|id| Self { inner: SessionImpl::Go(id) }).collect()
            }
        }
    }

    fn rust(sid: &str) -> Self {
        Self {
            inner: SessionImpl::Rust(Arc::new(RustSession {
                sid: sid.to_owned(),
                frames: Mutex::new(VecDeque::new()),
                ready: Condvar::new(),
                closed: Mutex::new(false),
            })),
        }
    }

    fn rust_ref(&self) -> SessionRef {
        match &self.inner {
            SessionImpl::Rust(s) => Arc::clone(s) as SessionRef,
            SessionImpl::Go(_) => unreachable!("Go session on the Rust backend"),
        }
    }

    fn go_id(&self) -> i64 {
        match &self.inner {
            SessionImpl::Go(id) => *id,
            SessionImpl::Rust(_) => unreachable!("Rust session on the Go backend"),
        }
    }

    /// Next frame, or [`NO_MESSAGES`] after [`READ_TIMEOUT`] (upstream `Read()`).
    pub fn read(&self) -> Result<String, String> {
        match &self.inner {
            SessionImpl::Rust(s) => s
                .pop(READ_TIMEOUT)
                .map(|f| String::from_utf8(f).expect("utf-8 frame"))
                .ok_or_else(|| NO_MESSAGES.into()),
            SessionImpl::Go(id) => {
                let p: GoParsed<String> = bridge::call(
                    "hub.SessionRead",
                    json!({"Session": id, "TimeoutMs": READ_TIMEOUT.as_millis() as u64}),
                );
                p.into_result().map(|r| r.expect("frame"))
            }
        }
    }

    pub fn closed(&self) -> bool {
        match &self.inner {
            SessionImpl::Rust(s) => *s.closed.lock().unwrap_or_else(|e| e.into_inner()),
            SessionImpl::Go(id) => bridge::call("hub.SessionClosed", json!({"Session": id})),
        }
    }
}

/// For each session, waits (one shared deadline) until it has received at least
/// `expected[i]` frames, then takes all of its frames and counts them by content.
pub fn drain_sessions(sessions: &[MockSession], expected: &[usize], timeout: Duration) -> Vec<HashMap<String, usize>> {
    match backend() {
        Backend::Rust => {
            let deadline = Instant::now() + timeout;
            sessions
                .iter()
                .zip(expected)
                .map(|(s, n)| {
                    let SessionImpl::Rust(rs) = &s.inner else { unreachable!() };
                    let mut counts = HashMap::new();
                    for f in rs.drain(*n, deadline) {
                        *counts.entry(String::from_utf8(f).expect("utf-8 frame")).or_default() += 1;
                    }
                    counts
                })
                .collect()
        }
        Backend::Go => {
            let ids: Vec<i64> = sessions.iter().map(MockSession::go_id).collect();
            bridge::call(
                "hub.DrainSessions",
                json!({"Sessions": ids, "Expected": expected, "TimeoutMs": timeout.as_millis() as u64}),
            )
        }
    }
}

// ----------------------------------------------------------------- hub ---

enum HubImpl {
    Rust { hub: Arc<rs::Hub>, runner: Option<JoinHandle<()>> },
    Go(i64),
}

/// A running hub: upstream `hub := NewHub(poolSize, ...); go hub.Run()`.
/// Dropping it calls `hub.Shutdown()` (upstream `defer hub.Shutdown()`).
pub struct Hub {
    inner: HubImpl,
}

impl Hub {
    pub fn start(pool_size: usize) -> Self {
        match backend() {
            Backend::Rust => {
                let hub = Arc::new(rs::Hub::new(pool_size));
                let h = Arc::clone(&hub);
                let runner = std::thread::spawn(move || h.run());
                Self { inner: HubImpl::Rust { hub, runner: Some(runner) } }
            }
            Backend::Go => {
                let id: i64 = bridge::call("hub.NewHub", json!({"PoolSize": pool_size}));
                let _: bool = bridge::call("hub.Run", json!({"Hub": id}));
                Self { inner: HubImpl::Go(id) }
            }
        }
    }

    fn go_session(&self, op: &str, s: &MockSession, stream: &str, identifier: &str) {
        let HubImpl::Go(id) = &self.inner else { unreachable!() };
        let _: bool =
            bridge::call(op, json!({"Hub": id, "Session": s.go_id(), "Stream": stream, "Identifier": identifier}));
    }

    pub fn add_session(&self, s: &MockSession) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.add_session(s.rust_ref()),
            HubImpl::Go(_) => self.go_session("hub.AddSession", s, "", ""),
        }
    }

    pub fn remove_session(&self, s: &MockSession) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.remove_session(&s.rust_ref()),
            HubImpl::Go(_) => self.go_session("hub.RemoveSession", s, "", ""),
        }
    }

    pub fn subscribe_session(&self, s: &MockSession, stream: &str, identifier: &str) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.subscribe_session(&s.rust_ref(), stream, identifier),
            HubImpl::Go(_) => self.go_session("hub.SubscribeSession", s, stream, identifier),
        }
    }

    pub fn unsubscribe_session(&self, s: &MockSession, stream: &str, identifier: &str) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.unsubscribe_session(&s.rust_ref(), stream, identifier),
            HubImpl::Go(_) => self.go_session("hub.UnsubscribeSession", s, stream, identifier),
        }
    }

    pub fn unsubscribe_session_from_channel(&self, s: &MockSession, identifier: &str) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.unsubscribe_session_from_channel(&s.rust_ref(), identifier),
            HubImpl::Go(_) => self.go_session("hub.UnsubscribeSessionFromChannel", s, "", identifier),
        }
    }

    /// Upstream unexported `unsubscribeSessionFromAllChannels`.
    pub fn unsubscribe_session_from_all_channels(&self, s: &MockSession) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.unsubscribe_session_from_all_channels(&s.rust_ref()),
            // Unexported upstream. The exported, synchronous `RemoveSession` calls it
            // and then only deletes the hub's sessions/identifiers entries, which gate
            // delivery never consults: subsequent broadcasts observe the same effect.
            HubImpl::Go(_) => self.go_session("hub.RemoveSession", s, "", ""),
        }
    }

    pub fn broadcast(&self, stream: &str, data: &str) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.broadcast(stream, data),
            HubImpl::Go(id) => {
                let _: bool = bridge::call("hub.Broadcast", json!({"Hub": id, "Stream": stream, "Data": data}));
            }
        }
    }

    /// `broadcast(stream, data)` for each item, in order.
    pub fn broadcast_many(&self, items: &[(String, String)]) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => items.iter().for_each(|(s, d)| hub.broadcast(s, d)),
            HubImpl::Go(id) => {
                let items: Vec<_> = items.iter().map(|(s, d)| json!({"Stream": s, "Data": d})).collect();
                let _: bool = bridge::call("hub.BroadcastMany", json!({"Hub": id, "Items": items}));
            }
        }
    }

    /// `add_session` for each session, in order.
    pub fn add_sessions(&self, sessions: &[MockSession]) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => sessions.iter().for_each(|s| hub.add_session(s.rust_ref())),
            HubImpl::Go(id) => {
                let ids: Vec<i64> = sessions.iter().map(MockSession::go_id).collect();
                let _: bool = bridge::call("hub.AddSessions", json!({"Hub": id, "Sessions": ids}));
            }
        }
    }

    /// `subscribe_session(sessions[i], stream, identifier)` for each item, in order.
    pub fn subscribe_sessions(&self, sessions: &[MockSession], subs: &[(usize, String, String)]) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => {
                subs.iter().for_each(|(i, st, id)| hub.subscribe_session(&sessions[*i].rust_ref(), st, id))
            }
            HubImpl::Go(id) => {
                let subs: Vec<_> = subs
                    .iter()
                    .map(|(i, st, ident)| json!({"Session": sessions[*i].go_id(), "Stream": st, "Identifier": ident}))
                    .collect();
                let _: bool = bridge::call("hub.SubscribeSessions", json!({"Hub": id, "Subs": subs}));
            }
        }
    }

    pub fn broadcast_message(&self, msg: StreamMessage) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.broadcast_message(msg),
            HubImpl::Go(id) => {
                let _: bool =
                    bridge::call("hub.BroadcastMessage", json!({"Hub": id, "Msg": stream_message_json(&msg)}));
            }
        }
    }

    pub fn remote_disconnect(&self, msg: RemoteDisconnectMessage) {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.remote_disconnect(msg),
            HubImpl::Go(id) => {
                let _: bool = bridge::call(
                    "hub.RemoteDisconnect",
                    json!({"Hub": id, "Msg": {"identifier": msg.identifier, "reconnect": msg.reconnect}}),
                );
            }
        }
    }

    pub fn size(&self) -> usize {
        match &self.inner {
            HubImpl::Rust { hub, .. } => hub.size(),
            HubImpl::Go(id) => bridge::call("hub.Size", json!({"Hub": id})),
        }
    }
}

impl Drop for Hub {
    fn drop(&mut self) {
        match &mut self.inner {
            HubImpl::Rust { hub, runner } => {
                hub.shutdown();
                if let Some(r) = runner.take() {
                    let _ = r.join();
                }
            }
            HubImpl::Go(id) => {
                let _: bool = bridge::call("hub.Shutdown", json!({"Hub": id}));
                let _: bool = bridge::call("msg.Release", json!({"Handles": [id]}));
            }
        }
    }
}

// ---------------------------------------------------------------- gate ---

enum GateImpl {
    Rust { gate: rs::Gate, ctx: rs::GateContext },
    Go(i64),
}

/// Upstream `NewGate(ctx, ...)` with a cancellable context owned by the facade.
pub struct Gate {
    inner: GateImpl,
}

impl Gate {
    pub fn new() -> Self {
        match backend() {
            Backend::Rust => {
                let ctx = rs::GateContext::new();
                Self { inner: GateImpl::Rust { gate: rs::Gate::new(&ctx), ctx } }
            }
            Backend::Go => Self { inner: GateImpl::Go(bridge::call("hub.NewGate", json!({}))) },
        }
    }

    /// Cancels the gate's context (upstream: the context's deadline expires).
    pub fn cancel(&self) {
        match &self.inner {
            GateImpl::Rust { ctx, .. } => ctx.cancel(),
            GateImpl::Go(id) => {
                let _: bool = bridge::call("hub.CancelGate", json!({"Gate": id}));
            }
        }
    }

    fn go(&self, op: &str, s: &MockSession, stream: &str, identifier: &str) {
        let GateImpl::Go(id) = &self.inner else { unreachable!() };
        let _: bool =
            bridge::call(op, json!({"Gate": id, "Session": s.go_id(), "Stream": stream, "Identifier": identifier}));
    }

    pub fn subscribe(&self, s: &MockSession, stream: &str, identifier: &str) {
        match &self.inner {
            GateImpl::Rust { gate, .. } => gate.subscribe(&s.rust_ref(), stream, identifier),
            GateImpl::Go(_) => self.go("hub.GateSubscribe", s, stream, identifier),
        }
    }

    pub fn unsubscribe(&self, s: &MockSession, stream: &str, identifier: &str) {
        match &self.inner {
            GateImpl::Rust { gate, .. } => gate.unsubscribe(&s.rust_ref(), stream, identifier),
            GateImpl::Go(_) => self.go("hub.GateUnsubscribe", s, stream, identifier),
        }
    }

    pub fn size(&self) -> usize {
        match &self.inner {
            GateImpl::Rust { gate, .. } => gate.size(),
            GateImpl::Go(id) => bridge::call("hub.GateSize", json!({"Gate": id})),
        }
    }

    pub fn broadcast(&self, msg: StreamMessage) {
        match &self.inner {
            GateImpl::Rust { gate, .. } => gate.broadcast(msg),
            GateImpl::Go(id) => {
                let _: bool = bridge::call("hub.GateBroadcast", json!({"Gate": id, "Msg": stream_message_json(&msg)}));
            }
        }
    }

    /// `len(gate.sessionsStreams)`.
    pub fn sessions_streams_size(&self) -> usize {
        match &self.inner {
            GateImpl::Rust { gate, .. } => gate.sessions_streams_size(),
            GateImpl::Go(_) => unimplemented!(
                "not_observable: Gate.sessionsStreams is unexported (and never written by the Go implementation)"
            ),
        }
    }
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

/// `toJSON(buildMessage(msg, identifier))`.
pub fn build_message_json(msg: &StreamMessage, identifier: &str) -> Vec<u8> {
    match backend() {
        Backend::Rust => rs::build_message(msg, identifier).to_json(),
        // buildMessage is unexported, but Gate.performBroadcast passes exactly
        // `buildMessage(msg, identifier)` to HubSession.Send; the bridge captures it.
        Backend::Go => {
            let json: String = bridge::call(
                "hub.BuildMessageJSON",
                json!({"Msg": stream_message_json(msg), "Identifier": identifier}),
            );
            json.into_bytes()
        }
    }
}
