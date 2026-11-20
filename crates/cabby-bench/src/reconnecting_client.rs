//! Browser-like reconnecting Action Cable client (upstream
//! `lib/reconnecting_client.go`).
//!
//! Each attempt dials `<url>?bench_sid=<n>`, waits for the welcome, subscribes to its
//! channels (all at once or sequentially), and ends on: missed server pings (the
//! watchdog starts at socket open), a subscription unconfirmed after all retries
//! (graceful close), reject, server disconnect, server close, or a [`ReconnectingClient::drop_connection`].
//! It then backs off and reconnects.
//!
//! A client is one task on the harness runtime ([`ReconnectingClient::run_async`]),
//! so a scenario can run tens of thousands of them.
//!
//! With [`Wire::Pusher`] the same life cycle runs over the Pusher protocol
//! ([`crate::pusher`]): the welcome is `pusher:connection_established`, channels are
//! public Pusher channels confirmed by `pusher_internal:subscription_succeeded`, a
//! subscription error rejects and `pusher:error` counts as a server disconnect. Pusher
//! servers do not all ping on a schedule, so the watchdog is client-driven, as in
//! pusher-js: any server message pushes the read deadline forward, the client sends
//! `pusher:ping` after [`ReconnectingClientConfig::keepalive`] without one, and it
//! answers the server's `pusher:ping` with `pusher:pong`. Probes send `pusher:ping` and
//! time the `pusher:pong`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::stream::{SplitSink, StreamExt};
use serde::Deserialize;
use tokio::time::Instant;
use tungstenite::Message as WsMessage;
use tungstenite::protocol::CloseFrame;
use tungstenite::protocol::frame::coding::CloseCode;

use crate::cancel::CancelToken;
use crate::histogram::Histogram;
use crate::net::{self, Ws};
use crate::pusher::{self, Wire};
use crate::rng::Rng;
use crate::runtime::block_on;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientState {
    /// Dialing or waiting for the welcome message.
    Connecting,
    /// Welcome received, waiting for confirmations.
    Subscribing,
    /// All subscriptions confirmed.
    Healthy,
    /// Waiting before the next attempt.
    Backoff,
}

impl ClientState {
    pub const ALL: [ClientState; 4] =
        [ClientState::Connecting, ClientState::Subscribing, ClientState::Healthy, ClientState::Backoff];

    pub fn as_str(self) -> &'static str {
        match self {
            ClientState::Connecting => "connecting",
            ClientState::Subscribing => "subscribing",
            ClientState::Healthy => "healthy",
            ClientState::Backoff => "backoff",
        }
    }
}

/// Why a connection attempt ended. [`CloseReason::ALL`] is the display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseReason {
    DialError,
    PingTimeout,
    SubscribeTimeout,
    Rejected,
    ServerDisconnect,
    ServerClosed,
    Dropped,
}

impl CloseReason {
    pub const ALL: [CloseReason; 7] = [
        CloseReason::DialError,
        CloseReason::PingTimeout,
        CloseReason::SubscribeTimeout,
        CloseReason::Rejected,
        CloseReason::ServerDisconnect,
        CloseReason::ServerClosed,
        CloseReason::Dropped,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            CloseReason::DialError => "dial_error",
            CloseReason::PingTimeout => "ping_timeout",
            CloseReason::SubscribeTimeout => "subscribe_timeout",
            CloseReason::Rejected => "rejected",
            CloseReason::ServerDisconnect => "server_disconnect",
            CloseReason::ServerClosed => "server_closed",
            CloseReason::Dropped => "dropped",
        }
    }
}

/// State, close reasons and timings aggregated across many clients; all methods are
/// safe for concurrent use.
pub struct ClientStats {
    /// Dial start → welcome.
    pub welcome: Histogram,
    /// First subscribe command for a channel → its confirmation (retries included).
    pub confirm: Histogram,
    /// Dial start → last confirmation.
    pub healthy: Histogram,
    /// Ping command → pong (probe clients only).
    pub probe: Histogram,
    states: [AtomicI64; 4],
    reasons: [AtomicI64; 7],
    attempts: AtomicI64,
    started: AtomicI64,
    retries: AtomicI64,
    sids: AtomicI64,
    /// Session ids of the connections the clients closed (or lost).
    closed: Mutex<HashSet<i64>>,
}

impl ClientStats {
    pub fn new() -> Self {
        Self {
            welcome: Histogram::new(),
            confirm: Histogram::new(),
            healthy: Histogram::new(),
            probe: Histogram::new(),
            states: Default::default(),
            reasons: Default::default(),
            attempts: AtomicI64::new(0),
            started: AtomicI64::new(0),
            retries: AtomicI64::new(0),
            sids: AtomicI64::new(0),
            closed: Mutex::new(HashSet::new()),
        }
    }

    fn state_cell(&self, s: ClientState) -> &AtomicI64 {
        &self.states[s as usize]
    }

    /// Clients currently in state `s`.
    pub fn state(&self, s: ClientState) -> i64 {
        self.state_cell(s).load(Ordering::SeqCst)
    }

    /// Attempts that ended with reason `r`.
    pub fn reason(&self, r: CloseReason) -> i64 {
        self.reasons[r as usize].load(Ordering::SeqCst)
    }

    /// All connection attempts, initial connects included.
    pub fn attempts(&self) -> i64 {
        self.attempts.load(Ordering::SeqCst)
    }

    /// Attempts after each client's first one.
    pub fn reconnects(&self) -> i64 {
        self.attempts.load(Ordering::SeqCst) - self.started.load(Ordering::SeqCst)
    }

    /// Re-sent subscribe commands.
    pub fn subscribe_retries(&self) -> i64 {
        self.retries.load(Ordering::SeqCst)
    }

    /// Whether a client already closed (or lost) the connection with session `sid`.
    pub fn session_closed(&self, sid: &str) -> bool {
        sid.parse::<i64>().is_ok_and(|n| self.closed.lock().unwrap_or_else(|e| e.into_inner()).contains(&n))
    }

    fn mark_closed(&self, sid: i64) {
        self.closed.lock().unwrap_or_else(|e| e.into_inner()).insert(sid);
    }
}

impl Default for ClientStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Capped exponential backoff with ±50% jitter:
/// `min(max, min × 2^n) × U(0.5, 1.5)`; zero when `min` is zero.
///
/// As upstream, the delay only doubles while it is below `max`, so a zero `max`
/// means a constant (jittered) `min`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Backoff {
    /// Delay after `n` consecutive failures; `unit` is a uniform sample in [0, 1).
    pub fn delay(&self, n: u32, unit: f64) -> Duration {
        if self.min.is_zero() {
            return Duration::ZERO;
        }
        let mut d = self.min;
        for _ in 0..n {
            if d >= self.max {
                break;
            }
            d = d.saturating_mul(2);
        }
        if !self.max.is_zero() && d > self.max {
            d = self.max;
        }
        d.mul_f64(0.5 + unit)
    }
}

pub struct ReconnectingClientConfig {
    pub url: String,
    /// Channel identifiers (raw JSON strings) subscribed after each welcome.
    pub channels: Vec<String>,
    pub sequential_subscribe: bool,
    /// Wait for a confirmation before re-sending; must be positive.
    pub subscribe_retry_interval: Duration,
    /// Re-sends per channel before the client reconnects.
    pub subscribe_retries_max: u32,
    pub ping_timeout: Duration,
    /// When non-zero, send `ping` commands at this interval while healthy and record
    /// the pong latency in `stats.probe`.
    pub probe_interval: Duration,
    pub backoff: Backoff,
    pub stats: std::sync::Arc<ClientStats>,
    /// Seeds the backoff jitter.
    pub seed: u64,
    /// Action Cable or Pusher frames.
    pub wire: Wire,
    /// Pusher only: send `pusher:ping` when nothing arrived for this long (zero: never).
    /// Probes ping on their own schedule instead.
    pub keepalive: Duration,
}

/// The open connection, as [`ReconnectingClient::drop_connection`] sees it.
struct Open {
    raw: std::net::TcpStream,
    sid: i64,
    dropped: bool,
}

pub struct ReconnectingClient {
    cfg: Box<ReconnectingClientConfig>,
    rng: Mutex<Rng>,
    state: AtomicU8,
    open: Mutex<Option<Open>>,
}

/// Keeps the shared state counters right however `run` ends.
struct StateGuard<'a>(&'a ReconnectingClient);

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        let s = self.0.current_state();
        self.0.cfg.stats.state_cell(s).fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Deserialize)]
struct ServerMessage {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    identifier: Option<String>,
}

/// A server message, whatever the wire format.
enum Incoming {
    Ping,
    Pong,
    Welcome,
    /// The subscription with this identifier (Action Cable) or channel (Pusher) is confirmed.
    Confirm(Option<String>),
    Reject,
    Disconnect,
    Other,
}

impl Incoming {
    fn parse(wire: Wire, text: &str) -> Option<Self> {
        Some(match wire {
            Wire::ActionCable => {
                let msg = serde_json::from_str::<ServerMessage>(text).ok()?;
                match msg.kind.as_str() {
                    "ping" => Incoming::Ping,
                    "pong" => Incoming::Pong,
                    "welcome" => Incoming::Welcome,
                    "confirm_subscription" => Incoming::Confirm(msg.identifier),
                    "reject_subscription" => Incoming::Reject,
                    "disconnect" => Incoming::Disconnect,
                    _ => Incoming::Other,
                }
            }
            Wire::Pusher => {
                let f = serde_json::from_str::<pusher::Frame>(text).ok()?;
                match f.event.as_ref() {
                    pusher::PING => Incoming::Ping,
                    pusher::PONG => Incoming::Pong,
                    pusher::CONNECTION_ESTABLISHED => Incoming::Welcome,
                    pusher::SUBSCRIPTION_SUCCEEDED => Incoming::Confirm(f.channel.map(|c| c.into_owned())),
                    e if pusher::is_subscription_error(e) => Incoming::Reject,
                    pusher::ERROR => Incoming::Disconnect,
                    _ => Incoming::Other,
                }
            }
        })
    }
}

/// One channel's subscribe command until it is confirmed.
struct Pending {
    /// The first send; confirmation latency counts from it.
    sent_at: Instant,
    /// When to re-send (or give up).
    retry_at: Instant,
    retries: u32,
}

type WsSink = SplitSink<Ws, WsMessage>;

impl ReconnectingClient {
    /// Builds an idle client (counted in the backoff state until [`ReconnectingClient::run`]).
    pub fn new(cfg: ReconnectingClientConfig) -> Self {
        cfg.stats.state_cell(ClientState::Backoff).fetch_add(1, Ordering::SeqCst);
        Self {
            rng: Mutex::new(Rng::new(cfg.seed ^ 0x9E37_79B9_7F4A_7C15)),
            cfg: Box::new(cfg),
            state: AtomicU8::new(ClientState::Backoff as u8),
            open: Mutex::new(None),
        }
    }

    fn current_state(&self) -> ClientState {
        ClientState::ALL[self.state.load(Ordering::SeqCst) as usize]
    }

    /// Moves the client to `s` and updates the shared counters (only the run task
    /// calls it).
    fn set_state(&self, s: ClientState) {
        let old = self.current_state();
        if old == s {
            return;
        }
        self.cfg.stats.state_cell(old).fetch_sub(1, Ordering::SeqCst);
        self.cfg.stats.state_cell(s).fetch_add(1, Ordering::SeqCst);
        self.state.store(s as u8, Ordering::SeqCst);
    }

    /// Runs the connect loop until `cancel`; `initial_delay` staggers the first attempt.
    pub fn run(&self, cancel: &CancelToken, initial_delay: Duration) {
        block_on(self.run_async(cancel, initial_delay));
    }

    /// Async [`ReconnectingClient::run`].
    pub async fn run_async(&self, cancel: &CancelToken, initial_delay: Duration) {
        let _guard = StateGuard(self);
        if !cancel.sleep_async(initial_delay).await {
            return;
        }
        self.cfg.stats.started.fetch_add(1, Ordering::SeqCst);
        let mut failures = 0u32;
        while !cancel.is_cancelled() {
            let (healthy, reason) = self.attempt(cancel).await;
            if cancel.is_cancelled() {
                return;
            }
            // Leave the healthy state before the reason is counted, so an observer
            // that sees the reason never also counts this client as healthy.
            self.set_state(ClientState::Backoff);
            self.cfg.stats.reasons[reason as usize].fetch_add(1, Ordering::SeqCst);
            if healthy {
                failures = 0;
            }
            let unit = self.rng.lock().unwrap_or_else(|e| e.into_inner()).unit();
            if !cancel.sleep_async(self.cfg.backoff.delay(failures, unit)).await {
                return;
            }
            failures = failures.saturating_add(1);
        }
    }

    /// Closes the current socket abruptly (no close frame), like an LB restart.
    /// Returns `false` when no connection is open (upstream `Drop`).
    pub fn drop_connection(&self) -> bool {
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        let Some(o) = open.as_mut() else { return false };
        o.dropped = true;
        self.cfg.stats.mark_closed(o.sid);
        net::abort(&o.raw);
        true
    }

    /// One connection from dial to close; whether the client reached the healthy
    /// state during it, and why it ended.
    async fn attempt(&self, cancel: &CancelToken) -> (bool, CloseReason) {
        let stats = &self.cfg.stats;
        stats.attempts.fetch_add(1, Ordering::SeqCst);
        self.set_state(ClientState::Connecting);
        let start = Instant::now();
        let sid = stats.sids.fetch_add(1, Ordering::SeqCst) + 1;
        let sep = if self.cfg.url.contains('?') { '&' } else { '?' };
        let url = format!("{}{sep}bench_sid={sid}", self.cfg.url);

        // The handshake timeout is the ping timeout (upstream `HandshakeTimeout`).
        let dialed = tokio::select! {
            () = cancel.cancelled() => return (false, CloseReason::Dropped),
            d = tokio::time::timeout(self.cfg.ping_timeout, net::dial(&url)) => d,
        };
        let dialed = match dialed {
            Ok(Ok(d)) => d,
            _ => {
                // The handshake can time out while the server still runs Connect.
                stats.mark_closed(sid);
                return (false, CloseReason::DialError);
            }
        };
        let raw = match dialed.raw.try_clone() {
            Ok(r) => r,
            Err(_) => {
                stats.mark_closed(sid);
                net::abort(&dialed.raw);
                return (false, CloseReason::DialError);
            }
        };
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = Some(Open { raw, sid, dropped: false });

        let (healthy, reason) = self.session(dialed.ws, start, cancel).await;

        let dropped = self.open.lock().unwrap_or_else(|e| e.into_inner()).take().is_some_and(|o| o.dropped);
        stats.mark_closed(sid);
        net::abort(&dialed.raw);
        (healthy, if dropped { CloseReason::Dropped } else { reason })
    }

    async fn session(&self, ws: Ws, start: Instant, cancel: &CancelToken) -> (bool, CloseReason) {
        let cfg = &self.cfg;
        let stats = &cfg.stats;
        let (mut sink, mut stream) = ws.split();
        // The ping watchdog starts when the socket opens: each ping pushes the read
        // deadline forward; any read past the deadline fails.
        let mut read_deadline = Instant::now() + cfg.ping_timeout;
        let mut probe = (!cfg.probe_interval.is_zero()).then(|| {
            let mut t = tokio::time::interval_at(Instant::now() + cfg.probe_interval, cfg.probe_interval);
            t.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            t
        });
        // Pusher: ping the server once it has been quiet for the keepalive period (the
        // deadline moves with every message and every ping sent).
        let keepalive = cfg.wire == Wire::Pusher && probe.is_none() && !cfg.keepalive.is_zero();
        let mut ping_due = Instant::now() + cfg.keepalive;
        let mut ping_sent_at: Option<Instant> = None;
        let mut pending: Option<HashMap<String, Pending>> = None;
        let mut unsent: VecDeque<String> = VecDeque::new();
        let mut subscribed = false;
        let mut healthy = false;

        loop {
            let next_retry = pending.as_ref().and_then(|p| p.values().map(|s| s.retry_at).min());
            tokio::select! {
                () = cancel.cancelled() => return (false, CloseReason::Dropped),
                _ = async { probe.as_mut().expect("probe ticker").tick().await }, if probe.is_some() => {
                    if !healthy || ping_sent_at.is_some() {
                        continue;
                    }
                    ping_sent_at = Some(Instant::now());
                    if self.send(&mut sink, "ping", "").await.is_err() {
                        return (healthy, CloseReason::ServerClosed);
                    }
                }
                () = tokio::time::sleep_until(ping_due), if keepalive => {
                    ping_due = Instant::now() + cfg.keepalive;
                    if self.send(&mut sink, "ping", "").await.is_err() {
                        return (healthy, CloseReason::ServerClosed);
                    }
                }
                () = sleep_until_opt(next_retry), if next_retry.is_some() => {
                    let now = Instant::now();
                    let p = pending.as_mut().expect("pending subscriptions");
                    let due: Vec<String> = p.iter().filter(|(_, s)| now >= s.retry_at).map(|(id, _)| id.clone()).collect();
                    for id in due {
                        let sub = p.get_mut(&id).expect("due subscription");
                        if sub.retries >= cfg.subscribe_retries_max {
                            close_gracefully(&mut sink).await;
                            return (healthy, CloseReason::SubscribeTimeout);
                        }
                        sub.retries += 1;
                        sub.retry_at = now + cfg.subscribe_retry_interval;
                        stats.retries.fetch_add(1, Ordering::SeqCst);
                        if self.send(&mut sink, "subscribe", &id).await.is_err() {
                            return (healthy, CloseReason::ServerClosed);
                        }
                    }
                }
                msg = tokio::time::timeout_at(read_deadline, stream.next()) => {
                    let text = match msg {
                        Err(_) => return (healthy, CloseReason::PingTimeout),
                        Ok(Some(Ok(WsMessage::Text(t)))) => t,
                        Ok(Some(Ok(WsMessage::Binary(_) | WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_)))) => continue,
                        Ok(Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None) => return (healthy, CloseReason::ServerClosed),
                    };
                    let Some(msg) = Incoming::parse(cfg.wire, text.as_str()) else { continue };
                    if cfg.wire == Wire::Pusher {
                        // Any message shows the connection is alive.
                        read_deadline = Instant::now() + cfg.ping_timeout;
                        ping_due = Instant::now() + cfg.keepalive;
                    }
                    match msg {
                        Incoming::Ping => {
                            read_deadline = Instant::now() + cfg.ping_timeout;
                            if cfg.wire == Wire::Pusher && self.send(&mut sink, "pong", "").await.is_err() {
                                return (healthy, CloseReason::ServerClosed);
                            }
                        }
                        Incoming::Pong => {
                            if let Some(sent) = ping_sent_at.take() {
                                stats.probe.record(sent.elapsed());
                            }
                        }
                        Incoming::Welcome => {
                            if subscribed {
                                continue;
                            }
                            subscribed = true;
                            stats.welcome.record(start.elapsed());
                            self.set_state(ClientState::Subscribing);
                            let p = pending.insert(HashMap::with_capacity(cfg.channels.len()));
                            unsent = cfg.channels.iter().cloned().collect();
                            while let Some(id) = unsent.pop_front() {
                                let now = Instant::now();
                                p.insert(id.clone(), Pending { sent_at: now, retry_at: now + cfg.subscribe_retry_interval, retries: 0 });
                                if self.send(&mut sink, "subscribe", &id).await.is_err() {
                                    return (false, CloseReason::ServerClosed);
                                }
                                if cfg.sequential_subscribe {
                                    break;
                                }
                            }
                            if p.is_empty() {
                                stats.healthy.record(start.elapsed());
                                self.set_state(ClientState::Healthy);
                                healthy = true;
                            }
                        }
                        Incoming::Confirm(identifier) => {
                            let Some(p) = pending.as_mut() else { continue };
                            // A late confirmation for a re-sent command is ignored.
                            let Some(sub) = identifier.as_deref().and_then(|id| p.remove(id)) else { continue };
                            stats.confirm.record(sub.sent_at.elapsed());
                            if let Some(id) = unsent.pop_front() {
                                let now = Instant::now();
                                p.insert(id.clone(), Pending { sent_at: now, retry_at: now + cfg.subscribe_retry_interval, retries: 0 });
                                if self.send(&mut sink, "subscribe", &id).await.is_err() {
                                    return (healthy, CloseReason::ServerClosed);
                                }
                            }
                            if p.is_empty() {
                                stats.healthy.record(start.elapsed());
                                self.set_state(ClientState::Healthy);
                                healthy = true;
                            }
                        }
                        Incoming::Reject => {
                            close_gracefully(&mut sink).await;
                            return (healthy, CloseReason::Rejected);
                        }
                        Incoming::Disconnect => return (healthy, CloseReason::ServerDisconnect),
                        Incoming::Other => {}
                    }
                }
            }
        }
    }

    /// `{"command":..,"identifier":..}` (Go marshals the map with sorted keys), with
    /// the ping timeout as write deadline. Pusher: `pusher:subscribe` to channel
    /// `identifier`, `pusher:ping` or `pusher:pong`.
    async fn send(&self, sink: &mut WsSink, command: &str, identifier: &str) -> Result<(), ()> {
        let text = match (self.cfg.wire, command) {
            (Wire::Pusher, "subscribe") => pusher::subscribe_frame(identifier),
            (Wire::Pusher, "ping") => pusher::PING_FRAME.to_owned(),
            (Wire::Pusher, _) => pusher::PONG_FRAME.to_owned(),
            (Wire::ActionCable, _) => format!(
                "{{\"command\":{},\"identifier\":{}}}",
                crate::gojson::string(command),
                crate::gojson::string(identifier)
            ),
        };
        match tokio::time::timeout(self.cfg.ping_timeout, sink.send(WsMessage::text(text))).await {
            Ok(Ok(())) => Ok(()),
            _ => Err(()),
        }
    }
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(t) => tokio::time::sleep_until(t).await,
        None => std::future::pending().await,
    }
}

/// Sends a close frame, as a browser's `ws.close()` would.
async fn close_gracefully(sink: &mut WsSink) {
    let frame = CloseFrame { code: CloseCode::Normal, reason: "".into() };
    let _ = tokio::time::timeout(Duration::from_secs(1), sink.send(WsMessage::Close(Some(frame)))).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_max_only_when_max_is_set() {
        let b = Backoff { min: Duration::from_millis(100), max: Duration::from_millis(350) };
        assert_eq!(Duration::from_millis(100), b.delay(0, 0.5));
        assert_eq!(Duration::from_millis(200), b.delay(1, 0.5));
        assert_eq!(Duration::from_millis(350), b.delay(5, 0.5));
        let constant = Backoff { min: Duration::from_millis(10), max: Duration::ZERO };
        assert_eq!(Duration::from_millis(10), constant.delay(7, 0.5));
        assert_eq!(Duration::ZERO, Backoff::default().delay(3, 0.9));
    }
}
