//! Browser-like reconnecting Action Cable client (upstream
//! `lib/reconnecting_client.go`).
//!
//! Each attempt dials `<url>?bench_sid=<n>`, waits for the welcome, subscribes to its
//! channels (all at once or sequentially), and ends on: missed server pings (the
//! watchdog starts at socket open), a subscription unconfirmed after all retries
//! (graceful close), reject, server disconnect, server close, or a [`ReconnectingClient::drop_connection`].
//! It then backs off and reconnects.

use std::time::Duration;

use cabby_core::pending;

use crate::cancel::CancelToken;
use crate::histogram::Histogram;

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
    _attempts: std::sync::atomic::AtomicI64,
}

impl ClientStats {
    pub fn new() -> Self {
        pending!("benchi::new_client_stats")
    }

    /// Clients currently in state `s`.
    pub fn state(&self, _s: ClientState) -> i64 {
        pending!("benchi::client_stats_state")
    }

    /// Attempts that ended with reason `r`.
    pub fn reason(&self, _r: CloseReason) -> i64 {
        pending!("benchi::client_stats_reason")
    }

    /// All connection attempts, initial connects included.
    pub fn attempts(&self) -> i64 {
        pending!("benchi::client_stats_attempts")
    }

    /// Attempts after each client's first one.
    pub fn reconnects(&self) -> i64 {
        pending!("benchi::client_stats_reconnects")
    }

    /// Re-sent subscribe commands.
    pub fn subscribe_retries(&self) -> i64 {
        pending!("benchi::client_stats_subscribe_retries")
    }

    /// Whether a client already closed (or lost) the connection with session `sid`.
    pub fn session_closed(&self, _sid: &str) -> bool {
        pending!("benchi::client_stats_session_closed")
    }
}

impl Default for ClientStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Capped exponential backoff with ±50% jitter:
/// `min(max, min × 2^n) × U(0.5, 1.5)`; zero when `min` is zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Backoff {
    /// Delay after `n` consecutive failures; `unit` is a uniform sample in [0, 1).
    pub fn delay(&self, _n: u32, _unit: f64) -> Duration {
        pending!("benchi::backoff_delay")
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
}

pub struct ReconnectingClient {
    _cfg: (),
}

impl ReconnectingClient {
    /// Builds an idle client (counted in the backoff state until [`ReconnectingClient::run`]).
    pub fn new(_cfg: ReconnectingClientConfig) -> Self {
        pending!("benchi::new_reconnecting_client")
    }

    /// Runs the connect loop until `cancel`; `initial_delay` staggers the first attempt.
    pub fn run(&self, _cancel: &CancelToken, _initial_delay: Duration) {
        pending!("benchi::reconnecting_client_run")
    }

    /// Closes the current socket abruptly (no close frame), like an LB restart.
    /// Returns `false` when no connection is open (upstream `Drop`).
    pub fn drop_connection(&self) -> bool {
        pending!("benchi::reconnecting_client_drop")
    }
}
