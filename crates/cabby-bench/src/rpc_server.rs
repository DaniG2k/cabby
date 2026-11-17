//! Fake AnyCable HTTP RPC application (upstream `lib/rpc_server.go`).
//!
//! Runs in the harness process so the injected RPC latency is the same for every
//! server under test. It accepts every connection (identifier `{"id":<bench_sid>}`),
//! confirms every subscription (streaming from `bench:<identifier>`) and acknowledges
//! every disconnect, each after a jittered delay.

use std::sync::Arc;
use std::time::Duration;

use cabby_core::pending;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcMethod {
    Connect,
    Command,
    Disconnect,
}

impl RpcMethod {
    pub const ALL: [RpcMethod; 3] = [RpcMethod::Connect, RpcMethod::Command, RpcMethod::Disconnect];

    pub fn as_str(self) -> &'static str {
        match self {
            RpcMethod::Connect => "connect",
            RpcMethod::Command => "command",
            RpcMethod::Disconnect => "disconnect",
        }
    }
}

/// Reports whether the client already closed the session with this id.
pub type SessionClosedFn = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Per-method latency falls back to `latency` when zero; each call sleeps
/// `latency × (1 ± jitter)`, uniform, `jitter` in [0, 1].
#[derive(Clone, Default)]
pub struct RpcServerConfig {
    pub latency: Duration,
    pub connect_latency: Duration,
    pub command_latency: Duration,
    pub disconnect_latency: Duration,
    pub jitter: f64,
    /// When set, Connect and Command calls that start after the close are counted
    /// in [`RpcMethodStats::after_close`].
    pub session_closed: Option<SessionClosedFn>,
}

impl RpcServerConfig {
    pub fn latency_for(&self, _m: RpcMethod) -> Duration {
        pending!("benchi::rpc_latency_for")
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RpcMethodStats {
    pub started: i64,
    pub completed: i64,
    pub in_flight: i64,
    /// Calls that started after the client had closed the session (Connect and
    /// Command only).
    pub after_close: i64,
}

pub struct RpcServer {
    _listener: (),
}

impl RpcServer {
    /// Starts on a loopback port.
    pub fn new(_cfg: RpcServerConfig) -> Self {
        pending!("benchi::new_rpc_server")
    }

    /// Base URL to pass as the server's RPC host.
    pub fn url(&self) -> String {
        pending!("benchi::rpc_server_url")
    }

    /// Stops the server, interrupting in-flight calls.
    pub fn close(&self) {
        pending!("benchi::rpc_server_close")
    }

    pub fn stats(&self, _m: RpcMethod) -> RpcMethodStats {
        pending!("benchi::rpc_server_stats")
    }
}

/// The session id of an RPC request body: the `bench_sid` URL parameter for Connect,
/// the `id` connection identifier otherwise (empty when absent).
pub fn session_id(_m: RpcMethod, _body: &[u8]) -> String {
    pending!("benchi::rpc_session_id")
}
