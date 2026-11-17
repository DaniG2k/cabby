//! RPC controller (upstream package `rpc`): calls the application's AnyCable RPC
//! service over gRPC or HTTP to authenticate connections and run channel commands.
//!
//! Status: interfaces only. Deadlines, retries, concurrency limits and connection
//! reuse sit behind explicit traits ([`Barrier`], [`ClientHelper`], [`RpcClient`])
//! and the options in [`Config`], mirroring upstream.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use cabby_core::common::{CommandResult, ConnectResult, SessionEnv};
use cabby_core::pending;
use cabby_core::protocol::ApplicationError;
use cabby_core::protos;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::metrics::Instrumenter;

mod barrier;
mod config;
pub mod grpc;
pub mod http;
mod inprocess;

pub use barrier::{Barrier, FixedSizeBarrier};
pub use config::{Config, DEFAULT_RPC_CONCURRENCY, DEFAULT_RPC_HOST, DialFun, Dialed, Dialer};
pub use inprocess::new_inprocess_service_dialer;
pub use tonic::Code;

/// Comma-separated list of compatible RPC proto versions, sent as `protov` metadata.
pub const PROTO_VERSIONS: &str = "v1";

/// Metric names registered by [`Controller::new`] (upstream constants).
pub mod metric_names {
    pub const RPC_CALLS: &str = "rpc_call_total";
    pub const RPC_RETRIES: &str = "rpc_retries_total";
    pub const RPC_FAILURES: &str = "rpc_error_total";
    pub const RPC_TIMEOUTS: &str = "rpc_timeout_total";
    pub const RPC_CANCELED: &str = "rpc_canceled_total";
    pub const RPC_WAIT_TIMEOUTS: &str = "rpc_wait_timeout_total";
    pub const RPC_PENDING: &str = "rpc_pending_num";
    pub const RPC_CAPACITY: &str = "rpc_capacity_num";
    pub const GRPC_ACTIVE_CONNS: &str = "grpc_active_conn_num";
}

/// Boxed future used by the object-safe traits below.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Request metadata as ordered key/value pairs (gRPC metadata, or the HTTP RPC
/// `x-anycable-meta-<key>` headers). Keys are lower-case.
pub type Metadata = Vec<(String, String)>;

/// Per-call context: the Rust counterpart of Go's `context.Context` as the
/// controller uses it (cancellation by the session, deadline, metadata).
#[derive(Debug, Clone, Default)]
pub struct Context {
    /// Cancelled when the session goes away.
    pub cancel: CancellationToken,
    pub deadline: Option<Instant>,
    /// Metadata attached to the call (Go: incoming metadata seen by a service).
    pub metadata: Metadata,
}

/// A gRPC status: what every RPC transport error is converted into upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcStatus {
    pub code: Code,
    pub message: String,
}

/// Errors returned by the controller and the RPC services.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RpcError {
    /// Upstream `ErrWaitTimeout`: no free RPC slot within the wait timeout.
    #[error("RPC wait timeout: no free slot")]
    WaitTimeout,
    /// The call failed with a gRPC status (including timeouts and HTTP RPC errors).
    #[error("rpc error: code = {:?} desc = {}", .0.code, .0.message)]
    Status(RpcStatus),
    /// The application answered with status ERROR.
    #[error(transparent)]
    Application(#[from] ApplicationError),
    /// Anything else (configuration, deserialization, readiness).
    #[error("{0}")]
    Other(String),
}

/// Go's `(*Result, error)` pair: upstream returns a result *and* an error for
/// application errors, and neither when the session was cancelled.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcOutcome<T> {
    pub result: Option<T>,
    pub error: Option<RpcError>,
}

/// Client side of the AnyCable RPC service (upstream `pb.RPCClient`).
pub trait RpcClient: Send + Sync {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, RpcStatus>>;
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, RpcStatus>>;
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, RpcStatus>>;
}

/// Server side of the AnyCable RPC service (upstream `pb.RPCServer`). The context
/// carries the *incoming* metadata.
pub trait RpcService: Send + Sync {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, RpcStatus>>;
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, RpcStatus>>;
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, RpcStatus>>;
}

/// Transport health of a dialed client (upstream `ClientHelper`).
pub trait ClientHelper: Send + Sync {
    /// `Ok` when calls may be attempted (gRPC: not shut down, recovering from
    /// transient failure at most once; HTTP: circuit breaker not open).
    fn ready(&self) -> Result<(), RpcError>;
    fn supports_active_conns(&self) -> bool;
    fn active_conns(&self) -> usize;
    fn close(&self);
}

/// The RPC controller (upstream `rpc.Controller`).
pub struct Controller {
    _config: Config,
    _metrics: Arc<dyn Instrumenter>,
}

impl fmt::Debug for Controller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Controller").finish_non_exhaustive()
    }
}

impl Controller {
    /// Registers the RPC metrics and builds the controller (upstream `NewController`).
    pub fn new(_metrics: Arc<dyn Instrumenter>, _config: Config) -> Result<Self, RpcError> {
        pending!("rpc::new_controller")
    }

    /// Dials the RPC service (`config.dial_fun`, or the gRPC/HTTP default).
    pub fn start(&self) -> Result<(), RpcError> {
        pending!("rpc::controller_start")
    }

    /// Waits for in-flight calls (bounded by the command timeout) and closes the client.
    pub async fn shutdown(&self) -> Result<(), RpcError> {
        pending!("rpc::controller_shutdown")
    }

    /// Connect RPC.
    pub async fn authenticate(&self, _ctx: &Context, _sid: &str, _env: &SessionEnv) -> RpcOutcome<ConnectResult> {
        pending!("rpc::authenticate")
    }

    /// Command RPC with `subscribe`.
    pub async fn subscribe(
        &self,
        _ctx: &Context,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _channel: &str,
    ) -> RpcOutcome<CommandResult> {
        pending!("rpc::subscribe")
    }

    /// Command RPC with `unsubscribe`.
    pub async fn unsubscribe(
        &self,
        _ctx: &Context,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _channel: &str,
    ) -> RpcOutcome<CommandResult> {
        pending!("rpc::unsubscribe")
    }

    /// Command RPC with `message`.
    pub async fn perform(
        &self,
        _ctx: &Context,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _channel: &str,
        _data: &str,
    ) -> RpcOutcome<CommandResult> {
        pending!("rpc::perform")
    }

    /// Disconnect RPC. Waits for a slot without the wait timeout.
    pub async fn disconnect(
        &self,
        _ctx: &Context,
        _sid: &str,
        _env: &SessionEnv,
        _id: &str,
        _subscriptions: &[String],
    ) -> Result<(), RpcError> {
        pending!("rpc::disconnect")
    }

    /// The effective wait-for-slot timeout (zero: unlimited).
    pub fn wait_timeout(&self) -> Duration {
        pending!("rpc::wait_timeout")
    }

    /// Active transport connections reported by the client helper.
    pub fn active_conns(&self) -> usize {
        pending!("rpc::active_conns")
    }
}
