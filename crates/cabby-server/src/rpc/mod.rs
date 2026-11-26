//! RPC controller (upstream package `rpc`): calls the application's AnyCable RPC
//! service over gRPC or HTTP to authenticate connections and run channel commands.
//!
//! Every call takes a slot from a fair semaphore ([`FixedSizeBarrier`]), waiting
//! at most the wait timeout; it then runs within the command timeout, retrying
//! `RESOURCE_EXHAUSTED` / `UNAVAILABLE` with exponential backoff, each attempt
//! bounded by the request timeout. Deadlines and session cancellation are
//! enforced here, whatever the client does, so the timeouts are exact.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use cabby_core::common::{CommandResult, ConnectResult, SessionEnv};
use cabby_core::logger::{Attr, Level, LogValue};
use cabby_core::protocol::{self, ApplicationError};
use cabby_core::protos;
use cabby_core::utils::MessageVerifier;
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

const RETRY_EXHAUSTED_INTERVAL_MS: u64 = 10;
const RETRY_UNAVAILABLE_INTERVAL_MS: u64 = 100;
const REFRESH_METRICS_INTERVAL: Duration = Duration::from_secs(10);
const SECRET_KEY_PHRASE: &str = "rpc-cable";

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

use metric_names::*;

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

impl Context {
    /// A child context whose deadline is at most `timeout` from now (Go
    /// `context.WithTimeout`). Cancellation and metadata are inherited.
    pub fn with_timeout(&self, timeout: Duration) -> Context {
        let deadline = Instant::now() + timeout;
        Context {
            cancel: self.cancel.clone(),
            deadline: Some(self.deadline.map_or(deadline, |d| d.min(deadline))),
            metadata: self.metadata.clone(),
        }
    }

    /// Go `ctx.Err()`: `Canceled` once cancelled, `DeadlineExceeded` once past the deadline.
    pub fn err(&self) -> Option<ClientError> {
        if self.cancel.is_cancelled() {
            Some(ClientError::Canceled)
        } else if self.deadline.is_some_and(|d| Instant::now() >= d) {
            Some(ClientError::DeadlineExceeded)
        } else {
            None
        }
    }

    /// Resolves when the context is done (Go `<-ctx.Done()`), with [`Context::err`].
    pub async fn done(&self) -> ClientError {
        match self.deadline {
            None => {
                self.cancel.cancelled().await;
                ClientError::Canceled
            }
            Some(deadline) => tokio::select! {
                () = self.cancel.cancelled() => ClientError::Canceled,
                () = tokio::time::sleep_until(deadline) => {
                    if self.cancel.is_cancelled() { ClientError::Canceled } else { ClientError::DeadlineExceeded }
                }
            },
        }
    }

    /// Runs `call` until it completes or the context is done; a done context
    /// yields the gRPC status a real transport reports (`CANCELED` /
    /// `DEADLINE_EXCEEDED`). A call that completes in the same poll wins.
    /// A status error that arrives once the deadline has passed is reported as
    /// the deadline (the server may have ended the call on its copy of it).
    pub async fn bound<T>(&self, call: impl Future<Output = Result<T, ClientError>>) -> Result<T, ClientError> {
        tokio::select! {
            biased;
            r = call => match r {
                Err(ClientError::Status(s))
                    if s.code != Code::DeadlineExceeded && !self.cancel.is_cancelled() && self.err().is_some() =>
                {
                    Err(ClientError::DeadlineExceeded.into_status())
                }
                r => r,
            },
            e = self.done() => Err(e.into_status()),
        }
    }
}

/// A gRPC status: what every RPC transport error is converted into upstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcStatus {
    pub code: Code,
    pub message: String,
}

/// Error of a single client/service call (Go: the `error` returned by a
/// `pb.RPCClient` method). Context errors are distinct from gRPC statuses, as in
/// Go, where a client may return the bare `context.Canceled` /
/// `context.DeadlineExceeded` or a status error (real transports convert context
/// errors into `Canceled` / `DeadlineExceeded` statuses).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ClientError {
    /// Go `context.Canceled`.
    #[error("context canceled")]
    Canceled,
    /// Go `context.DeadlineExceeded`.
    #[error("context deadline exceeded")]
    DeadlineExceeded,
    #[error("rpc error: code = {:?} desc = {}", .0.code, .0.message)]
    Status(RpcStatus),
    #[error("{0}")]
    Other(String),
}

impl ClientError {
    pub fn status(code: Code, message: impl Into<String>) -> Self {
        ClientError::Status(RpcStatus { code, message: message.into() })
    }

    /// A context error as the status grpc-go reports for it.
    fn into_status(self) -> Self {
        match self {
            ClientError::Canceled => ClientError::status(Code::Cancelled, "context canceled"),
            ClientError::DeadlineExceeded => ClientError::status(Code::DeadlineExceeded, "context deadline exceeded"),
            other => other,
        }
    }
}

impl From<ClientError> for RpcError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Status(s) => RpcError::Status(s),
            other => RpcError::Other(other.to_string()),
        }
    }
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

impl<T> RpcOutcome<T> {
    fn none() -> Self {
        RpcOutcome { result: None, error: None }
    }

    fn failed(error: RpcError) -> Self {
        RpcOutcome { result: None, error: Some(error) }
    }
}

/// Client side of the AnyCable RPC service (upstream `pb.RPCClient`).
pub trait RpcClient: Send + Sync {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>>;
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>>;
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>>;
}

/// Server side of the AnyCable RPC service (upstream `pb.RPCServer`). The context
/// carries the *incoming* metadata.
pub trait RpcService: Send + Sync {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>>;
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>>;
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>>;
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

/// Why a call produced no response.
enum CallError {
    /// Go `errors.Is(err, context.Canceled)`: the session went away.
    Canceled,
    Failed(RpcError),
}

/// A taken barrier slot, released on drop (also when the session's call future
/// is dropped mid-flight).
struct Slot<'a>(&'a FixedSizeBarrier);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

/// Decrements the pending gauge on drop.
struct Pending<'a>(&'a dyn Instrumenter);

impl<'a> Pending<'a> {
    fn new(metrics: &'a dyn Instrumenter) -> Self {
        metrics.gauge_increment(RPC_PENDING);
        Pending(metrics)
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.0.gauge_decrement(RPC_PENDING);
    }
}

fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 1);
    all.push(Attr::new("context", LogValue::String("rpc".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

/// The RPC controller (upstream `rpc.Controller`).
pub struct Controller {
    config: Config,
    metrics: Arc<dyn Instrumenter>,
    barrier: FixedSizeBarrier,
    dialed: OnceLock<Dialed>,
    /// Timeout of a single request (zero: none).
    request_timeout: Duration,
    /// Timeout of a command including retries (zero: none).
    command_timeout: Duration,
    /// Timeout for a free slot (zero: none).
    wait_timeout: Duration,
    /// Stops the metrics refresher.
    stop: CancellationToken,
}

impl fmt::Debug for Controller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Controller").field("host", &self.config.host).finish_non_exhaustive()
    }
}

/// The RPC host for the start log: a URL with credentials and query values
/// masked (upstream `FilteredURL`), redacted as in error texts first when it may
/// hold part of a password (D-093). Bare `host:port` pairs are not valid URLs
/// and carry no credentials: they are shown as given.
pub(crate) fn logged_host(host: &str) -> String {
    if !host.contains("://") {
        return host.to_owned();
    }
    let shown = if cabby_core::utils::userinfo_may_be_cut(host) {
        cabby_core::utils::redact_refused_url(host)
    } else {
        host.to_owned()
    };
    cabby_core::logger::LogValuer::log_value(&cabby_core::logger::filtered_url(&shown)).to_string()
}

fn millis(ms: i64) -> Duration {
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

impl Controller {
    /// Registers the RPC metrics and builds the controller (upstream `NewController`).
    pub fn new(metrics: Arc<dyn Instrumenter>, config: Config) -> Result<Self, RpcError> {
        metrics.register_counter(RPC_CALLS, "The total number of RPC calls");
        metrics.register_counter(RPC_RETRIES, "The total number of RPC call retries");
        metrics.register_counter(RPC_FAILURES, "The total number of failed RPC calls");
        metrics.register_counter(RPC_TIMEOUTS, "The total number of RPC call that timed out");
        metrics.register_counter(RPC_CANCELED, "The total number of canceled RPC calls");
        metrics.register_counter(RPC_WAIT_TIMEOUTS, "The total number of RPC calls that timed out waiting in a queue");
        metrics.register_gauge(RPC_PENDING, "The number of pending RPC calls");

        let mut capacity = config.concurrency;
        if capacity <= 0 {
            capacity = DEFAULT_RPC_CONCURRENCY;
            // Upstream logs through the runner's logger here, without `context=rpc`.
            crate::metrics::log(Level::Warn, "RPC concurrency must be positive, reverted to the default value", &[]);
        }
        let barrier = FixedSizeBarrier::new(capacity)?;

        if barrier.has_dynamic_capacity() {
            metrics.register_gauge(RPC_CAPACITY, "The max number of concurrent RPC calls allowed");
            metrics.gauge_set(RPC_CAPACITY, barrier.capacity() as u64);
        }

        let implementation = config.implementation();
        if implementation == "grpc" {
            metrics.register_gauge(GRPC_ACTIVE_CONNS, "The number of active HTTP connections used by gRPC");
        }

        // TODO(v2) upstream: remove the HTTP fallback.
        let request_timeout = if implementation == "http" {
            millis(config.effective_http_request_timeout())
        } else {
            millis(config.request_timeout)
        };
        let command_timeout = millis(config.command_timeout);
        let wait_timeout = if config.wait_timeout < 0 { command_timeout } else { millis(config.wait_timeout) };

        Ok(Self {
            config,
            metrics,
            barrier,
            dialed: OnceLock::new(),
            request_timeout,
            command_timeout,
            wait_timeout,
            stop: CancellationToken::new(),
        })
    }

    /// Dials the RPC service (`config.dial_fun`, or the gRPC/HTTP default).
    pub fn start(&self) -> Result<(), RpcError> {
        let implementation = self.config.implementation();
        let mut config = self.config.clone();

        let dialer: Dialer = match &config.dial_fun {
            Some(DialFun(d)) => Arc::clone(d),
            None => match implementation.as_str() {
                "http" => {
                    if config.secret.is_empty() && !config.secret_base.is_empty() {
                        config.secret = MessageVerifier::new(&config.secret_base).sign(SECRET_KEY_PHRASE.as_bytes());
                        log(Level::Info, "auto-generated authorization secret from the application secret", &[]);
                    }
                    http::new_http_dialer(&config)?
                }
                "grpc" => Arc::new(grpc::default_dialer),
                other => return Err(RpcError::Other(format!("unknown RPC implementation: {other}"))),
            },
        };

        let (client, helper) = dialer(&config)?;

        let join = |items: &[String], empty: &str| {
            if items.is_empty() { empty.to_owned() } else { items.join(",") }
        };
        let logged_host = logged_host(&config.host);
        log(
            Level::Info,
            &format!(
                "RPC controller initialized: {logged_host} (concurrency: {}, impl: {implementation}, enable_tls: {}, \
                 proto_versions: {PROTO_VERSIONS}, proxy_headers: {}, proxy_cookies: {})",
                self.barrier.capacity_info(),
                config.tls_enabled(),
                join(&config.proxy_headers, "<none>"),
                join(&config.proxy_cookies, "<all>"),
            ),
            &[],
        );

        let helper_for_metrics = Arc::clone(&helper);
        if self.dialed.set((client, helper)).is_err() {
            return Err(RpcError::Other("RPC controller already started".into()));
        }

        if helper_for_metrics.supports_active_conns() || self.barrier.has_dynamic_capacity() {
            self.spawn_metrics_refresher(helper_for_metrics);
        }
        self.barrier.start();
        Ok(())
    }

    /// Refreshes the connection gauge every 10s (needs a Tokio runtime; started
    /// only when one is current).
    fn spawn_metrics_refresher(&self, helper: Arc<dyn ClientHelper>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (metrics, stop) = (Arc::clone(&self.metrics), self.stop.clone());
        handle.spawn(async move {
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = tokio::time::sleep(REFRESH_METRICS_INTERVAL) => {
                        if helper.supports_active_conns() {
                            metrics.gauge_set(GRPC_ACTIVE_CONNS, helper.active_conns() as u64);
                        }
                    }
                }
            }
        });
    }

    /// Fails if calls are still in flight (upstream does not wait either: its
    /// check is not retriable), then closes the client.
    pub async fn shutdown(&self) -> Result<(), RpcError> {
        let Some((_, helper)) = self.dialed.get() else {
            return Ok(());
        };
        self.stop.cancel();

        let busy = self.barrier.busy_count();
        if busy > 0 {
            log(
                Level::Info,
                "waiting for active RPC calls to finish",
                &[Attr::new("num", LogValue::Uint(busy as u64))],
            );
        }
        let result = helper.ready().and_then(|()| {
            let busy = self.barrier.busy_count();
            if busy > 0 {
                Err(RpcError::Other(format!("terminated while completing active RPC calls: {busy}")))
            } else {
                log(Level::Info, "all active RPC calls finished", &[]);
                Ok(())
            }
        });
        self.barrier.stop();
        helper.close();
        result
    }

    fn dialed(&self) -> Result<&Dialed, RpcError> {
        self.dialed.get().ok_or_else(|| RpcError::Other("RPC controller is not started".into()))
    }

    /// Takes a slot within the wait timeout. `Ok(None)`: the session was cancelled.
    async fn acquire(&self, ctx: &Context) -> Result<Option<Slot<'_>>, RpcError> {
        let result = {
            let _pending = Pending::new(self.metrics.as_ref());
            if self.wait_timeout.is_zero() {
                self.barrier.wait(ctx).await
            } else {
                self.barrier.wait(&ctx.with_timeout(self.wait_timeout)).await
            }
        };

        match result {
            Ok(()) if ctx.err().is_none() => Ok(Some(Slot(&self.barrier))),
            Ok(()) => {
                // Both the slot and the session's end were ready.
                self.barrier.release();
                self.metrics.counter_increment(RPC_CANCELED);
                Ok(None)
            }
            Err(_) if ctx.err().is_some() => {
                self.metrics.counter_increment(RPC_CANCELED);
                Ok(None)
            }
            Err(_) => {
                self.metrics.counter_increment(RPC_WAIT_TIMEOUTS);
                Err(RpcError::WaitTimeout)
            }
        }
    }

    fn request_context(&self, parent: &Context, sid: &str) -> Context {
        let mut ctx = if self.request_timeout.is_zero() {
            Context { cancel: parent.cancel.clone(), deadline: parent.deadline, metadata: Vec::new() }
        } else {
            let mut c = parent.with_timeout(self.request_timeout);
            c.metadata = Vec::new();
            c
        };
        // Outgoing metadata replaces whatever the session context carried.
        ctx.metadata = vec![("sid".to_owned(), sid.to_owned()), ("protov".to_owned(), PROTO_VERSIONS.to_owned())];
        ctx
    }

    /// Upstream `retry`: runs `op` until it succeeds, fails with a non-retriable
    /// error, or the command timeout / session ends.
    async fn retry<T, F>(&self, ctx: &Context, sid: &str, mut op: F) -> Result<T, CallError>
    where
        F: for<'c> FnMut(&'c dyn RpcClient, &'c Context) -> BoxFuture<'c, Result<T, ClientError>>,
    {
        let (client, helper) = self.dialed().map_err(CallError::Failed)?;
        let command_ctx;
        let ctx = if self.command_timeout.is_zero() {
            ctx
        } else {
            command_ctx = ctx.with_timeout(self.command_timeout);
            &command_ctx
        };

        let mut attempt: u32 = 0;
        let mut was_exhausted = false;
        let mut last_seen: Option<RpcStatus> = None;

        loop {
            helper.ready().map_err(CallError::Failed)?;

            let request_ctx = self.request_context(ctx, sid);
            let err = match request_ctx.bound(op(client.as_ref(), &request_ctx)).await {
                Ok(res) => return Ok(res),
                Err(e) => e,
            };

            let status = match err {
                ClientError::Canceled => {
                    // Upstream decorates the last seen error ("canceled retrying"),
                    // transparently: the result is that error.
                    return Err(match last_seen {
                        Some(s) => CallError::Failed(RpcError::Status(s)),
                        None => CallError::Canceled,
                    });
                }
                ClientError::Status(s) => s,
                other => return Err(CallError::Failed(other.into())),
            };

            if status.code == Code::DeadlineExceeded {
                self.metrics.counter_increment(RPC_TIMEOUTS);
            }
            if !matches!(status.code, Code::ResourceExhausted | Code::Unavailable) {
                return Err(CallError::Failed(RpcError::Status(status)));
            }

            log(
                Level::Debug,
                "RPC failed",
                &[
                    Attr::new("sid", LogValue::String(sid.to_owned())),
                    Attr::new("code", LogValue::String(format!("{:?}", status.code))),
                    Attr::new("error", LogValue::String(status.message.clone())),
                ],
            );

            let interval = if status.code == Code::ResourceExhausted {
                if !was_exhausted {
                    attempt = 0;
                    was_exhausted = true;
                }
                self.barrier.exhausted();
                RETRY_EXHAUSTED_INTERVAL_MS
            } else {
                if was_exhausted {
                    was_exhausted = false;
                    attempt = 0;
                }
                RETRY_UNAVAILABLE_INTERVAL_MS
            };
            let delay = Duration::from_millis(2u64.saturating_pow(attempt).saturating_mul(interval));
            let interrupted = format!("rpc error: code = {:?} desc = {}", status.code, status.message);
            last_seen = Some(status);

            self.metrics.counter_increment(RPC_RETRIES);

            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                e = ctx.done() => {
                    // Upstream `errorx.DecorateMany` of two unrelated errors: an
                    // opaque wrapper, neither a status nor `context.Canceled`.
                    return Err(CallError::Failed(RpcError::Other(format!(
                        "synthetic.wrap: retry interrupted, cause: {e} (hidden: {interrupted})"
                    ))));
                }
            }

            attempt += 1;
        }
    }

    /// Connect RPC.
    pub async fn authenticate(&self, ctx: &Context, sid: &str, env: &SessionEnv) -> RpcOutcome<ConnectResult> {
        let _slot = match self.acquire(ctx).await {
            Ok(Some(slot)) => slot,
            Ok(None) => return RpcOutcome::none(),
            Err(e) => return RpcOutcome::failed(e),
        };

        let request = protocol::new_connect_message(env);
        self.metrics.counter_increment(RPC_CALLS);

        match self.retry(ctx, sid, |client, c| client.connect(c, request.clone())).await {
            Err(CallError::Canceled) => {
                self.metrics.counter_increment(RPC_CANCELED);
                RpcOutcome::none()
            }
            Err(CallError::Failed(e)) => {
                self.metrics.counter_increment(RPC_FAILURES);
                RpcOutcome::failed(e)
            }
            Ok(response) => {
                let parsed = protocol::parse_connect_response(&response);
                RpcOutcome { result: Some(parsed.result), error: parsed.error.map(RpcError::Application) }
            }
        }
    }

    async fn command(&self, ctx: &Context, sid: &str, request: protos::CommandMessage) -> RpcOutcome<CommandResult> {
        let _slot = match self.acquire(ctx).await {
            Ok(Some(slot)) => slot,
            Ok(None) => return RpcOutcome::none(),
            Err(e) => return RpcOutcome::failed(e),
        };

        let response = self.retry(ctx, sid, |client, c| client.command(c, request.clone())).await;

        // Upstream `parseCommandResponse`: cancelled calls are not counted as calls.
        if let Err(CallError::Canceled) = response {
            self.metrics.counter_increment(RPC_CANCELED);
            return RpcOutcome::none();
        }
        self.metrics.counter_increment(RPC_CALLS);
        match response {
            Err(CallError::Failed(e)) => {
                self.metrics.counter_increment(RPC_FAILURES);
                RpcOutcome::failed(e)
            }
            Err(CallError::Canceled) => RpcOutcome::none(),
            Ok(response) => {
                let parsed = protocol::parse_command_response(&response);
                RpcOutcome { result: Some(parsed.result), error: parsed.error.map(RpcError::Application) }
            }
        }
    }

    /// Command RPC with `subscribe`.
    pub async fn subscribe(
        &self,
        ctx: &Context,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> RpcOutcome<CommandResult> {
        self.command(ctx, sid, protocol::new_command_message(env, "subscribe", channel, id, "")).await
    }

    /// Command RPC with `unsubscribe`.
    pub async fn unsubscribe(
        &self,
        ctx: &Context,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> RpcOutcome<CommandResult> {
        self.command(ctx, sid, protocol::new_command_message(env, "unsubscribe", channel, id, "")).await
    }

    /// Command RPC with `message`.
    pub async fn perform(
        &self,
        ctx: &Context,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
        data: &str,
    ) -> RpcOutcome<CommandResult> {
        self.command(ctx, sid, protocol::new_command_message(env, "message", channel, id, data)).await
    }

    /// Disconnect RPC. Waits for a slot without the wait timeout (disconnect
    /// calls run the application's disconnect callbacks).
    pub async fn disconnect(
        &self,
        ctx: &Context,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        subscriptions: &[String],
    ) -> Result<(), RpcError> {
        let acquired = {
            let _pending = Pending::new(self.metrics.as_ref());
            self.barrier.wait(ctx).await
        };
        if acquired.is_err() {
            self.metrics.counter_increment(RPC_CANCELED);
            return Ok(());
        }
        let _slot = Slot(&self.barrier);

        let request = protocol::new_disconnect_message(env, id, subscriptions);
        self.metrics.counter_increment(RPC_CALLS);

        match self.retry(ctx, sid, |client, c| client.disconnect(c, request.clone())).await {
            Err(CallError::Canceled) => {
                self.metrics.counter_increment(RPC_CANCELED);
                Ok(())
            }
            Err(CallError::Failed(e)) => {
                self.metrics.counter_increment(RPC_FAILURES);
                Err(e)
            }
            Ok(response) => protocol::parse_disconnect_response(&response).map_err(|e| {
                self.metrics.counter_increment(RPC_FAILURES);
                RpcError::Application(e)
            }),
        }
    }

    /// The effective wait-for-slot timeout (zero: unlimited).
    pub fn wait_timeout(&self) -> Duration {
        self.wait_timeout
    }

    /// Active transport connections reported by the client helper.
    pub fn active_conns(&self) -> usize {
        self.dialed.get().map_or(0, |(_, helper)| helper.active_conns())
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
