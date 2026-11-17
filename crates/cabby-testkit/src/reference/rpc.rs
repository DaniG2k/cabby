//! Facade for upstream package `rpc`.
//!
//! Both backends talk to a real [`crate::fake_rpc::FakeRpc`] over loopback
//! sockets: the Go arm drives the pinned Go controller / HTTP service through the
//! bridge, the Rust arm drives `cabby_server::rpc`. Calls can be started in the
//! background ([`InFlight`]) so tests can hold them at fake gates and cancel
//! their contexts.

use std::sync::Arc;
use std::time::Duration;

use cabby_core::common::{CommandResult, ConnectResult, PresenceEvent, SessionEnv, Status, StreamMessage, StringMap};
use cabby_core::protocol::ApplicationError;
use cabby_core::protos;
use cabby_server::rpc as rs;
pub use cabby_server::rpc::{Code, Config, RpcError, RpcOutcome, RpcStatus, metric_names};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::metrics::Metrics;
use crate::rpc_doubles::{AlwaysReady, ForwardingService, TonicRpcClient};
use crate::runtime::{block_on_within, spawn};
use crate::{Backend, backend, bridge};

const AWAIT: Duration = Duration::from_secs(20);

// --- Go shapes -----------------------------------------------------------------

#[derive(Deserialize)]
struct GoError {
    text: String,
    wait_timeout: bool,
    code: Option<i32>,
    message: String,
}

fn go_error(e: GoError) -> RpcError {
    if e.wait_timeout {
        RpcError::WaitTimeout
    } else if let Some(m) = e.text.strip_prefix("Application error: ") {
        RpcError::Application(ApplicationError { message: m.to_owned() })
    } else if let Some(code) = e.code {
        RpcError::Status(RpcStatus { code: Code::from_i32(code), message: e.message })
    } else {
        RpcError::Other(e.text)
    }
}

#[derive(Deserialize)]
struct GoResult<T> {
    result: Option<T>,
    error: Option<GoError>,
}

#[derive(Deserialize)]
struct GoHandle {
    handle: i64,
    error: Option<GoError>,
}

fn go_status(code: i64) -> Status {
    match code {
        0 => Status::Success,
        1 => Status::Failure,
        2 => Status::Error,
        other => panic!("unknown Go common status {other}"),
    }
}

#[derive(Deserialize)]
struct GoStreamMessage {
    stream: String,
    data: String,
    #[serde(default)]
    offset: u64,
    #[serde(default)]
    epoch: String,
}

impl From<GoStreamMessage> for StreamMessage {
    fn from(g: GoStreamMessage) -> Self {
        // Broadcast metadata is not asserted by the rpc suite.
        StreamMessage { stream: g.stream, data: g.data, meta: None, offset: g.offset, epoch: g.epoch }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConnectResult {
    identifier: String,
    transmissions: Option<Vec<String>>,
    broadcasts: Option<Vec<GoStreamMessage>>,
    #[serde(rename = "CState")]
    cstate: Option<StringMap>,
    #[serde(rename = "IState")]
    istate: Option<StringMap>,
    disconnect_interest: i32,
    status: i64,
}

impl From<GoConnectResult> for ConnectResult {
    fn from(g: GoConnectResult) -> Self {
        ConnectResult {
            identifier: g.identifier,
            transmissions: g.transmissions.unwrap_or_default(),
            broadcasts: g.broadcasts.unwrap_or_default().into_iter().map(Into::into).collect(),
            cstate: g.cstate,
            istate: g.istate,
            disconnect_interest: g.disconnect_interest,
            status: go_status(g.status),
        }
    }
}

#[derive(Deserialize)]
struct GoPresenceEvent {
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default)]
    info: Option<Value>,
    id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoCommandResult {
    stop_all_streams: bool,
    disconnect: bool,
    streams: Option<Vec<String>>,
    stopped_streams: Option<Vec<String>>,
    transmissions: Option<Vec<String>>,
    broadcasts: Option<Vec<GoStreamMessage>>,
    #[serde(rename = "CState")]
    cstate: Option<StringMap>,
    #[serde(rename = "IState")]
    istate: Option<StringMap>,
    presence: Option<GoPresenceEvent>,
    disconnect_interest: i32,
    status: i64,
}

impl From<GoCommandResult> for CommandResult {
    fn from(g: GoCommandResult) -> Self {
        CommandResult {
            stop_all_streams: g.stop_all_streams,
            disconnect: g.disconnect,
            streams: g.streams.unwrap_or_default(),
            stopped_streams: g.stopped_streams.unwrap_or_default(),
            transmissions: g.transmissions.unwrap_or_default(),
            broadcasts: g.broadcasts.unwrap_or_default().into_iter().map(Into::into).collect(),
            cstate: g.cstate,
            istate: g.istate,
            presence: g.presence.map(|p| PresenceEvent { kind: p.kind, info: p.info, id: p.id }),
            disconnect_interest: g.disconnect_interest,
            status: go_status(g.status),
        }
    }
}

fn env_json(env: &SessionEnv) -> Value {
    json!({
        "url": env.url,
        "headers": env.headers,
        "identifiers": env.identifiers,
        "connection_state": env.connection_state,
        "channel_states": env.channel_states,
    })
}

#[derive(serde::Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    host: String,
    proxy_headers: Option<Vec<String>>,
    proxy_cookies: Option<Vec<String>>,
    concurrency: i64,
    #[serde(rename = "EnableTLS")]
    enable_tls: bool,
    #[serde(rename = "TLSVerify")]
    tls_verify: bool,
    #[serde(rename = "TLSRootCA")]
    tls_root_ca: String,
    max_recv_size: i64,
    max_send_size: i64,
    implementation: String,
    secret: String,
    request_timeout: i64,
    command_timeout: i64,
    wait_timeout: i64,
    #[serde(rename = "HTTPRequestTimeout")]
    http_request_timeout: i64,
    secret_base: String,
}

fn config_to_go(c: &Config) -> GoConfig {
    assert!(c.dial_fun.is_none(), "custom dialers are selected with `Dial`, not passed to Go");
    GoConfig {
        host: c.host.clone(),
        proxy_headers: Some(c.proxy_headers.clone()),
        proxy_cookies: Some(c.proxy_cookies.clone()),
        concurrency: c.concurrency,
        enable_tls: c.enable_tls,
        tls_verify: c.tls_verify,
        tls_root_ca: c.tls_root_ca.clone(),
        max_recv_size: c.max_recv_size,
        max_send_size: c.max_send_size,
        implementation: c.implementation.clone(),
        secret: c.secret.clone(),
        request_timeout: c.request_timeout,
        command_timeout: c.command_timeout,
        wait_timeout: c.wait_timeout,
        http_request_timeout: c.http_request_timeout,
        secret_base: c.secret_base.clone(),
    }
}

/// Go's nil and empty slices both become an empty `Vec` (the rpc suite never
/// distinguishes them).
fn config_from_go(g: GoConfig) -> Config {
    Config {
        host: g.host,
        proxy_headers: g.proxy_headers.unwrap_or_default(),
        proxy_cookies: g.proxy_cookies.unwrap_or_default(),
        concurrency: g.concurrency,
        enable_tls: g.enable_tls,
        tls_verify: g.tls_verify,
        tls_root_ca: g.tls_root_ca,
        max_recv_size: g.max_recv_size,
        max_send_size: g.max_send_size,
        implementation: g.implementation,
        dial_fun: None,
        secret: g.secret,
        request_timeout: g.request_timeout,
        command_timeout: g.command_timeout,
        wait_timeout: g.wait_timeout,
        http_request_timeout: g.http_request_timeout,
        secret_base: g.secret_base,
    }
}

// --- Config ----------------------------------------------------------------------

/// `rpc.NewConfig()`
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => config_from_go(bridge::call("rpc.NewConfig", json!({}))),
    }
}

fn config_op<T: DeserializeOwned>(c: &Config, op: &str) -> T {
    bridge::call("rpc.Config", json!({"Config": config_to_go(c), "Op": op}))
}

/// `c.Impl()`
pub fn config_impl(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.implementation(),
        Backend::Go => config_op(c, "Impl"),
    }
}

/// `c.ToToml()`
pub fn config_to_toml(c: &Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => config_op(c, "ToToml"),
    }
}

/// Decodes `toml` on top of `base` (Go: `toml.Decode(s, &base)`).
pub fn config_decode_toml(base: &Config, toml: &str) -> Result<Config, String> {
    match backend() {
        Backend::Rust => {
            let mut c = base.clone();
            c.decode_toml(toml).map(|()| c)
        }
        Backend::Go => {
            let r: GoResult<GoConfig> =
                bridge::call("rpc.Config.DecodeToml", json!({"Base": config_to_go(base), "Toml": toml}));
            match r.error {
                Some(e) => Err(e.text),
                None => Ok(config_from_go(r.result.expect("config"))),
            }
        }
    }
}

// --- Contexts ----------------------------------------------------------------------

/// A call context (Go `context.Context`): cancellable, optional deadline, optional
/// incoming metadata.
pub struct Ctx {
    rust: rs::Context,
    go: i64,
}

impl Ctx {
    fn make(cancelled: bool, timeout: Option<Duration>, metadata: &[(&str, &str)]) -> Self {
        let rust = rs::Context {
            cancel: Default::default(),
            deadline: timeout.map(|t| tokio::time::Instant::now() + t),
            metadata: metadata.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
        };
        if cancelled {
            rust.cancel.cancel();
        }
        let go = match backend() {
            Backend::Rust => 0,
            Backend::Go => bridge::call(
                "rpcm.NewContext",
                json!({"Canceled": cancelled, "TimeoutMs": timeout.map_or(0, |t| t.as_millis() as u64), "Metadata": metadata}),
            ),
        };
        Self { rust, go }
    }

    /// `context.Background()`
    pub fn background() -> Self {
        Self::make(false, None, &[])
    }

    /// A context cancelled before use.
    pub fn cancelled() -> Self {
        Self::make(true, None, &[])
    }

    /// `context.WithTimeout(context.Background(), timeout)`
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::make(false, Some(timeout), &[])
    }

    /// `metadata.NewIncomingContext(context.Background(), metadata.Pairs(...))`
    pub fn with_incoming_metadata(pairs: &[(&str, &str)]) -> Self {
        Self::make(false, None, pairs)
    }

    /// Cancels a context whose call is waiting for an RPC slot (or not started).
    pub fn cancel(&self) {
        self.rust.cancel.cancel();
        if backend() == Backend::Go {
            let _: bool = bridge::call("rpcm.CancelContext", json!({"Ctx": self.go}));
        }
    }

    /// Cancels a context while its RPC is in flight at the RPC service.
    ///
    /// Against the real Go gRPC client this was observed to make `Authenticate`
    /// return `(nil, status Canceled "context canceled")` rather than upstream's
    /// mock-based `(nil, nil)`, so the Go arm does not claim the upstream result.
    pub fn cancel_in_flight(&self) {
        match backend() {
            Backend::Rust => self.rust.cancel.cancel(),
            Backend::Go => unimplemented!(
                "not_observable: upstream's mock pb.RPCClient returns a bare context.Canceled; every real Go \
                 transport (gRPC, in-process grpchan, HTTP) turns a mid-call cancellation into a gRPC status \
                 error, so Controller's errors.Is(err, context.Canceled) branch is unreachable through the \
                 exported API"
            ),
        }
    }
}

// --- Background calls ----------------------------------------------------------------

type Decode<T> = fn(Value) -> T;

/// A call running in the background.
pub enum InFlight<T: Send + 'static> {
    Rust(tokio::task::JoinHandle<T>),
    Go(i64, Decode<T>),
}

impl<T: Send + 'static> InFlight<T> {
    /// Waits for the result (bounded).
    pub fn wait(self) -> T {
        match self {
            InFlight::Rust(j) => {
                block_on_within(AWAIT, j).unwrap_or_else(|e| std::panic::resume_unwind(e.into_panic()))
            }
            InFlight::Go(c, decode) => {
                decode(bridge::call("rpcm.Await", json!({"Call": c, "TimeoutMs": AWAIT.as_millis() as u64})))
            }
        }
    }
}

fn decode_connect(v: Value) -> RpcOutcome<ConnectResult> {
    let r: GoResult<GoConnectResult> = serde_json::from_value(v).expect("connect outcome");
    RpcOutcome { result: r.result.map(Into::into), error: r.error.map(go_error) }
}

fn decode_command(v: Value) -> RpcOutcome<CommandResult> {
    let r: GoResult<GoCommandResult> = serde_json::from_value(v).expect("command outcome");
    RpcOutcome { result: r.result.map(Into::into), error: r.error.map(go_error) }
}

fn decode_unit(v: Value) -> Result<(), RpcError> {
    let r: GoResult<Value> = serde_json::from_value(v).expect("outcome");
    r.error.map_or(Ok(()), |e| Err(go_error(e)))
}

// --- Controller --------------------------------------------------------------------------

/// How the controller reaches the RPC service.
#[derive(Debug, Clone)]
pub enum Dial {
    /// As configured (`config.host`, default gRPC or HTTP dialer).
    Config,
    /// A plain gRPC client to `target` with an always-ready helper: the
    /// equivalent of upstream `NewTestController`'s injected mock client and
    /// `MockState{ready: true}`.
    TestClient(String),
    /// `NewInprocessServiceDialer(service, MockState{ready: true})` with a service
    /// that forwards every call (and its incoming metadata) to `target`.
    Inprocess(String),
}

#[derive(Clone)]
pub enum Controller {
    Rust(Arc<rs::Controller>),
    Go(i64),
}

impl Controller {
    /// `rpc.NewController(metrics, &config, logger)`
    pub fn new(metrics: &Metrics, config: &Config, dial: Dial) -> Result<Self, RpcError> {
        match backend() {
            Backend::Rust => {
                let mut config = config.clone();
                match dial {
                    Dial::Config => {}
                    Dial::TestClient(target) => config.dial_fun = Some(rs::DialFun(TonicRpcClient::dialer(&target))),
                    Dial::Inprocess(target) => {
                        config.dial_fun = Some(rs::DialFun(rs::new_inprocess_service_dialer(
                            Arc::new(ForwardingService::new(&target)),
                            Arc::new(AlwaysReady),
                        )))
                    }
                }
                let instrumenter: Arc<dyn cabby_server::metrics::Instrumenter> = metrics.rust();
                rs::Controller::new(instrumenter, config).map(|c| Controller::Rust(Arc::new(c)))
            }
            Backend::Go => {
                let (kind, target) = match dial {
                    Dial::Config => ("", String::new()),
                    Dial::TestClient(t) => ("client", t),
                    Dial::Inprocess(t) => ("inprocess", t),
                };
                let r: GoHandle = bridge::call(
                    "rpc.NewController",
                    json!({"Metrics": metrics.go_handle(), "Config": config_to_go(config), "Dial": kind, "Target": target}),
                );
                match r.error {
                    Some(e) => Err(go_error(e)),
                    None => Ok(Controller::Go(r.handle)),
                }
            }
        }
    }

    pub fn start(&self) -> Result<(), RpcError> {
        match self {
            Controller::Rust(c) => c.start(),
            Controller::Go(h) => decode_unit(bridge::call("rpc.Controller.Start", json!({"Handle": h}))),
        }
    }

    pub fn shutdown(&self) -> Result<(), RpcError> {
        match self {
            Controller::Rust(c) => block_on_within(AWAIT, c.shutdown()),
            Controller::Go(h) => {
                let call: i64 = bridge::call("rpc.Controller.Shutdown", json!({"Handle": h}));
                InFlight::Go(call, decode_unit).wait()
            }
        }
    }

    fn go_call(h: i64, ctx: &Ctx, method: &str, args: Value) -> i64 {
        let mut a = args;
        a["Handle"] = json!(h);
        a["Ctx"] = json!(ctx.go);
        a["Method"] = json!(method);
        bridge::call("rpc.Controller.Call", a)
    }

    pub fn spawn_authenticate(&self, ctx: &Ctx, sid: &str, env: &SessionEnv) -> InFlight<RpcOutcome<ConnectResult>> {
        match self {
            Controller::Rust(c) => {
                let (c, ctx, sid, env) = (Arc::clone(c), ctx.rust.clone(), sid.to_owned(), env.clone());
                InFlight::Rust(spawn(async move { c.authenticate(&ctx, &sid, &env).await }))
            }
            Controller::Go(h) => InFlight::Go(
                Self::go_call(*h, ctx, "Authenticate", json!({"Sid": sid, "Env": env_json(env)})),
                decode_connect,
            ),
        }
    }

    pub fn authenticate(&self, ctx: &Ctx, sid: &str, env: &SessionEnv) -> RpcOutcome<ConnectResult> {
        self.spawn_authenticate(ctx, sid, env).wait()
    }

    #[allow(clippy::too_many_arguments)] // mirrors the upstream method signatures
    fn spawn_command(
        &self,
        ctx: &Ctx,
        method: &'static str,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
        data: &str,
    ) -> InFlight<RpcOutcome<CommandResult>> {
        match self {
            Controller::Rust(c) => {
                let (c, ctx) = (Arc::clone(c), ctx.rust.clone());
                let (sid, env, id, channel, data) =
                    (sid.to_owned(), env.clone(), id.to_owned(), channel.to_owned(), data.to_owned());
                InFlight::Rust(spawn(async move {
                    match method {
                        "Subscribe" => c.subscribe(&ctx, &sid, &env, &id, &channel).await,
                        "Unsubscribe" => c.unsubscribe(&ctx, &sid, &env, &id, &channel).await,
                        _ => c.perform(&ctx, &sid, &env, &id, &channel, &data).await,
                    }
                }))
            }
            Controller::Go(h) => InFlight::Go(
                Self::go_call(
                    *h,
                    ctx,
                    method,
                    json!({"Sid": sid, "Env": env_json(env), "ID": id, "Channel": channel, "Data": data}),
                ),
                decode_command,
            ),
        }
    }

    pub fn spawn_subscribe(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> InFlight<RpcOutcome<CommandResult>> {
        self.spawn_command(ctx, "Subscribe", sid, env, id, channel, "")
    }

    pub fn subscribe(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> RpcOutcome<CommandResult> {
        self.spawn_subscribe(ctx, sid, env, id, channel).wait()
    }

    pub fn spawn_unsubscribe(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
    ) -> InFlight<RpcOutcome<CommandResult>> {
        self.spawn_command(ctx, "Unsubscribe", sid, env, id, channel, "")
    }

    pub fn spawn_perform(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
        data: &str,
    ) -> InFlight<RpcOutcome<CommandResult>> {
        self.spawn_command(ctx, "Perform", sid, env, id, channel, data)
    }

    pub fn perform(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        channel: &str,
        data: &str,
    ) -> RpcOutcome<CommandResult> {
        self.spawn_perform(ctx, sid, env, id, channel, data).wait()
    }

    pub fn spawn_disconnect(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        subscriptions: &[&str],
    ) -> InFlight<Result<(), RpcError>> {
        let subs: Vec<String> = subscriptions.iter().map(|s| (*s).to_owned()).collect();
        match self {
            Controller::Rust(c) => {
                let (c, ctx, sid, env, id) =
                    (Arc::clone(c), ctx.rust.clone(), sid.to_owned(), env.clone(), id.to_owned());
                InFlight::Rust(spawn(async move { c.disconnect(&ctx, &sid, &env, &id, &subs).await }))
            }
            Controller::Go(h) => InFlight::Go(
                Self::go_call(
                    *h,
                    ctx,
                    "Disconnect",
                    json!({"Sid": sid, "Env": env_json(env), "ID": id, "Subscriptions": subs}),
                ),
                decode_unit,
            ),
        }
    }

    pub fn disconnect(
        &self,
        ctx: &Ctx,
        sid: &str,
        env: &SessionEnv,
        id: &str,
        subscriptions: &[&str],
    ) -> Result<(), RpcError> {
        self.spawn_disconnect(ctx, sid, env, id, subscriptions).wait()
    }

    /// Upstream unexported `controller.waitTimeout`.
    pub fn wait_timeout(&self) -> Duration {
        match self {
            Controller::Rust(c) => c.wait_timeout(),
            Controller::Go(_) => unimplemented!(
                "not_observable: Controller.waitTimeout is unexported and only observable as the wall-clock \
                 time a call waits for a slot"
            ),
        }
    }

    /// Upstream `controller.clientState.ActiveConns()` (unexported field).
    pub fn active_conns(&self) -> usize {
        match self {
            Controller::Rust(c) => c.active_conns(),
            Controller::Go(_) => unimplemented!(
                "not_observable: Controller.clientState is unexported; the grpc_active_conn_num gauge is only \
                 refreshed every 10s"
            ),
        }
    }
}

// --- HTTP service ----------------------------------------------------------------------------

#[derive(Clone)]
pub enum HttpService {
    Rust(Arc<rs::http::HttpService>),
    Go(i64),
}

fn status_err(s: RpcStatus) -> RpcError {
    RpcError::Status(s)
}

fn decode_response<T: DeserializeOwned>(v: Value) -> Result<T, RpcError> {
    let r: GoResult<T> = serde_json::from_value(v).expect("service outcome");
    match r.error {
        Some(e) => Err(go_error(e)),
        None => Ok(r.result.expect("response")),
    }
}

impl HttpService {
    /// `rpc.NewHTTPService(&config)`
    pub fn new(config: &Config) -> Result<Self, RpcError> {
        match backend() {
            Backend::Rust => rs::http::HttpService::new(config).map(|s| HttpService::Rust(Arc::new(s))),
            Backend::Go => {
                let r: GoHandle = bridge::call("rpc.NewHTTPService", json!({"Config": config_to_go(config)}));
                match r.error {
                    Some(e) => Err(go_error(e)),
                    None => Ok(HttpService::Go(r.handle)),
                }
            }
        }
    }

    fn go_call<T: Send + 'static>(h: i64, ctx: &Ctx, method: &str, request: Value, decode: Decode<T>) -> InFlight<T> {
        let call: i64 = bridge::call(
            "rpc.HTTPService.Call",
            json!({"Handle": h, "Ctx": ctx.go, "Method": method, "Request": request}),
        );
        InFlight::Go(call, decode)
    }

    pub fn spawn_connect(
        &self,
        ctx: &Ctx,
        request: &protos::ConnectionRequest,
    ) -> InFlight<Result<protos::ConnectionResponse, RpcError>> {
        match self {
            HttpService::Rust(s) => {
                let (s, ctx, req) = (Arc::clone(s), ctx.rust.clone(), request.clone());
                InFlight::Rust(spawn(async move {
                    rs::RpcService::connect(s.as_ref(), &ctx, req).await.map_err(status_err)
                }))
            }
            HttpService::Go(h) => Self::go_call(*h, ctx, "Connect", json!(request), decode_response),
        }
    }

    pub fn connect(
        &self,
        ctx: &Ctx,
        request: &protos::ConnectionRequest,
    ) -> Result<protos::ConnectionResponse, RpcError> {
        self.spawn_connect(ctx, request).wait()
    }

    pub fn command(&self, ctx: &Ctx, request: &protos::CommandMessage) -> Result<protos::CommandResponse, RpcError> {
        match self {
            HttpService::Rust(s) => {
                let (s, ctx, req) = (Arc::clone(s), ctx.rust.clone(), request.clone());
                InFlight::Rust(spawn(async move {
                    rs::RpcService::command(s.as_ref(), &ctx, req).await.map_err(status_err)
                }))
            }
            HttpService::Go(h) => Self::go_call(*h, ctx, "Command", json!(request), decode_response),
        }
        .wait()
    }

    pub fn disconnect(
        &self,
        ctx: &Ctx,
        request: &protos::DisconnectRequest,
    ) -> Result<protos::DisconnectResponse, RpcError> {
        match self {
            HttpService::Rust(s) => {
                let (s, ctx, req) = (Arc::clone(s), ctx.rust.clone(), request.clone());
                InFlight::Rust(spawn(async move {
                    rs::RpcService::disconnect(s.as_ref(), &ctx, req).await.map_err(status_err)
                }))
            }
            HttpService::Go(h) => Self::go_call(*h, ctx, "Disconnect", json!(request), decode_response),
        }
        .wait()
    }

    /// `rpc.NewHTTPClientHelper(service).Ready()`
    pub fn helper_ready(&self) -> Result<(), RpcError> {
        match self {
            HttpService::Rust(s) => rs::ClientHelper::ready(&rs::http::HttpClientHelper::new(Arc::clone(s))),
            HttpService::Go(h) => decode_unit(bridge::call("rpc.HTTPClientHelper.Ready", json!({"Handle": h}))),
        }
    }
}
