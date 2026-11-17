//! Facade for upstream package `api`.
//!
//! Upstream handler tests use `mocks.NewBroker(t)` and `mocks.NewHandler(t)`
//! (testify; expectations are asserted on cleanup) and drive the server with
//! `httptest.NewRecorder`. [`MockBroker`] and [`MockHandler`] are data-driven
//! equivalents: under Go the bridge wraps the same expectations in Go doubles
//! of `broker.Broker` and `broadcast.Handler`, and the calls they received are
//! recorded back on these mocks.

use std::sync::{Arc, Mutex};

use cabby_core::broker::{BrokerError, Cacheable, PresenceInfoOptions};
use cabby_core::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use cabby_server::api::{self as rs, BroadcastHandler};
/// Re-exported so translated tests need no direct `cabby-server` dependency.
pub use cabby_server::api::{ApiError, Config, DEFAULT_API_PATH, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::go_types::*;
use crate::{Backend, backend, bridge};

// ------------------------------------------------------------------ config

/// `api.Config` as encoding/json writes it (exported field names).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoConfig {
    host: String,
    port: u16,
    path: String,
    secret: String,
    secret_base: String,
    #[serde(rename = "AddCORSHeaders")]
    add_cors_headers: bool,
    #[serde(rename = "CORSHosts")]
    cors_hosts: String,
    max_body_size: i64,
}

impl From<&Config> for GoConfig {
    fn from(c: &Config) -> Self {
        GoConfig {
            host: c.host.clone(),
            port: c.port,
            path: c.path.clone(),
            secret: c.secret.clone(),
            secret_base: c.secret_base.clone(),
            add_cors_headers: c.add_cors_headers,
            cors_hosts: c.cors_hosts.clone(),
            max_body_size: c.max_body_size,
        }
    }
}

impl From<GoConfig> for Config {
    fn from(g: GoConfig) -> Self {
        Config {
            host: g.host,
            port: g.port,
            path: g.path,
            secret: g.secret,
            secret_base: g.secret_base,
            add_cors_headers: g.add_cors_headers,
            cors_hosts: g.cors_hosts,
            max_body_size: g.max_body_size,
        }
    }
}

#[derive(Deserialize)]
struct GoConfigReply {
    config: GoConfig,
    error: Option<String>,
}

impl GoConfigReply {
    fn apply(self, conf: &mut Config) -> Result<(), ApiError> {
        *conf = self.config.into();
        self.error.map_or(Ok(()), |message| Err(ApiError { message }))
    }
}

/// `api.NewConfig()`.
pub fn new_config() -> Config {
    match backend() {
        Backend::Rust => Config::new(),
        Backend::Go => bridge::call::<_, GoConfig>("api.NewConfig", json!({})).into(),
    }
}

/// `config.IsSecured()`.
pub fn config_is_secured(conf: &Config) -> bool {
    match backend() {
        Backend::Rust => conf.is_secured(),
        Backend::Go => bridge::call("api.Config.IsSecured", json!({"Config": GoConfig::from(conf)})),
    }
}

/// `config.DeriveSecret()`.
pub fn config_derive_secret(conf: &mut Config) -> Result<(), ApiError> {
    match backend() {
        Backend::Rust => conf.derive_secret(),
        Backend::Go => {
            bridge::call::<_, GoConfigReply>("api.Config.DeriveSecret", json!({"Config": GoConfig::from(&*conf)}))
                .apply(conf)
        }
    }
}

/// `config.ToToml()`.
pub fn config_to_toml(conf: &Config) -> String {
    match backend() {
        Backend::Rust => conf.to_toml(),
        Backend::Go => bridge::call("api.Config.ToToml", json!({"Config": GoConfig::from(conf)})),
    }
}

/// `toml.Decode(s, &config)`.
pub fn config_merge_toml(conf: &mut Config, toml: &str) -> Result<(), ApiError> {
    match backend() {
        Backend::Rust => conf.merge_toml(toml),
        Backend::Go => bridge::call::<_, GoConfigReply>(
            "api.Config.DecodeToml",
            json!({"Config": GoConfig::from(&*conf), "Toml": toml}),
        )
        .apply(conf),
    }
}

// ------------------------------------------------------------ authenticator

fn request_to_go(req: &HttpRequest) -> Value {
    json!({
        "method": req.method,
        "path": req.path,
        "headers": req.headers,
        "body": String::from_utf8(req.body.clone()).expect("UTF-8 request body"),
    })
}

/// `api.NewAuthenticator(secret)` under test.
pub struct Authenticator {
    secret: String,
    rust: Option<rs::Authenticator>,
    go: Option<GoAuthenticator>,
}

#[derive(Deserialize)]
struct GoAuthenticator {
    is_enabled: bool,
    secret: String,
}

#[derive(Deserialize)]
struct GoNewAuthenticator {
    #[serde(flatten)]
    auth: GoAuthenticator,
    error: Option<String>,
}

/// `api.NewAuthenticator(secret)`.
pub fn new_authenticator(secret: &str) -> Result<Authenticator, ApiError> {
    match backend() {
        Backend::Rust => {
            rs::Authenticator::new(secret).map(|a| Authenticator { secret: secret.into(), rust: Some(a), go: None })
        }
        Backend::Go => {
            let r: GoNewAuthenticator = bridge::call("api.NewAuthenticator", json!({"Secret": secret}));
            match r.error {
                Some(message) => Err(ApiError { message }),
                None => Ok(Authenticator { secret: secret.into(), rust: None, go: Some(r.auth) }),
            }
        }
    }
}

impl Authenticator {
    pub fn is_enabled(&self) -> bool {
        match (&self.rust, &self.go) {
            (Some(a), _) => a.is_enabled(),
            (_, Some(g)) => g.is_enabled,
            _ => unreachable!(),
        }
    }

    pub fn secret(&self) -> String {
        match (&self.rust, &self.go) {
            (Some(a), _) => a.secret(),
            (_, Some(g)) => g.secret.clone(),
            _ => unreachable!(),
        }
    }

    pub fn authenticate(&self, req: &HttpRequest) -> bool {
        match &self.rust {
            Some(a) => a.authenticate(req),
            None => bridge::call(
                "api.Authenticator.Authenticate",
                json!({"Secret": self.secret, "Request": request_to_go(req)}),
            ),
        }
    }
}

// ------------------------------------------------------------------ mocks

/// A call received by the broker mock.
#[derive(Debug, Clone, PartialEq)]
pub enum BrokerCall {
    SupportsPresence,
    /// `PresenceInfo(stream, opts...)`. mockery unrolls the variadic, so
    /// upstream's `On("PresenceInfo", "a", mock.Anything)` matches exactly one
    /// option of any value: `options` is how many were passed, values ignored.
    PresenceInfo {
        stream: String,
        options: usize,
    },
    /// Any other `broker.Broker` method (always unexpected in these tests).
    Other(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BrokerReturn {
    Bool(bool),
    PresenceInfo(Result<PresenceInfo, BrokerError>),
}

/// `mocks.NewBroker(t)`: unexpected calls panic; [`MockBroker::assert_expectations`]
/// is the cleanup check.
#[derive(Default)]
pub struct MockBroker {
    expectations: Mutex<Vec<(BrokerCall, BrokerReturn)>>,
    calls: Mutex<Vec<BrokerCall>>,
    /// Calls with no matching expectation (also rejected by a panic in `answer`,
    /// which a server thread could swallow).
    unmatched: Mutex<Vec<BrokerCall>>,
}

impl MockBroker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn on(&self, call: BrokerCall, ret: BrokerReturn) {
        self.expectations.lock().unwrap().push((call, ret));
    }

    /// Every expectation was called at least once and no call went unmatched
    /// (testify `AssertExpectations`, plus its failure on unexpected calls).
    pub fn assert_expectations(&self) {
        let unmatched = self.unmatched.lock().unwrap();
        assert!(unmatched.is_empty(), "unexpected broker calls: {unmatched:?}");
        let calls = self.calls.lock().unwrap();
        for (c, _) in self.expectations.lock().unwrap().iter() {
            assert!(calls.contains(c), "expected broker call {c:?} was not made; calls: {calls:?}");
        }
    }

    fn answer(&self, call: BrokerCall) -> BrokerReturn {
        let ret = self.expectations.lock().unwrap().iter().find(|(c, _)| *c == call).map(|(_, r)| r.clone());
        self.calls.lock().unwrap().push(call.clone());
        if ret.is_none() {
            self.unmatched.lock().unwrap().push(call.clone());
        }
        ret.unwrap_or_else(|| panic!("mock: unexpected broker call {call:?}"))
    }

    fn expectations_to_go(&self) -> Value {
        let exps = self.expectations.lock().unwrap();
        Value::Array(
            exps.iter()
                .map(|(c, r)| {
                    let (method, args) = match c {
                        BrokerCall::SupportsPresence => ("SupportsPresence", json!({})),
                        BrokerCall::PresenceInfo { stream, options } => {
                            ("PresenceInfo", json!({"stream": stream, "options": options}))
                        }
                        BrokerCall::Other(m) => (m.as_str(), json!({})),
                    };
                    let returns = match r {
                        BrokerReturn::Bool(b) => json!({"result": b, "error": null}),
                        BrokerReturn::PresenceInfo(Ok(i)) => json!({"result": presence_info_to_go(i), "error": null}),
                        BrokerReturn::PresenceInfo(Err(e)) => json!({"result": null, "error": e.message}),
                    };
                    json!({"method": method, "args": args, "returns": returns})
                })
                .collect(),
        )
    }

    fn replay(&self, calls: &Value) {
        for c in calls.as_array().into_iter().flatten() {
            let call = match c["method"].as_str().unwrap_or_default() {
                "SupportsPresence" => BrokerCall::SupportsPresence,
                "PresenceInfo" => BrokerCall::PresenceInfo {
                    stream: c["args"]["stream"].as_str().unwrap_or_default().into(),
                    options: c["args"]["options"].as_u64().expect("PresenceInfo option count") as usize,
                },
                other => BrokerCall::Other(other.into()),
            };
            self.answer(call);
        }
    }

    fn unexpected<T>(&self, method: &str) -> T {
        self.answer(BrokerCall::Other(method.into()));
        unreachable!("an `Other` call never has an expectation")
    }
}

impl cabby_core::broker::Broker for MockBroker {
    fn start(&self) -> Result<(), BrokerError> {
        self.unexpected("Start")
    }
    fn shutdown(&self) -> Result<(), BrokerError> {
        self.unexpected("Shutdown")
    }
    fn announce(&self) -> String {
        self.unexpected("Announce")
    }
    fn handle_broadcast(&self, _msg: &StreamMessage) -> Result<(), BrokerError> {
        self.unexpected("HandleBroadcast")
    }
    fn handle_command(&self, _msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.unexpected("HandleCommand")
    }
    fn subscribe(&self, _stream: &str) -> String {
        self.unexpected("Subscribe")
    }
    fn unsubscribe(&self, _stream: &str) -> String {
        self.unexpected("Unsubscribe")
    }
    fn history_from(&self, _stream: &str, _epoch: &str, _offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.unexpected("HistoryFrom")
    }
    fn history_since(&self, _stream: &str, _ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.unexpected("HistorySince")
    }
    fn commit_session(&self, _sid: &str, _session: &dyn Cacheable) -> Result<(), BrokerError> {
        self.unexpected("CommitSession")
    }
    fn restore_session(&self, _sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        self.unexpected("RestoreSession")
    }
    fn touch_session(&self, _sid: &str) -> Result<(), BrokerError> {
        self.unexpected("TouchSession")
    }
    fn supports_presence(&self) -> bool {
        match self.answer(BrokerCall::SupportsPresence) {
            BrokerReturn::Bool(b) => b,
            other => panic!("mock: SupportsPresence returns a bool, got {other:?}"),
        }
    }
    fn presence_add(
        &self,
        _stream: &str,
        _sid: &str,
        _pid: &str,
        _info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        self.unexpected("PresenceAdd")
    }
    fn presence_remove(&self, _stream: &str, _sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        self.unexpected("PresenceRemove")
    }
    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        // `Some(opts)` is Go's single `WithPresenceInfoOptions(opts)` argument.
        match self.answer(BrokerCall::PresenceInfo { stream: stream.into(), options: usize::from(opts.is_some()) }) {
            BrokerReturn::PresenceInfo(r) => r,
            other => panic!("mock: PresenceInfo returns presence info, got {other:?}"),
        }
    }
    fn touch_presence(&self, _sid: &str) -> Result<(), BrokerError> {
        self.unexpected("TouchPresence")
    }
    fn peak(&self, _stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.unexpected("Peak")
    }
}

/// A `HandleBroadcast(payload)` expectation and its return value.
type HandlerExpectation = (Vec<u8>, Result<(), ApiError>);

/// `mocks.NewHandler(t)` for `broadcast.Handler`.
#[derive(Default)]
pub struct MockHandler {
    expectations: Mutex<Vec<HandlerExpectation>>,
    calls: Mutex<Vec<(String, Vec<u8>)>>,
    /// Calls with no matching expectation (see [`MockBroker`]).
    unmatched: Mutex<Vec<(String, Vec<u8>)>>,
}

impl MockHandler {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// `handler.On("HandleBroadcast", payload).Return(ret)`.
    pub fn on_handle_broadcast(&self, payload: &[u8], ret: Result<(), ApiError>) {
        self.expectations.lock().unwrap().push((payload.to_vec(), ret));
    }

    /// Every expectation was called at least once and no call went unmatched.
    pub fn assert_expectations(&self) {
        let unmatched = self.unmatched.lock().unwrap();
        assert!(
            unmatched.is_empty(),
            "unexpected handler calls: {:?}",
            unmatched.iter().map(|(m, p)| format!("{m}({})", String::from_utf8_lossy(p))).collect::<Vec<_>>()
        );
        let calls = self.calls.lock().unwrap();
        for (p, _) in self.expectations.lock().unwrap().iter() {
            assert!(
                calls.contains(&("HandleBroadcast".into(), p.clone())),
                "expected HandleBroadcast({:?}) was not made",
                String::from_utf8_lossy(p)
            );
        }
    }

    fn answer(&self, method: &str, payload: &[u8]) -> Result<(), ApiError> {
        self.calls.lock().unwrap().push((method.into(), payload.to_vec()));
        let ret = if method == "HandleBroadcast" {
            let exps = self.expectations.lock().unwrap();
            exps.iter().find(|(p, _)| p == payload).map(|(_, r)| r.clone())
        } else {
            None
        };
        if ret.is_none() {
            self.unmatched.lock().unwrap().push((method.into(), payload.to_vec()));
        }
        ret.unwrap_or_else(|| panic!("mock: unexpected handler call {method}({:?})", String::from_utf8_lossy(payload)))
    }

    fn expectations_to_go(&self) -> Value {
        let exps = self.expectations.lock().unwrap();
        Value::Array(
            exps.iter()
                .map(|(p, r)| {
                    json!({"method": "HandleBroadcast",
                           "args": {"payload": String::from_utf8_lossy(p)},
                           "returns": {"result": null, "error": r.as_ref().err().map(|e| e.message.clone())}})
                })
                .collect(),
        )
    }

    fn replay(&self, calls: &Value) {
        for c in calls.as_array().into_iter().flatten() {
            let payload = c["args"]["payload"].as_str().unwrap_or_default();
            let _ = self.answer(c["method"].as_str().unwrap_or_default(), payload.as_bytes());
        }
    }
}

impl BroadcastHandler for MockHandler {
    fn handle_broadcast(&self, payload: &[u8]) -> Result<(), ApiError> {
        self.answer("HandleBroadcast", payload)
    }
    fn handle_pubsub(&self, payload: &[u8]) {
        let _ = self.answer("HandlePubSub", payload);
    }
}

// ------------------------------------------------------------------ server

/// `api.NewServer(&config, brk, handler, logger)` under test. Under Go, each
/// operation rebuilds the server from the same inputs (handlers are stateless)
/// and drives it through the HTTP server `Start` registered it on.
pub struct Server {
    conf: Config,
    broker: Arc<MockBroker>,
    handler: Arc<MockHandler>,
    rust: Option<rs::Server>,
}

#[derive(Deserialize)]
struct GoServed {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    broker_calls: Value,
    #[serde(default)]
    handler_calls: Value,
}

impl Server {
    fn go(&self, action: &str, req: Option<&HttpRequest>) -> GoServed {
        let reply: GoServed = bridge::call(
            "api.Server",
            json!({
                "Config": GoConfig::from(&self.conf),
                "Broker": self.broker.expectations_to_go(),
                "Handler": self.handler.expectations_to_go(),
                "Action": action,
                "Request": req.map(request_to_go),
            }),
        );
        self.broker.replay(&reply.broker_calls);
        self.handler.replay(&reply.handler_calls);
        reply
    }

    pub fn start(&self) -> Result<(), ApiError> {
        match &self.rust {
            Some(s) => s.start(),
            None => self.go("start", None).error.map_or(Ok(()), |message| Err(ApiError { message })),
        }
    }

    /// `api.server.ServeHTTP(httptest.NewRecorder(), req)`.
    pub fn serve(&self, req: &HttpRequest) -> HttpResponse {
        match &self.rust {
            Some(s) => s.serve(req),
            None => {
                let r = self.go("serve", Some(req));
                if let Some(e) = r.error {
                    panic!("Go api server failed to start: {e}");
                }
                HttpResponse { status: r.status, headers: r.headers, body: r.body.into_bytes() }
            }
        }
    }

    pub fn shutdown(&self) -> Result<(), ApiError> {
        match &self.rust {
            Some(s) => s.shutdown(),
            // Every Go operation shuts its server down before returning.
            None => Ok(()),
        }
    }
}

/// `api.NewServer(&config, brk, handler, slog.Default())`; derives `config.secret`.
pub fn new_server(conf: &mut Config, broker: &Arc<MockBroker>, handler: &Arc<MockHandler>) -> Result<Server, ApiError> {
    match backend() {
        Backend::Rust => rs::Server::new(conf, broker.clone(), handler.clone()).map(|s| Server {
            conf: conf.clone(),
            broker: broker.clone(),
            handler: handler.clone(),
            rust: Some(s),
        }),
        Backend::Go => {
            let r: GoConfigReply = bridge::call("api.NewServer", json!({"Config": GoConfig::from(&*conf)}));
            r.apply(conf)?;
            Ok(Server { conf: conf.clone(), broker: broker.clone(), handler: handler.clone(), rust: None })
        }
    }
}

/// Upstream's `defer api.Shutdown(context.Background()) // nolint:errcheck`:
/// shuts the server down when dropped, ignoring the result, also when the test
/// fails earlier.
pub struct ShutdownOnDrop(Server);

/// Wraps `server` so it is shut down at the end of the scope.
pub fn defer_shutdown(server: Server) -> ShutdownOnDrop {
    ShutdownOnDrop(server)
}

impl std::ops::Deref for ShutdownOnDrop {
    type Target = Server;
    fn deref(&self) -> &Server {
        &self.0
    }
}

thread_local! {
    static SILENT_PANICS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A panic hook that stays quiet on threads that set `SILENT_PANICS`, and
/// otherwise defers to the previous hook.
fn install_silencing_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !SILENT_PANICS.with(|s| s.get()) {
                previous(info);
            }
        }));
    });
}

impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // The test already failed. Shut down anyway (as a Go defer does), but
            // keep a panicking shutdown (e.g. a pending marker) out of the test's
            // output, so the runner still classifies the case by its first failure.
            install_silencing_hook();
            SILENT_PANICS.with(|s| s.set(true));
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.0.shutdown()));
            SILENT_PANICS.with(|s| s.set(false));
        } else {
            let _ = self.0.shutdown();
        }
    }
}

/// `rr.Header().Get(name)`: first value, case-insensitive name.
pub fn header<'a>(resp: &'a HttpResponse, name: &str) -> Option<&'a str> {
    resp.headers.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
}

/// Builds a request (`http.NewRequest(method, path, body)` plus headers).
pub fn request(method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> HttpRequest {
    HttpRequest {
        method: method.into(),
        path: path.into(),
        headers: headers.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect(),
        body: body.to_vec(),
    }
}
