//! A scriptable fake AnyCable RPC application (SPEC.md §5, §9).
//!
//! One [`FakeRpc`] serves the `anycable.RPC` gRPC service and AnyCable's HTTP
//! RPC protocol at the same time, on separate loopback ports. It
//!
//! * **records** every call in arrival order: transport, method, decoded request,
//!   metadata (gRPC metadata or HTTP headers, lower-cased), HTTP path and raw body,
//!   connection index, and whether the client abandoned it;
//! * **answers from a script** of expectations, matched like testify mocks: the
//!   first registered expectation whose method and matcher fit and that has uses
//!   left wins; unmatched calls are recorded as unexpected and fail (gRPC
//!   `UNIMPLEMENTED`, HTTP 404);
//! * **holds calls at [`Gate`]s**, so a test can keep calls in flight, wait until
//!   a given number are held, and release them one by one (no sleeps).
//!
//! HTTP RPC wire format (from upstream `rpc/http.go`, verified against the Go
//! client in `tests/unit/rpc`): `POST <base>/connect|command|disconnect` with a
//! JSON body (Go `encoding/json` of the protobuf struct: snake_case keys, zero
//! values omitted, enums as integers), `Authorization: Bearer <secret>` if
//! configured, and call metadata as `X-Anycable-Meta-<key>` headers. The fake
//! answers 200 with the response as JSON. Non-POST requests get 405 and other
//! paths 404 (both recorded in [`FakeRpc::stray_http`]).
//!
//! ```no_run
//! use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply};
//! use cabby_core::protos;
//! let fake = FakeRpc::start();
//! let gate = fake.gate();
//! fake.on(Method::Connect).held_by(&gate).reply(protos::ConnectionResponse {
//!     status: protos::Status::Success as i32,
//!     ..Default::default()
//! });
//! // ... point a client at fake.grpc_target() or fake.http_url() ...
//! gate.await_held(1, cabby_testkit::runtime::DEADLINE);
//! gate.release_one();
//! ```

use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cabby_core::protos;
use cabby_core::protos::rpc_server::{Rpc, RpcServer};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::Code;

use crate::fake_http::{self, Handler, RecordedRequest, Served};
use crate::runtime::runtime;
use crate::sync::{Gate, Watched};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Connect,
    Command,
    Disconnect,
}

impl Method {
    /// HTTP RPC path segment / lower-case name.
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Connect => "connect",
            Method::Command => "command",
            Method::Disconnect => "disconnect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Grpc,
    Http,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    Connect(protos::ConnectionRequest),
    Command(protos::CommandMessage),
    Disconnect(protos::DisconnectRequest),
}

impl Request {
    pub fn method(&self) -> Method {
        match self {
            Request::Connect(_) => Method::Connect,
            Request::Command(_) => Method::Command,
            Request::Disconnect(_) => Method::Disconnect,
        }
    }

    pub fn connect(&self) -> &protos::ConnectionRequest {
        match self {
            Request::Connect(r) => r,
            other => panic!("expected a Connect request, got {other:?}"),
        }
    }

    pub fn command(&self) -> &protos::CommandMessage {
        match self {
            Request::Command(r) => r,
            other => panic!("expected a Command request, got {other:?}"),
        }
    }

    pub fn disconnect(&self) -> &protos::DisconnectRequest {
        match self {
            Request::Disconnect(r) => r,
            other => panic!("expected a Disconnect request, got {other:?}"),
        }
    }
}

impl From<protos::ConnectionRequest> for Request {
    fn from(r: protos::ConnectionRequest) -> Self {
        Request::Connect(r)
    }
}
impl From<protos::CommandMessage> for Request {
    fn from(r: protos::CommandMessage) -> Self {
        Request::Command(r)
    }
}
impl From<protos::DisconnectRequest> for Request {
    fn from(r: protos::DisconnectRequest) -> Self {
        Request::Disconnect(r)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Connect(protos::ConnectionResponse),
    Command(protos::CommandResponse),
    Disconnect(protos::DisconnectResponse),
}

/// HTTP details of a call received over HTTP RPC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpDetails {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
}

/// Lifecycle of a recorded call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallState {
    /// Being handled (possibly held at a gate).
    InFlight,
    /// A reply was produced.
    Replied,
    /// The client went away (cancellation, deadline, closed connection) first.
    Abandoned,
}

/// One recorded RPC call.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    /// Arrival order across both transports, from 0.
    pub seq: usize,
    pub transport: Transport,
    pub request: Request,
    /// gRPC metadata or HTTP headers; keys lower-cased, arrival order.
    pub metadata: Vec<(String, String)>,
    pub http: Option<HttpDetails>,
    /// Index of the TCP connection (per transport, accept order).
    pub connection: u64,
    /// No expectation matched.
    pub unexpected: bool,
    pub state: CallState,
}

impl Call {
    pub fn method(&self) -> Method {
        self.request.method()
    }

    /// First value for `key` (case-insensitive).
    pub fn meta(&self, key: &str) -> Option<&str> {
        let key = key.to_ascii_lowercase();
        self.metadata.iter().find(|(k, _)| *k == key).map(|(_, v)| v.as_str())
    }

    /// All values for `key`, like Go's `metadata.MD.Get`.
    pub fn meta_all(&self, key: &str) -> Vec<&str> {
        let key = key.to_ascii_lowercase();
        self.metadata.iter().filter(|(k, _)| *k == key).map(|(_, v)| v.as_str()).collect()
    }
}

type ReplyFn = Arc<dyn Fn(&Call) -> Reply + Send + Sync>;

/// What the fake answers.
#[derive(Clone)]
#[allow(clippy::large_enum_variant)] // scripted once per expectation; size is irrelevant
pub enum Reply {
    Ok(Response),
    /// A gRPC error status. Over HTTP: 401 for UNAUTHENTICATED, 422 for
    /// INVALID_ARGUMENT, 500 otherwise, with the message as body.
    Status(Code, String),
    /// A raw HTTP response (HTTP transport only; gRPC gets INTERNAL).
    Http {
        status: u16,
        body: Vec<u8>,
    },
    /// Computed from the call.
    With(ReplyFn),
}

impl fmt::Debug for Reply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reply::Ok(r) => f.debug_tuple("Ok").field(r).finish(),
            Reply::Status(c, m) => f.debug_tuple("Status").field(c).field(m).finish(),
            Reply::Http { status, body } => {
                f.debug_struct("Http").field("status", status).field("body", &String::from_utf8_lossy(body)).finish()
            }
            Reply::With(_) => f.write_str("With(..)"),
        }
    }
}

impl Reply {
    pub fn status(code: Code, message: impl Into<String>) -> Self {
        Reply::Status(code, message.into())
    }

    pub fn http(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Reply::Http { status, body: body.into() }
    }

    pub fn with(f: impl Fn(&Call) -> Reply + Send + Sync + 'static) -> Self {
        Reply::With(Arc::new(f))
    }

    fn resolve(self, call: &Call) -> Reply {
        match self {
            Reply::With(f) => f(call).resolve(call),
            other => other,
        }
    }
}

impl From<protos::ConnectionResponse> for Reply {
    fn from(r: protos::ConnectionResponse) -> Self {
        Reply::Ok(Response::Connect(r))
    }
}
impl From<protos::CommandResponse> for Reply {
    fn from(r: protos::CommandResponse) -> Self {
        Reply::Ok(Response::Command(r))
    }
}
impl From<protos::DisconnectResponse> for Reply {
    fn from(r: protos::DisconnectResponse) -> Self {
        Reply::Ok(Response::Disconnect(r))
    }
}

type Matcher = Arc<dyn Fn(&Call) -> bool + Send + Sync>;

struct Expectation {
    method: Method,
    matcher: Option<Matcher>,
    /// `None`: unlimited.
    remaining: Option<usize>,
    gate: Option<Gate>,
    reply: Reply,
}

/// Builder returned by [`FakeRpc::on`]; registers the expectation on [`ExpectationBuilder::reply`].
#[must_use = "an expectation is registered only by calling .reply(..)"]
pub struct ExpectationBuilder<'a> {
    fake: &'a FakeRpc,
    method: Method,
    matcher: Option<Matcher>,
    remaining: Option<usize>,
    gate: Option<Gate>,
}

impl ExpectationBuilder<'_> {
    /// Matches only calls whose decoded request equals `request` exactly.
    pub fn with_request(self, request: impl Into<Request>) -> Self {
        let request = request.into();
        self.matching(move |c| c.request == request)
    }

    /// Matches only calls satisfying `f`.
    pub fn matching(mut self, f: impl Fn(&Call) -> bool + Send + Sync + 'static) -> Self {
        self.matcher = Some(Arc::new(f));
        self
    }

    /// Matches at most once (testify `.Once()`).
    pub fn once(self) -> Self {
        self.times(1)
    }

    pub fn times(mut self, n: usize) -> Self {
        self.remaining = Some(n);
        self
    }

    /// Holds matching calls at `gate` before replying.
    pub fn held_by(mut self, gate: &Gate) -> Self {
        self.gate = Some(gate.clone());
        self
    }

    pub fn reply(self, reply: impl Into<Reply>) {
        let e = Expectation {
            method: self.method,
            matcher: self.matcher,
            remaining: self.remaining,
            gate: self.gate,
            reply: reply.into(),
        };
        self.fake.inner.script.lock().unwrap_or_else(|e| e.into_inner()).push(e);
    }
}

/// Options for [`FakeRpc::start_with`].
#[derive(Debug, Clone, Default)]
pub struct FakeRpcOptions {
    /// gRPC `MaxConcurrentStreams` per connection (upstream test servers use 1).
    pub grpc_max_concurrent_streams: Option<u32>,
    /// Serve gRPC over TLS with this PEM certificate chain and private key.
    pub grpc_tls: Option<(Vec<u8>, Vec<u8>)>,
    /// HTTP RPC mount path (e.g. `/_anycable`): when set, only
    /// `POST <mount>/connect|command|disconnect` are RPC calls and every other
    /// path gets 404 (otherwise any prefix is accepted).
    pub http_mount_path: Option<String>,
}

struct Inner {
    log: Watched<Vec<Call>>,
    script: Mutex<Vec<Expectation>>,
    stray_http: Watched<Vec<RecordedRequest>>,
    shutdown: CancellationToken,
    /// Remote addresses of accepted gRPC connections, in accept order.
    grpc_peers: Watched<Vec<SocketAddr>>,
    http_mount_path: Option<String>,
}

/// The fake RPC application. Dropping it shuts both servers down and unblocks
/// every held call.
pub struct FakeRpc {
    inner: Arc<Inner>,
    grpc_addr: SocketAddr,
    http: Served,
}

impl Drop for FakeRpc {
    fn drop(&mut self) {
        self.inner.shutdown.cancel();
    }
}

struct AbandonGuard<'a> {
    inner: &'a Inner,
    seq: usize,
    done: bool,
}

impl Drop for AbandonGuard<'_> {
    fn drop(&mut self) {
        let state = if self.done { CallState::Replied } else { CallState::Abandoned };
        let seq = self.seq;
        self.inner.log.update(|log| log[seq].state = state);
    }
}

impl Inner {
    async fn handle(
        &self,
        transport: Transport,
        request: Request,
        metadata: Vec<(String, String)>,
        http: Option<HttpDetails>,
        connection: u64,
    ) -> Reply {
        let mut call = Call {
            seq: 0,
            transport,
            request,
            metadata,
            http,
            connection,
            unexpected: false,
            state: CallState::InFlight,
        };
        let matched = {
            let mut script = self.script.lock().unwrap_or_else(|e| e.into_inner());
            script
                .iter_mut()
                .find(|e| {
                    e.method == call.method() && e.remaining != Some(0) && e.matcher.as_ref().is_none_or(|m| m(&call))
                })
                .map(|e| {
                    if let Some(n) = &mut e.remaining {
                        *n -= 1;
                    }
                    (e.gate.clone(), e.reply.clone())
                })
        };
        call.unexpected = matched.is_none();
        let seq = self.log.update(|log| {
            call.seq = log.len();
            log.push(call.clone());
            call.seq
        });
        call.seq = seq;
        let mut guard = AbandonGuard { inner: self, seq, done: false };
        let reply = match matched {
            None => Reply::status(
                Code::Unimplemented,
                format!("fake RPC: unexpected {} call: {:?}", call.method().as_str(), call.request),
            ),
            Some((gate, reply)) => {
                if let Some(gate) = gate
                    && !gate.pass(&self.shutdown).await
                {
                    guard.done = true;
                    return Reply::status(Code::Unavailable, "fake RPC: shutting down");
                }
                reply.resolve(&call)
            }
        };
        guard.done = true;
        reply
    }
}

fn grpc_metadata(md: &tonic::metadata::MetadataMap) -> Vec<(String, String)> {
    md.iter()
        .filter_map(|kv| match kv {
            tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                Some((k.as_str().to_owned(), v.to_str().unwrap_or_default().to_owned()))
            }
            tonic::metadata::KeyAndValueRef::Binary(..) => None,
        })
        .collect()
}

fn grpc_reply<T>(reply: Reply, pick: impl FnOnce(Response) -> Option<T>) -> Result<tonic::Response<T>, tonic::Status> {
    match reply {
        Reply::Ok(r) => {
            let shown = format!("{r:?}");
            pick(r)
                .map(tonic::Response::new)
                .ok_or_else(|| tonic::Status::internal(format!("fake RPC: scripted reply of the wrong type: {shown}")))
        }
        Reply::Status(code, msg) => Err(tonic::Status::new(code, msg)),
        Reply::Http { status, .. } => {
            Err(tonic::Status::internal(format!("fake RPC: HTTP reply {status} scripted for a gRPC call")))
        }
        Reply::With(_) => unreachable!("resolved"),
    }
}

/// Connection index of a gRPC call, from the accepted peers in accept order.
fn grpc_connection<T>(req: &tonic::Request<T>, peers: &Watched<Vec<SocketAddr>>) -> u64 {
    let Some(remote) = req.remote_addr() else { return u64::MAX };
    peers.read(|p| p.iter().position(|a| *a == remote).map_or(u64::MAX, |i| i as u64))
}

#[derive(Clone)]
struct GrpcEndpoint {
    inner: Arc<Inner>,
}

#[tonic::async_trait]
impl Rpc for GrpcEndpoint {
    async fn connect(
        &self,
        request: tonic::Request<protos::ConnectionRequest>,
    ) -> Result<tonic::Response<protos::ConnectionResponse>, tonic::Status> {
        let conn = grpc_connection(&request, &self.inner.grpc_peers);
        let md = grpc_metadata(request.metadata());
        let reply = self.inner.handle(Transport::Grpc, request.into_inner().into(), md, None, conn).await;
        grpc_reply(reply, |r| match r {
            Response::Connect(r) => Some(r),
            _ => None,
        })
    }

    async fn command(
        &self,
        request: tonic::Request<protos::CommandMessage>,
    ) -> Result<tonic::Response<protos::CommandResponse>, tonic::Status> {
        let conn = grpc_connection(&request, &self.inner.grpc_peers);
        let md = grpc_metadata(request.metadata());
        let reply = self.inner.handle(Transport::Grpc, request.into_inner().into(), md, None, conn).await;
        grpc_reply(reply, |r| match r {
            Response::Command(r) => Some(r),
            _ => None,
        })
    }

    async fn disconnect(
        &self,
        request: tonic::Request<protos::DisconnectRequest>,
    ) -> Result<tonic::Response<protos::DisconnectResponse>, tonic::Status> {
        let conn = grpc_connection(&request, &self.inner.grpc_peers);
        let md = grpc_metadata(request.metadata());
        let reply = self.inner.handle(Transport::Grpc, request.into_inner().into(), md, None, conn).await;
        grpc_reply(reply, |r| match r {
            Response::Disconnect(r) => Some(r),
            _ => None,
        })
    }
}

fn json_response<T: serde::Serialize>(value: &T) -> fake_http::HttpResponse {
    let mut r = fake_http::response(200, serde_json::to_vec(value).expect("serializable response"));
    r.headers_mut().insert(hyper::header::CONTENT_TYPE, hyper::header::HeaderValue::from_static("application/json"));
    r
}

fn http_reply(reply: Reply) -> fake_http::HttpResponse {
    match reply {
        Reply::Ok(Response::Connect(r)) => json_response(&r),
        Reply::Ok(Response::Command(r)) => json_response(&r),
        Reply::Ok(Response::Disconnect(r)) => json_response(&r),
        Reply::Status(code, msg) => {
            let status = match code {
                Code::Unauthenticated => 401,
                Code::InvalidArgument => 422,
                Code::Unimplemented => 404,
                _ => 500,
            };
            fake_http::response(status, msg)
        }
        Reply::Http { status, body } => fake_http::response(status, body),
        Reply::With(_) => unreachable!("resolved"),
    }
}

fn decode<T: serde::de::DeserializeOwned + Default>(body: &[u8]) -> Result<T, String> {
    serde_json::from_slice(body).map_err(|e| e.to_string())
}

async fn handle_http(inner: Arc<Inner>, req: RecordedRequest) -> fake_http::HttpResponse {
    let (prefix, last) = req.path.rsplit_once('/').unwrap_or(("", req.path.as_str()));
    let method = match last {
        "connect" => Some(Method::Connect),
        "command" => Some(Method::Command),
        "disconnect" => Some(Method::Disconnect),
        _ => None,
    }
    .filter(|_| inner.http_mount_path.as_deref().is_none_or(|m| m.trim_end_matches('/') == prefix));
    let Some(method) = method.filter(|_| req.method == "POST") else {
        let status = if req.method == "POST" { 404 } else { 405 };
        inner.stray_http.update(|v| v.push(req));
        return fake_http::response(status, Bytes::new());
    };
    let decoded: Result<Request, String> = match method {
        Method::Connect => decode::<protos::ConnectionRequest>(&req.body).map(Into::into),
        Method::Command => decode::<protos::CommandMessage>(&req.body).map(Into::into),
        Method::Disconnect => decode::<protos::DisconnectRequest>(&req.body).map(Into::into),
    };
    let request = match decoded {
        Ok(r) => r,
        Err(e) => {
            inner.stray_http.update(|v| v.push(req));
            return fake_http::response(400, format!("fake RPC: undecodable {} body: {e}", method.as_str()));
        }
    };
    let details = HttpDetails { method: req.method, path: req.path, body: req.body };
    let reply = inner.handle(Transport::Http, request, req.headers, Some(details), req.connection).await;
    http_reply(reply)
}

impl FakeRpc {
    pub fn start() -> Self {
        Self::start_with(FakeRpcOptions::default())
    }

    /// Binds both listeners before returning, so the servers are ready.
    pub fn start_with(options: FakeRpcOptions) -> Self {
        let shutdown = CancellationToken::new();
        let inner = Arc::new(Inner {
            log: Watched::new(Vec::new()),
            script: Mutex::new(Vec::new()),
            stray_http: Watched::new(Vec::new()),
            shutdown: shutdown.clone(),
            grpc_peers: Watched::new(Vec::new()),
            http_mount_path: options.http_mount_path.clone(),
        });

        // gRPC
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binding a loopback port");
        std_listener.set_nonblocking(true).expect("non-blocking listener");
        let grpc_addr = std_listener.local_addr().expect("local addr");
        let rt = runtime();
        let listener = {
            let _g = rt.enter();
            tokio::net::TcpListener::from_std(std_listener).expect("tokio listener")
        };
        let endpoint = GrpcEndpoint { inner: Arc::clone(&inner) };
        let accepted = Arc::clone(&inner);
        let incoming = TcpListenerStream::new(listener).map(move |s| {
            if let Ok(stream) = &s {
                let peer = stream.peer_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
                accepted.grpc_peers.update(|p| p.push(peer));
            }
            s
        });
        let mut builder = tonic::transport::Server::builder();
        if let Some(n) = options.grpc_max_concurrent_streams {
            builder = builder.max_concurrent_streams(n);
        }
        if let Some((cert, key)) = &options.grpc_tls {
            let tls =
                tonic::transport::ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(cert, key));
            builder = builder.tls_config(tls).expect("valid gRPC TLS configuration");
        }
        let router = builder.add_service(RpcServer::new(endpoint));
        let token = shutdown.clone();
        rt.spawn(async move {
            let _ = router.serve_with_incoming_shutdown(incoming, token.cancelled_owned()).await;
        });

        // HTTP
        let http_inner = Arc::clone(&inner);
        let handler: Handler = Arc::new(move |req| Box::pin(handle_http(Arc::clone(&http_inner), req)));
        let http = fake_http::serve(handler, shutdown);

        Self { inner, grpc_addr, http }
    }

    /// `127.0.0.1:<port>`: an upstream-style gRPC `host` setting.
    pub fn grpc_target(&self) -> String {
        self.grpc_addr.to_string()
    }

    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }

    /// `http://127.0.0.1:<port>`: base URL for HTTP RPC (any path prefix works).
    pub fn http_url(&self) -> String {
        format!("http://{}", self.http.addr)
    }

    pub fn http_addr(&self) -> SocketAddr {
        self.http.addr
    }

    /// Starts scripting replies for `method`.
    pub fn on(&self, method: Method) -> ExpectationBuilder<'_> {
        ExpectationBuilder { fake: self, method, matcher: None, remaining: None, gate: None }
    }

    /// A new closed gate. Gates are independent of the fake, but calls held at a
    /// gate are released with an error when the fake is dropped.
    pub fn gate(&self) -> Gate {
        Gate::new()
    }

    /// Every call so far, in arrival order.
    pub fn calls(&self) -> Vec<Call> {
        self.inner.log.read(Clone::clone)
    }

    pub fn calls_of(&self, method: Method) -> Vec<Call> {
        self.calls().into_iter().filter(|c| c.method() == method).collect()
    }

    pub fn call_count(&self, method: Method) -> usize {
        self.inner.log.read(|l| l.iter().filter(|c| c.method() == method).count())
    }

    pub fn unexpected_calls(&self) -> Vec<Call> {
        self.calls().into_iter().filter(|c| c.unexpected).collect()
    }

    /// HTTP requests that were not RPC calls (wrong method/path, undecodable body).
    pub fn stray_http(&self) -> Vec<RecordedRequest> {
        self.inner.stray_http.read(Clone::clone)
    }

    /// Blocks until at least `n` calls of `method` arrived; panics after `deadline`.
    pub fn await_calls(&self, method: Method, n: usize, deadline: Duration) -> Vec<Call> {
        let ok = self.inner.log.wait_until(deadline, |l| l.iter().filter(|c| c.method() == method).count() >= n);
        assert!(ok, "expected {n} {} call(s) within {deadline:?}, got {:?}", method.as_str(), self.calls());
        self.calls_of(method)
    }

    /// Blocks until call `seq` is no longer in flight; returns its final state.
    pub fn await_call_settled(&self, seq: usize, deadline: Duration) -> CallState {
        let ok = self.inner.log.wait_until(deadline, |l| l.get(seq).is_some_and(|c| c.state != CallState::InFlight));
        assert!(ok, "call {seq} still in flight after {deadline:?}");
        self.inner.log.read(|l| l[seq].state)
    }

    /// Blocks until at least `n` calls are in `state`; panics after `deadline`.
    pub fn await_in_state(&self, state: CallState, n: usize, deadline: Duration) {
        let ok = self.inner.log.wait_until(deadline, |l| l.iter().filter(|c| c.state == state).count() >= n);
        assert!(ok, "expected {n} call(s) in state {state:?} within {deadline:?}, got {:?}", self.calls());
    }

    /// Accepted gRPC TCP connections.
    pub fn grpc_connections(&self) -> u64 {
        self.inner.grpc_peers.read(Vec::len) as u64
    }

    /// Accepted HTTP TCP connections.
    pub fn http_connections(&self) -> u64 {
        self.http.connections.load(Ordering::SeqCst)
    }

    /// Blocks until at least `n` gRPC connections were accepted; `false` on deadline.
    pub fn await_grpc_connections(&self, n: u64, deadline: Duration) -> bool {
        self.inner.grpc_peers.wait_until(deadline, |p| p.len() as u64 >= n)
    }
}
