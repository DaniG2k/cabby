//! HTTP RPC transport (upstream `rpc/http.go`).
//!
//! Wire format (pinned by `tests/unit/rpc/http_test.rs` against the Go client):
//! `POST <host>/<connect|command|disconnect>`, `Content-Type: application/json`,
//! body = Go `encoding/json` of the protobuf struct (snake_case keys, empty values
//! omitted, enums as integers), `Authorization: Bearer <secret>` when configured,
//! and every metadata pair as an `X-Anycable-Meta-<key>` header. Response
//! statuses map to gRPC codes: 401 Unauthenticated, 400/422 InvalidArgument,
//! other non-200 Unknown; transport errors are DeadlineExceeded when the context
//! is done, Unauthenticated for TLS failures, Unavailable otherwise. A response
//! body over `rpc.max_recv_size` (16 MiB by default) is Unknown, not retried,
//! and counts as a success for the circuit breaker (D-068).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use cabby_core::logger::{LogValuer, compact_value_bytes};
use cabby_core::{gojson, protos};
use http::header::{AUTHORIZATION, CONTENT_TYPE, HeaderName, HeaderValue};
use serde::de::DeserializeOwned;
use tokio::time::Instant;

use super::{
    BoxFuture, ClientError, ClientHelper, Code, Config, Context, Dialer, RpcError, RpcService,
    new_inprocess_service_dialer,
};
use crate::http_client::{HttpClient, Origin};

/// Idle keep-alive connections are dropped before typical server keep-alive
/// timeouts (e.g., Puma's `persistent_timeout`) close them.
const IDLE_CONN_TIMEOUT: Duration = Duration::from_secs(10);

// --- Circuit breaker (sony/gobreaker `TwoStepCircuitBreaker` with upstream settings) ---

const CB_MAX_REQUESTS: u32 = 5;
const CB_INTERVAL: Duration = Duration::from_secs(10);
const CB_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CbState {
    Closed,
    HalfOpen,
    Open,
}

#[derive(Debug, Default, Clone, Copy)]
struct Counts {
    requests: u32,
    total_successes: u32,
    total_failures: u32,
    consecutive_successes: u32,
    consecutive_failures: u32,
}

#[derive(Debug)]
struct Breaker {
    state: CbState,
    generation: u64,
    counts: Counts,
    expiry: Option<Instant>,
}

impl Breaker {
    fn new() -> Self {
        let mut b = Breaker { state: CbState::Closed, generation: 0, counts: Counts::default(), expiry: None };
        b.new_generation(Instant::now());
        b
    }

    fn ready_to_trip(c: &Counts) -> bool {
        c.requests >= 10 && f64::from(c.total_failures) / f64::from(c.requests) >= 0.8
    }

    fn new_generation(&mut self, now: Instant) {
        self.generation += 1;
        self.counts = Counts::default();
        self.expiry = match self.state {
            CbState::Closed => Some(now + CB_INTERVAL),
            CbState::Open => Some(now + CB_TIMEOUT),
            CbState::HalfOpen => None,
        };
    }

    fn set_state(&mut self, state: CbState, now: Instant) {
        if self.state == state {
            return;
        }
        self.state = state;
        self.new_generation(now);
        match state {
            CbState::Open => cb_logger().log(cabby_core::logger::Level::Warn, "circuit breaker is open", &[]),
            CbState::Closed => cb_logger().log(cabby_core::logger::Level::Info, "circuit breaker is closed", &[]),
            CbState::HalfOpen => {}
        }
    }

    fn current(&mut self, now: Instant) -> CbState {
        match self.state {
            CbState::Closed if self.expiry.is_some_and(|e| e <= now) => self.new_generation(now),
            CbState::Open if self.expiry.is_some_and(|e| e <= now) => self.set_state(CbState::HalfOpen, now),
            _ => {}
        }
        self.state
    }

    /// gobreaker `beforeRequest`: the generation to report to, or the refusal.
    fn allow(&mut self) -> Result<u64, &'static str> {
        let now = Instant::now();
        match self.current(now) {
            CbState::Open => return Err("circuit breaker is open"),
            CbState::HalfOpen if self.counts.requests >= CB_MAX_REQUESTS => return Err("too many requests"),
            _ => {}
        }
        self.counts.requests += 1;
        Ok(self.generation)
    }

    /// gobreaker `afterRequest`.
    fn done(&mut self, generation: u64, success: bool) {
        let now = Instant::now();
        let state = self.current(now);
        if generation != self.generation {
            return;
        }
        if success {
            let c = &mut self.counts;
            c.total_successes += 1;
            c.consecutive_successes += 1;
            c.consecutive_failures = 0;
            if state == CbState::HalfOpen && c.consecutive_successes >= CB_MAX_REQUESTS {
                self.set_state(CbState::Closed, now);
            }
        } else {
            match state {
                CbState::Closed => {
                    let c = &mut self.counts;
                    c.total_failures += 1;
                    c.consecutive_failures += 1;
                    c.consecutive_successes = 0;
                    if Self::ready_to_trip(c) {
                        self.set_state(CbState::Open, now);
                    }
                }
                CbState::HalfOpen => self.set_state(CbState::Open, now),
                CbState::Open => {}
            }
        }
    }
}

/// Upstream `slog.With("context", "cb", "name", name)`: the default logger,
/// so without the runner's `nodeid`.
fn cb_logger() -> cabby_core::logger::Logger {
    cabby_core::logger::Logger::root().with("context", "cb").with("name", "httrpc")
}

/// Reports a request's outcome to the breaker on drop (failure unless marked
/// successful, as upstream's deferred callback; also when the call is dropped).
struct CbGuard<'a> {
    breaker: &'a Mutex<Breaker>,
    generation: u64,
    success: bool,
}

impl Drop for CbGuard<'_> {
    fn drop(&mut self) {
        lock(self.breaker).done(self.generation, self.success);
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// --- Service --------------------------------------------------------------------------

/// Calls the application over HTTP (upstream `HTTPService`). Keeps up to
/// `concurrency` idle keep-alive connections and guards calls with a circuit
/// breaker (opens after >= 10 requests in a 10s window with >= 80% failures).
pub struct HttpService {
    client: HttpClient,
    secret: Option<HeaderValue>,
    breaker: Mutex<Breaker>,
}

impl std::fmt::Debug for HttpService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpService").field("url", &self.client.origin().raw()).finish_non_exhaustive()
    }
}

/// Upstream `NewHTTPDialer`: the HTTP service behind an in-process dialer.
pub(crate) fn new_http_dialer(config: &Config) -> Result<Dialer, RpcError> {
    let service = Arc::new(HttpService::new(config)?);
    let helper = Arc::new(HttpClientHelper::new(Arc::clone(&service)));
    Ok(new_inprocess_service_dialer(service, helper))
}

impl HttpService {
    pub fn new(config: &Config) -> Result<Self, RpcError> {
        let origin = Origin::parse(&config.host).map_err(RpcError::Other)?;
        let tls = match config.tls_config(&[b"http/1.1"])? {
            Some(tls) => Some(tls),
            // An https URL without explicit TLS settings uses the defaults.
            None if origin.https => Some(
                crate::client_tls::client_config(&crate::client_tls::TlsOptions {
                    root_ca: String::new(),
                    verify: true,
                    alpn: vec![b"http/1.1".to_vec()],
                })
                .map_err(RpcError::Other)?,
            ),
            None => None,
        };
        let tls = if origin.https { tls } else { None };
        // Go's zero `MaxIdleConnsPerHost` means its default of 2.
        let max_idle = usize::try_from(config.concurrency).ok().filter(|c| *c > 0).unwrap_or(2);
        let timeout = u64::try_from(config.request_timeout).ok().filter(|t| *t > 0).map(Duration::from_millis);
        let secret = if config.secret.is_empty() {
            None
        } else {
            Some(
                HeaderValue::from_str(&format!("Bearer {}", config.secret))
                    .map_err(|e| RpcError::Other(format!("invalid HTTP RPC secret: {e}")))?,
            )
        };
        let max_recv = usize::try_from(config.max_recv_size).ok().filter(|n| *n > 0);
        let mut client = HttpClient::new(origin, tls, max_idle, IDLE_CONN_TIMEOUT, timeout);
        if let Some(limit) = max_recv {
            client = client.with_max_response_bytes(limit);
        }
        Ok(Self { client, secret, breaker: Mutex::new(Breaker::new()) })
    }

    fn breaker_open(&self) -> bool {
        lock(&self.breaker).current(Instant::now()) == CbState::Open
    }

    async fn perform<T: DeserializeOwned>(
        &self,
        ctx: &Context,
        path: &str,
        payload: Vec<u8>,
    ) -> Result<T, ClientError> {
        let generation = lock(&self.breaker).allow().map_err(|e| ClientError::Other(e.to_owned()))?;
        let mut cb = CbGuard { breaker: &self.breaker, generation, success: false };

        let mut headers: Vec<(HeaderName, HeaderValue)> = Vec::with_capacity(ctx.metadata.len() + 2);
        headers.push((CONTENT_TYPE, HeaderValue::from_static("application/json")));
        if let Some(secret) = &self.secret {
            headers.push((AUTHORIZATION, secret.clone()));
        }
        for (i, (k, v)) in ctx.metadata.iter().enumerate() {
            // The first value of each key, as upstream (`v[0]`).
            if ctx.metadata[..i].iter().any(|(prev, _)| prev == k) {
                continue;
            }
            if let (Ok(name), Ok(value)) =
                (HeaderName::try_from(format!("x-anycable-meta-{k}")), HeaderValue::from_str(v))
            {
                headers.push((name, value));
            }
        }

        let exchange = self.client.post(path, &headers, Bytes::from(payload));
        // The request is abandoned (its connection dropped) when the context ends.
        let result = tokio::select! {
            biased;
            r = exchange => r.map_err(Some),
            _ = ctx.done() => Err(None),
        };

        let res = match result {
            Ok(res) => res,
            Err(None) => return Err(ClientError::status(Code::DeadlineExceeded, "request timeout")),
            Err(Some(_)) if ctx.err().is_some() => {
                return Err(ClientError::status(Code::DeadlineExceeded, "request timeout"));
            }
            // Handle TLS errors.
            Err(Some(err)) if err.tls || err.message.contains("tls:") => {
                return Err(ClientError::status(Code::Unauthenticated, err.message));
            }
            // The application answered, with too much: final, and a success
            // for the breaker.
            Err(Some(err)) if err.too_large => {
                cb.success = true;
                return Err(ClientError::status(Code::Unknown, err.message));
            }
            Err(Some(err)) => return Err(ClientError::status(Code::Unavailable, err.message)),
        };
        cb.success = true;
        drop(cb);

        let reason = || compact_value_bytes(&res.body).log_value().to_string();
        match res.status {
            200 => {}
            401 => return Err(ClientError::status(Code::Unauthenticated, "http returned 401")),
            400 | 422 => return Err(ClientError::status(Code::InvalidArgument, reason())),
            _ => return Err(ClientError::status(Code::Unknown, reason())),
        }

        decode(&res.body)
    }
}

/// Decodes a JSON response the way Go's `json.Unmarshal` into the generated
/// struct treats it: unknown fields are ignored and `null` leaves a field unset.
fn decode<T: DeserializeOwned>(body: &[u8]) -> Result<T, ClientError> {
    fn strip_nulls(v: &mut serde_json::Value) {
        match v {
            serde_json::Value::Object(map) => {
                map.retain(|_, v| !v.is_null());
                map.values_mut().for_each(strip_nulls);
            }
            serde_json::Value::Array(items) => items.iter_mut().for_each(strip_nulls),
            _ => {}
        }
    }
    match serde_json::from_slice::<T>(body) {
        Ok(v) => Ok(v),
        Err(first) => {
            let mut value: serde_json::Value =
                serde_json::from_slice(body).map_err(|_| ClientError::Other(first.to_string()))?;
            strip_nulls(&mut value);
            serde_json::from_value(value).map_err(|e| ClientError::Other(e.to_string()))
        }
    }
}

impl RpcService for HttpService {
    fn connect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::ConnectionRequest,
    ) -> BoxFuture<'a, Result<protos::ConnectionResponse, ClientError>> {
        Box::pin(async move { self.perform(ctx, "connect", gojson::to_vec(&request)).await })
    }
    fn command<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::CommandMessage,
    ) -> BoxFuture<'a, Result<protos::CommandResponse, ClientError>> {
        Box::pin(async move { self.perform(ctx, "command", gojson::to_vec(&request)).await })
    }
    fn disconnect<'a>(
        &'a self,
        ctx: &'a Context,
        request: protos::DisconnectRequest,
    ) -> BoxFuture<'a, Result<protos::DisconnectResponse, ClientError>> {
        Box::pin(async move { self.perform(ctx, "disconnect", gojson::to_vec(&request)).await })
    }
}

/// Readiness of an [`HttpService`] (upstream `httpClientHelper`): not ready while
/// the circuit breaker is open.
#[derive(Debug)]
pub struct HttpClientHelper {
    service: Arc<HttpService>,
}

impl HttpClientHelper {
    pub fn new(service: Arc<HttpService>) -> Self {
        Self { service }
    }
}

impl ClientHelper for HttpClientHelper {
    fn ready(&self) -> Result<(), RpcError> {
        if self.service.breaker_open() {
            return Err(RpcError::Other("http rpc is temporarily unavailable".into()));
        }
        Ok(())
    }
    fn supports_active_conns(&self) -> bool {
        false
    }
    fn active_conns(&self) -> usize {
        0
    }
    fn close(&self) {
        self.service.client.close_idle();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
    }

    /// Request bodies are byte-identical to Go's `json.Marshal` of the generated
    /// structs: declaration order, `omitempty`, sorted map keys, HTML escaping,
    /// and `{}` for a present but empty message.
    #[test]
    fn request_bodies_match_go_encoding() {
        let msg = protos::CommandMessage {
            command: "subscribe".into(),
            identifier: "chat".into(),
            connection_identifiers: "user=1".into(),
            data: String::new(),
            env: Some(protos::Env {
                url: "ws://example.com/cable".into(),
                headers: map(&[("x-b", "<b>"), ("cookie", "a=b"), ("accept", "&")]),
                cstate: map(&[("_s_", "x")]),
                istate: Default::default(),
            }),
        };
        assert_eq!(
            r#"{"command":"subscribe","identifier":"chat","connection_identifiers":"user=1","env":{"url":"ws://example.com/cable","headers":{"accept":"{U}0026","cookie":"a=b","x-b":"{U}003cb{U}003e"},"cstate":{"_s_":"x"}}}"#
                .replace("{U}", &format!("{}u", '\\')),
            gojson::to_string(&msg)
        );
        assert_eq!("{}", gojson::to_string(&protos::ConnectionRequest::default()));
        assert_eq!(r#"{"env":{}}"#, gojson::to_string(&protos::ConnectionRequest { env: Some(Default::default()) }));
    }

    #[test]
    fn decodes_go_style_replies() {
        let r: protos::CommandResponse =
            decode(br#"{"status":1,"streams":null,"unknown":true,"env":{"cstate":{"a":"b"}}}"#).expect("decoded");
        assert_eq!(1, r.status);
        assert!(r.streams.is_empty());
        assert_eq!(Some("b"), r.env.as_ref().and_then(|e| e.cstate.get("a")).map(String::as_str));
    }

    #[test]
    fn breaker_opens_at_ten_requests_with_eighty_percent_failures() {
        let mut b = Breaker::new();
        for i in 0..9 {
            let g = b.allow().expect("closed");
            b.done(g, i < 1);
        }
        // 9 requests, 8 failures: not yet (fewer than 10 requests).
        assert_eq!(CbState::Closed, b.current(Instant::now()));
        let g = b.allow().expect("closed");
        b.done(g, false);
        assert_eq!(CbState::Open, b.current(Instant::now()));
        assert_eq!(Err("circuit breaker is open"), b.allow());
    }

    fn service(addr: std::net::SocketAddr, max_recv_size: i64) -> HttpService {
        let mut config = Config::new();
        config.host = format!("http://{addr}");
        config.max_recv_size = max_recv_size;
        HttpService::new(&config).expect("a service")
    }

    /// Pins D-068: `rpc.max_recv_size` bounds HTTP RPC responses: a body of
    /// exactly the limit is decoded, one byte more fails.
    #[tokio::test]
    async fn deviation_d068_max_recv_size_bounds_http_rpc_responses() {
        use crate::http_client::dial_error_tests::{serve_forever, with_length};

        let body = br#"{"status":1}    "#; // 16 bytes
        let addr = serve_forever(with_length(body)).await;
        let ok: protos::CommandResponse =
            service(addr, 16).perform(&Context::default(), "command", b"{}".to_vec()).await.expect("at the limit");
        assert_eq!(1, ok.status);

        let err = service(addr, 15)
            .perform::<protos::CommandResponse>(&Context::default(), "command", b"{}".to_vec())
            .await
            .expect_err("over the limit");
        assert!(err.to_string().contains("response body exceeds limit of 15 bytes"), "{err}");

        let default: protos::CommandResponse =
            service(addr, 0).perform(&Context::default(), "command", b"{}".to_vec()).await.expect("16 MiB by default");
        assert_eq!(1, default.status);
    }

    /// An oversized response is not retried (its code is neither Unavailable
    /// nor ResourceExhausted) and counts as a success for the circuit breaker:
    /// the application answered.
    #[tokio::test]
    async fn an_oversized_response_is_final_and_keeps_the_breaker_closed() {
        use crate::http_client::dial_error_tests::{serve_forever, with_length};

        let addr = serve_forever(with_length(&[b' '; 64])).await;
        let service = service(addr, 16);
        for _ in 0..20 {
            match service.perform::<protos::CommandResponse>(&Context::default(), "command", b"{}".to_vec()).await {
                Err(ClientError::Status(status)) => assert_eq!(Code::Unknown, status.code, "{}", status.message),
                other => panic!("expected a status error, got {other:?}"),
            }
        }
        assert!(!service.breaker_open(), "20 oversized responses leave the breaker closed");
    }
}
