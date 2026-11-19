//! Fake AnyCable HTTP RPC application (upstream `lib/rpc_server.go`).
//!
//! Runs in the harness process so the injected RPC latency is the same for every
//! server under test. It accepts every connection (identifier `{"id":<bench_sid>}`),
//! confirms every subscription (streaming from `bench:<identifier>`) and acknowledges
//! every disconnect, each after a jittered delay.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::gojson;
use crate::runtime::rt;

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

    fn index(self) -> usize {
        self as usize
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
    pub fn latency_for(&self, m: RpcMethod) -> Duration {
        let l = match m {
            RpcMethod::Connect => self.connect_latency,
            RpcMethod::Command => self.command_latency,
            RpcMethod::Disconnect => self.disconnect_latency,
        };
        if l.is_zero() { self.latency } else { l }
    }

    fn jittered(&self, m: RpcMethod) -> Duration {
        let d = self.latency_for(m);
        if self.jitter > 0.0 && !d.is_zero() {
            let factor = 1.0 + self.jitter * (2.0 * crate::rng::unit() - 1.0);
            return d.mul_f64(factor.max(0.0));
        }
        d
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

#[derive(Default)]
struct Counters {
    started: AtomicI64,
    completed: AtomicI64,
    after_close: AtomicI64,
}

/// Counts a call as completed when its handler finishes or is dropped (the
/// server closed, or the caller hung up), like upstream's deferred increment.
struct CompletedGuard<'a>(&'a Counters);

impl Drop for CompletedGuard<'_> {
    fn drop(&mut self) {
        self.0.completed.fetch_add(1, Ordering::SeqCst);
    }
}

struct Shared {
    cfg: RpcServerConfig,
    counters: [Counters; 3],
    next_id: AtomicI64,
    stop: CancellationToken,
}

pub struct RpcServer {
    shared: Arc<Shared>,
    url: String,
}

impl RpcServer {
    /// Starts on a loopback port.
    pub fn new(cfg: RpcServerConfig) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("binding the fake RPC server");
        listener.set_nonblocking(true).expect("non-blocking listener");
        let url = format!("http://{}", listener.local_addr().expect("RPC server address"));
        let shared = Arc::new(Shared {
            cfg,
            counters: Default::default(),
            next_id: AtomicI64::new(0),
            stop: CancellationToken::new(),
        });
        let s = Arc::clone(&shared);
        rt().spawn(async move {
            let Ok(listener) = tokio::net::TcpListener::from_std(listener) else { return };
            loop {
                let conn = tokio::select! {
                    () = s.stop.cancelled() => return,
                    c = listener.accept() => c,
                };
                let Ok((stream, _)) = conn else { continue };
                let _ = stream.set_nodelay(true);
                let s = Arc::clone(&s);
                rt().spawn(async move {
                    let svc = service_fn(|req| {
                        let s = Arc::clone(&s);
                        async move { Ok::<_, Infallible>(s.handle(req).await) }
                    });
                    let conn = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), svc);
                    // Closing the server drops the connection and its in-flight calls.
                    tokio::select! {
                        _ = conn => {}
                        () = s.stop.cancelled() => {}
                    }
                });
            }
        });
        Self { shared, url }
    }

    /// Base URL to pass as the server's RPC host.
    pub fn url(&self) -> String {
        self.url.clone()
    }

    /// Stops the server, interrupting in-flight calls.
    pub fn close(&self) {
        self.shared.stop.cancel();
    }

    pub fn stats(&self, m: RpcMethod) -> RpcMethodStats {
        let c = &self.shared.counters[m.index()];
        let completed = c.completed.load(Ordering::SeqCst);
        let started = c.started.load(Ordering::SeqCst);
        RpcMethodStats {
            started,
            completed,
            in_flight: started - completed,
            after_close: c.after_close.load(Ordering::SeqCst),
        }
    }
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        self.close();
    }
}

fn response(status: u16, body: String) -> http::Response<Full<Bytes>> {
    let mut resp = http::Response::new(Full::new(Bytes::from(body)));
    *resp.status_mut() = http::StatusCode::from_u16(status).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR);
    let content_type = if status == 200 { "application/json" } else { "text/plain; charset=utf-8" };
    resp.headers_mut().insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static(content_type));
    resp
}

impl Shared {
    async fn handle(&self, req: http::Request<Incoming>) -> http::Response<Full<Bytes>> {
        let m = match req.uri().path() {
            "/connect" => RpcMethod::Connect,
            "/command" => RpcMethod::Command,
            "/disconnect" => RpcMethod::Disconnect,
            _ => return response(404, "404 page not found\n".into()),
        };
        let c = &self.counters[m.index()];
        c.started.fetch_add(1, Ordering::SeqCst);
        let _done = CompletedGuard(c);

        let body = match req.into_body().collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => return response(422, format!("{e}\n")),
        };
        // Upstream decodes the first JSON value of the body.
        let mut values = serde_json::Deserializer::from_slice(&body).into_iter::<serde::de::IgnoredAny>();
        match values.next() {
            Some(Ok(_)) => {}
            Some(Err(e)) => return response(422, format!("{e}\n")),
            None => return response(422, "EOF\n".into()),
        }

        if m != RpcMethod::Disconnect
            && let Some(f) = &self.cfg.session_closed
            && f(&session_id(m, &body))
        {
            c.after_close.fetch_add(1, Ordering::SeqCst);
        }

        let d = self.cfg.jittered(m);
        if !d.is_zero() {
            tokio::select! {
                () = tokio::time::sleep(d) => {}
                () = self.stop.cancelled() => {}
            }
        }

        let out = match m {
            RpcMethod::Connect => Ok(self.connect(&body)),
            RpcMethod::Command => command(&body),
            RpcMethod::Disconnect => Ok(r#"{"status":1}"#.to_owned()),
        };
        match out {
            // Go's json.Encoder terminates the value with a newline.
            Ok(json) => response(200, json + "\n"),
            Err(e) => response(422, format!("{e}\n")),
        }
    }

    /// Uses the client's session id (the `bench_sid` URL parameter) as the
    /// connection identifier; without it, makes one up.
    fn connect(&self, body: &[u8]) -> String {
        let mut id = session_id(RpcMethod::Connect, body);
        if id.is_empty() {
            id = format!("rpc-{}", self.next_id.fetch_add(1, Ordering::SeqCst) + 1);
        }
        let identifiers = format!("{{\"id\":{}}}", gojson::string(&id));
        format!(
            r#"{{"status":1,"identifiers":{},"transmissions":[{}]}}"#,
            gojson::string(&identifiers),
            gojson::string(r#"{"type":"welcome"}"#)
        )
    }
}

#[derive(Deserialize, Default)]
struct CommandRequest {
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    identifier: Option<String>,
}

fn command(body: &[u8]) -> Result<String, String> {
    let req: CommandRequest = serde_json::Deserializer::from_slice(body)
        .into_iter::<CommandRequest>()
        .next()
        .unwrap_or_else(|| Ok(CommandRequest::default()))
        .map_err(|e| e.to_string())?;
    let identifier = req.identifier.unwrap_or_default();
    Ok(match req.command.as_deref().unwrap_or_default() {
        "subscribe" => {
            // Go marshals the map with sorted keys.
            let confirm = format!(r#"{{"identifier":{},"type":"confirm_subscription"}}"#, gojson::string(&identifier));
            format!(
                r#"{{"status":1,"streams":[{}],"transmissions":[{}]}}"#,
                gojson::string(&format!("bench:{identifier}")),
                gojson::string(&confirm)
            )
        }
        "unsubscribe" => r#"{"status":1,"stop_streams":true}"#.to_owned(),
        _ => r#"{"status":1}"#.to_owned(),
    })
}

/// The session id of an RPC request body: the `bench_sid` URL parameter for Connect,
/// the `id` connection identifier otherwise (empty when absent).
pub fn session_id(m: RpcMethod, body: &[u8]) -> String {
    if m == RpcMethod::Connect {
        #[derive(Deserialize)]
        struct Env {
            #[serde(default)]
            url: Option<String>,
        }
        #[derive(Deserialize)]
        struct Req {
            #[serde(default)]
            env: Option<Env>,
        }
        let Ok(req) = serde_json::from_slice::<Req>(body) else { return String::new() };
        let Some(url) = req.env.and_then(|e| e.url) else { return String::new() };
        return query_param(&url, "bench_sid").unwrap_or_default();
    }
    #[derive(Deserialize)]
    struct Req {
        #[serde(default)]
        connection_identifiers: Option<String>,
    }
    #[derive(Deserialize)]
    struct Ids {
        #[serde(default)]
        id: Option<String>,
    }
    let Ok(req) = serde_json::from_slice::<Req>(body) else { return String::new() };
    let Some(ids) = req.connection_identifiers else { return String::new() };
    serde_json::from_str::<Ids>(&ids).ok().and_then(|i| i.id).unwrap_or_default()
}

/// The first value of query parameter `key` in `url`, percent-decoded.
fn query_param(url: &str, key: &str) -> Option<String> {
    let query = url.split_once('?')?.1;
    let query = query.split_once('#').map_or(query, |(q, _)| q);
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (percent_decode(k)? == key).then(|| percent_decode(v)).flatten()
    })
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 2;
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids() {
        assert_eq!("a b", session_id(RpcMethod::Connect, br#"{"env":{"url":"ws://h/cable?x=1&bench_sid=a%20b"}}"#));
        assert_eq!("", session_id(RpcMethod::Connect, br#"{"env":{"url":"ws://h/cable"}}"#));
        assert_eq!("7", session_id(RpcMethod::Command, br#"{"connection_identifiers":"{\"id\":\"7\"}"}"#));
        assert_eq!("", session_id(RpcMethod::Disconnect, br#"{"identifiers":"{\"id\":\"7\"}"}"#));
    }

    #[test]
    fn subscribe_confirms_and_streams() {
        let out = command(br#"{"command":"subscribe","identifier":"x"}"#).expect("command");
        assert_eq!(
            r#"{"status":1,"streams":["bench:x"],"transmissions":["{\"identifier\":\"x\",\"type\":\"confirm_subscription\"}"]}"#,
            out
        );
    }
}
