//! The WebSocket endpoint: handshake, origin check, subprotocol negotiation and
//! upgrade (upstream `ws.WebsocketHandler`, on gorilla's `Upgrader`).

use std::sync::Arc;

use base64::Engine as _;
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tungstenite::protocol::{Role, WebSocketConfig};

use cabby_core::logger::{Attr, Level, LogValue};

use super::connection::{self, CLOSE_ABNORMAL_CLOSURE, WsConnection};
use super::{Config, OriginCheck, check_origin};
use crate::server::{
    self, ConnInfo, Handler, HeadersExtractor, HttpRequest, Request, RequestInfo, Response, ResponseFuture,
};

/// Called when a session ends (upstream `callback`).
pub type SessionCallback = Box<dyn FnOnce() + Send>;

/// Sets up the session of an upgraded connection (upstream `sessionHandler`).
/// It runs on the node's bounded command pool ([`crate::node::execute`]: the node
/// API is synchronous and authentication performs an RPC call) and should return
/// once the session is serving, without waiting for it to end; an error is
/// logged.
pub type SessionHandler = Arc<dyn Fn(WsConnection, RequestInfo, SessionCallback) -> Result<(), String> + Send + Sync>;

/// Headers extractor shared with the upgrade tasks.
pub type SharedHeadersExtractor = Arc<dyn HeadersExtractor + Send + Sync>;

fn log(level: Level, msg: &str, attrs: &[Attr]) {
    let mut all = Vec::with_capacity(attrs.len() + 1);
    all.push(Attr::new("context", LogValue::String("ws".into())));
    all.extend_from_slice(attrs);
    crate::metrics::log(level, msg, &all);
}

struct Inner {
    subprotocols: Vec<String>,
    extractor: Option<SharedHeadersExtractor>,
    origins: OriginCheck,
    ws_config: WebSocketConfig,
    session_handler: SessionHandler,
}

/// The WebSocket endpoint (upstream `ws.WebsocketHandler`); mount it on a server
/// with [`HttpServer::setup_handler`](crate::server::HttpServer::setup_handler).
#[derive(Clone)]
pub struct WebsocketHandler {
    inner: Arc<Inner>,
}

/// Upstream `WebsocketHandler(subprotocols, headersExtractor, config, l,
/// sessionHandler)`. An empty `subprotocols` negotiates none.
pub fn websocket_handler(
    subprotocols: Vec<String>,
    extractor: Option<SharedHeadersExtractor>,
    config: &Config,
    session_handler: SessionHandler,
) -> WebsocketHandler {
    if config.enable_compression {
        log(Level::Warn, "WebSocket compression is not supported; connections are not compressed", &[]);
    }
    WebsocketHandler {
        inner: Arc::new(Inner {
            subprotocols,
            extractor,
            origins: check_origin(&config.allowed_origins),
            ws_config: websocket_config(config),
            session_handler,
        }),
    }
}

/// gorilla's buffer defaults (4096) apply to sizes of 0 or less; a message size
/// of 0 or less means no limit.
fn websocket_config(config: &Config) -> WebSocketConfig {
    let size = |v: i64| usize::try_from(v).ok().filter(|v| *v > 0).unwrap_or(4096);
    let max_message = usize::try_from(config.max_message_size).ok().filter(|v| *v > 0);
    WebSocketConfig::default()
        .read_buffer_size(size(config.read_buffer_size))
        .write_buffer_size(size(config.write_buffer_size))
        .max_message_size(max_message)
        .max_frame_size(max_message)
}

/// Whether a comma-separated header (any of its occurrences) contains `token`,
/// case-insensitively (gorilla `tokenListContainsValue`).
fn token_list_contains(headers: &http::HeaderMap, name: http::header::HeaderName, token: &str) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|v| v.split(',').map(|t| t.trim_matches([' ', '\t'])).any(|t| t.eq_ignore_ascii_case(token)))
}

/// gorilla `returnError`: a plain-text status reply advertising version 13.
fn handshake_error(status: u16) -> Response {
    let text = http::StatusCode::from_u16(status).ok().and_then(|s| s.canonical_reason()).unwrap_or("");
    let mut resp = server::error_response(status, text);
    resp.headers_mut().insert("sec-websocket-version", http::HeaderValue::from_static("13"));
    resp
}

/// gorilla `isValidChallengeKey`: base64 of 16 bytes.
fn valid_challenge_key(key: &str) -> bool {
    !key.is_empty() && base64::engine::general_purpose::STANDARD.decode(key).is_ok_and(|k| k.len() == 16)
}

/// The first server subprotocol the client offers (gorilla `selectSubprotocol`).
fn select_subprotocol<'a>(server: &'a [String], client_header: &str) -> Option<&'a str> {
    let client_header = client_header.trim();
    if client_header.is_empty() {
        return None;
    }
    server.iter().find(|s| client_header.split(',').any(|c| c.trim() == s.as_str())).map(String::as_str)
}

impl Inner {
    /// Validates the handshake in gorilla's order: `Ok((accept key, subprotocol))`, or the error status.
    fn handshake(&self, req: &Request) -> Result<(String, String), u16> {
        use http::header::{
            CONNECTION, ORIGIN, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION, UPGRADE,
        };

        let headers = req.headers();
        if !token_list_contains(headers, CONNECTION, "upgrade") || !token_list_contains(headers, UPGRADE, "websocket") {
            log(
                Level::Debug,
                "WebSocket connection upgrade failed",
                &[Attr::new(
                    "error",
                    LogValue::String("websocket: the client is not using the websocket protocol".into()),
                )],
            );
            return Err(400);
        }
        if req.method() != http::Method::GET {
            return Err(405);
        }
        if !token_list_contains(headers, SEC_WEBSOCKET_VERSION, "13") {
            return Err(400);
        }
        let origin = headers.get(ORIGIN).map(|o| o.to_str().unwrap_or(""));
        if !self.origins.allows(origin) {
            log(
                Level::Debug,
                "WebSocket connection upgrade failed",
                &[Attr::new(
                    "error",
                    LogValue::String("websocket: request origin not allowed by Upgrader.CheckOrigin".into()),
                )],
            );
            return Err(403);
        }
        let key = headers.get(SEC_WEBSOCKET_KEY).and_then(|k| k.to_str().ok()).unwrap_or("");
        if !valid_challenge_key(key) {
            return Err(400);
        }
        let client_protocols = headers.get(SEC_WEBSOCKET_PROTOCOL).and_then(|p| p.to_str().ok()).unwrap_or("");
        let subprotocol = select_subprotocol(&self.subprotocols, client_protocols).unwrap_or("").to_owned();
        Ok((tungstenite::handshake::derive_accept_key(key.as_bytes()), subprotocol))
    }

    /// Runs after the 101 response: takes the socket over, starts the session and
    /// drives the connection until it ends.
    async fn serve_upgraded(
        self: Arc<Self>,
        on_upgrade: hyper::upgrade::OnUpgrade,
        request: HttpRequest,
        subprotocol: String,
    ) {
        let upgraded = match on_upgrade.await {
            Ok(upgraded) => upgraded,
            Err(e) => {
                log(
                    Level::Debug,
                    "WebSocket connection upgrade failed",
                    &[Attr::new("error", LogValue::String(e.to_string()))],
                );
                return;
            }
        };
        let stream = WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(self.ws_config)).await;
        let (conn, io_loop) = connection::connect(stream, subprotocol);

        let extractor = self.extractor.as_deref().map(|e| e as &dyn HeadersExtractor);
        match RequestInfo::new(&request, extractor) {
            Err(e) => connection::close_without_waiting(&conn, CLOSE_ABNORMAL_CLOSURE, &e.0),
            Ok(info) => {
                let client_protocols = request.header("Sec-Websocket-Protocol");
                if conn.subprotocol().is_empty() && !client_protocols.is_empty() {
                    log(
                        Level::Debug,
                        "no subprotocol negotiated",
                        &[
                            Attr::new("sid", LogValue::String(info.uid.clone())),
                            Attr::new("client", LogValue::String(client_protocols.to_owned())),
                        ],
                    );
                }
                let handler = Arc::clone(&self.session_handler);
                crate::node::execute(Box::new(move || {
                    let sid = info.uid.clone();
                    log(
                        Level::Debug,
                        "WebSocket session established",
                        &[Attr::new("sid", LogValue::String(sid.clone()))],
                    );
                    let completed_sid = sid.clone();
                    let callback: SessionCallback = Box::new(move || {
                        log(
                            Level::Debug,
                            "WebSocket session completed",
                            &[Attr::new("sid", LogValue::String(completed_sid))],
                        );
                    });
                    if let Err(e) = handler(conn, info, callback) {
                        log(
                            Level::Error,
                            "WebSocket session failed",
                            &[Attr::new("sid", LogValue::String(sid)), Attr::new("error", LogValue::String(e))],
                        );
                    }
                }));
            }
        }
        // Nothing of the request outlives the setup: an idle connection holds only
        // its io loop.
        drop(request);
        drop(self);
        io_loop.await;
    }
}

impl Handler for WebsocketHandler {
    fn call(&self, mut req: Request) -> ResponseFuture {
        let (accept, subprotocol) = match self.inner.handshake(&req) {
            Ok(ok) => ok,
            Err(status) => return Box::pin(std::future::ready(handshake_error(status))),
        };

        let on_upgrade = hyper::upgrade::on(&mut req);
        let conn: ConnInfo = server::conn_info(&req);
        let (parts, _body) = req.into_parts();
        let request = HttpRequest::from_parts(&parts, &conn);

        let mut resp = http::Response::new(server::empty());
        *resp.status_mut() = http::StatusCode::SWITCHING_PROTOCOLS;
        let headers = resp.headers_mut();
        headers.insert(http::header::UPGRADE, http::HeaderValue::from_static("websocket"));
        headers.insert(http::header::CONNECTION, http::HeaderValue::from_static("Upgrade"));
        if let Ok(v) = http::HeaderValue::from_str(&accept) {
            headers.insert(http::header::SEC_WEBSOCKET_ACCEPT, v);
        }
        if !subprotocol.is_empty()
            && let Ok(v) = http::HeaderValue::from_str(&subprotocol)
        {
            headers.insert(http::header::SEC_WEBSOCKET_PROTOCOL, v);
        }
        if let Ok(v) = http::HeaderValue::from_str(&crate::telemetry::version()) {
            headers.insert("x-anycable-version", v);
        }

        tokio::spawn(Arc::clone(&self.inner).serve_upgraded(on_upgrade, request, subprotocol));
        Box::pin(std::future::ready(resp))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_the_first_server_protocol_the_client_offers() {
        let server = vec!["actioncable-v1-json".to_owned(), "actioncable-v1-ext-json".to_owned()];
        assert_eq!(
            Some("actioncable-v1-json"),
            select_subprotocol(&server, "actioncable-v1-ext-json, actioncable-v1-json")
        );
        assert_eq!(Some("actioncable-v1-ext-json"), select_subprotocol(&server, "actioncable-v1-ext-json"));
        assert_eq!(None, select_subprotocol(&server, "other"));
        assert_eq!(None, select_subprotocol(&[], "actioncable-v1-json"));
    }

    #[test]
    fn validates_challenge_keys() {
        assert!(valid_challenge_key("dGhlIHNhbXBsZSBub25jZQ=="));
        assert!(!valid_challenge_key(""));
        assert!(!valid_challenge_key("c2hvcnQ="));
    }
}
