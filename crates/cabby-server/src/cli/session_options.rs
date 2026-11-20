//! Session options from the connection request and the configuration
//! (upstream `cli/session_options.go`), and the default WebSocket handler
//! (upstream `Runner.defaultWebSocketHandler`).

use std::sync::Arc;
use std::time::Duration;

use cabby_core::logger::{Attr, Level, LogValue};

use crate::config::Config;
use crate::node::{Connection, Node, Session, SessionOption};
use crate::server::{DefaultHeadersExtractor, RequestInfo};
use crate::ws::{self, SessionCallback, SessionHandler, WsConnection};

const PING_INTERVAL_PARAMETER: &str = "pi";
const PING_PRECISION_PARAMETER: &str = "ptp";
const PREV_SESSION_HEADER: &str = "X-ANYCABLE-RESTORE-SID";
const PREV_SESSION_PARAM: &str = "sid";

/// Upstream `common.ActionCableV1JSON`.
pub const ACTION_CABLE_V1_JSON: &str = "actioncable-v1-json";
/// Upstream `common.ActionCableV1ExtJSON`.
pub const ACTION_CABLE_V1_EXT_JSON: &str = "actioncable-v1-ext-json";

/// Upstream `common.ActionCableProtocols()`.
pub fn action_cable_protocols() -> Vec<String> {
    vec![ACTION_CABLE_V1_JSON.to_owned(), ACTION_CABLE_V1_EXT_JSON.to_owned()]
}

/// Upstream `common.IsExtendedActionCableProtocol`.
pub fn is_extended_action_cable_protocol(protocol: &str) -> bool {
    protocol == ACTION_CABLE_V1_EXT_JSON
}

fn secs(v: i64) -> Duration {
    Duration::from_secs(u64::try_from(v).unwrap_or(0))
}

/// The settings session options are derived from (a copy, so the handler does
/// not hold the whole configuration).
#[derive(Debug, Clone)]
pub(super) struct SessionSettings {
    write_timeout: i64,
    max_pending_size: u64,
    sessions_ttl: i64,
    presence_ttl: i64,
    pong_timeout: i64,
    enable_native_ping: bool,
}

impl SessionSettings {
    pub(super) fn new(c: &Config) -> Self {
        Self {
            write_timeout: c.ws.write_timeout,
            max_pending_size: c.ws.max_pending_size,
            sessions_ttl: c.broker.sessions_ttl,
            presence_ttl: c.broker.presence_ttl,
            pong_timeout: c.app.pong_timeout,
            enable_native_ping: c.app.enable_native_ping,
        }
    }

    /// Upstream `sessionOptionsFromWSConfig`.
    fn ws_config_options(&self) -> Vec<SessionOption> {
        vec![
            SessionOption::WriteTimeout(secs(self.write_timeout)),
            SessionOption::MaxPendingSize(self.max_pending_size),
        ]
    }

    /// Upstream `sessionOptionsFromProtocol`.
    fn protocol_options(&self, protocol: &str) -> Vec<SessionOption> {
        let mut opts = Vec::new();
        if is_extended_action_cable_protocol(protocol) {
            // Session cache and presence keepalive intervals, slightly less than
            // the corresponding TTLs.
            opts.push(SessionOption::KeepaliveIntervals {
                sessions_ttl: Duration::from_millis(u64::try_from(self.sessions_ttl * 500).unwrap_or(0)),
                presence_ttl: Duration::from_millis(u64::try_from(self.presence_ttl * 500).unwrap_or(0)),
            });
            if self.pong_timeout > 0 {
                opts.push(SessionOption::PongTimeout(secs(self.pong_timeout)));
            }
        }
        if self.enable_native_ping && self.pong_timeout > 0 {
            opts.push(SessionOption::PongTimeout(secs(self.pong_timeout)));
        }
        opts
    }

    /// Upstream `sessionOptionsFromParams`.
    fn params_options(info: &RequestInfo) -> Vec<SessionOption> {
        let mut opts = Vec::new();
        let raw = info.param(PING_INTERVAL_PARAMETER);
        if !raw.is_empty() {
            match go_atoi(&raw) {
                Some(val) => opts.push(SessionOption::PingInterval(secs(val))),
                None => crate::metrics::log(
                    Level::Warn,
                    "invalid ping interval value, must be integer",
                    &[Attr::new("val", LogValue::String(raw))],
                ),
            }
        }
        let precision = info.param(PING_PRECISION_PARAMETER);
        if !precision.is_empty() {
            opts.push(SessionOption::PingPrecision(precision));
        }
        let header = info.anycable_header(PREV_SESSION_HEADER);
        if !header.is_empty() {
            opts.push(SessionOption::PrevSid(header));
        } else {
            let param = info.param(PREV_SESSION_PARAM);
            if !param.is_empty() {
                opts.push(SessionOption::PrevSid(param));
            }
        }
        opts
    }
}

/// Go `strconv.Atoi` (decimal, optional sign, fits an `int`).
fn go_atoi(s: &str) -> Option<i64> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// The headers extractor of the client endpoints (upstream
/// `DefaultHeadersExtractor{Headers: RPC.ProxyHeaders, Cookies: RPC.ProxyCookies}`
/// plus the JWT header).
pub(super) fn headers_extractor(c: &Config) -> DefaultHeadersExtractor {
    DefaultHeadersExtractor {
        auth_header: if c.jwt.enabled() { c.jwt.header_key() } else { String::new() },
        headers: c.rpc.proxy_headers.clone(),
        cookies: c.rpc.proxy_cookies.clone(),
    }
}

/// Upstream `Runner.defaultWebSocketHandler`: an Action Cable session per
/// connection, authenticated on connect, then served without a thread of its
/// own (the connection pushes messages to the session's reader).
pub(super) fn default_websocket_handler(node: Arc<Node>, c: &Config) -> ws::WebsocketHandler {
    let settings = SessionSettings::new(c);
    let handler: SessionHandler = Arc::new(move |conn: WsConnection, info: RequestInfo, callback: SessionCallback| {
        if !node.admit_new_client() {
            ws::close_without_waiting(&conn, ws::CLOSE_TRY_AGAIN_LATER, "Try again later");
            return Ok(());
        }

        let mut opts = settings.ws_config_options();
        opts.extend(settings.protocol_options(conn.subprotocol()));
        opts.extend(SessionSettings::params_options(&info));

        let headers = info.headers.clone().unwrap_or_default();
        let session = Session::new(
            &node,
            Arc::new(conn.clone()) as Arc<dyn Connection>,
            &info.url,
            Some(headers),
            &info.uid,
            opts,
        );

        if session.authenticate_on_connect() {
            node.authenticate(&session, &[]).map_err(|e| e.0)?;
        }

        let pong = Arc::downgrade(&session);
        conn.set_pong_handler(move || {
            if let Some(session) = pong.upgrade() {
                session.handle_native_pong();
            }
        });

        conn.set_inbound_handler(Arc::new(session.reader(callback)));
        Ok(())
    });
    ws::websocket_handler(action_cable_protocols(), Some(Arc::new(headers_extractor(c))), &c.ws, handler)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ping_intervals_like_go_atoi() {
        assert_eq!(Some(3), go_atoi("3"));
        assert_eq!(Some(-1), go_atoi("-1"));
        assert_eq!(None, go_atoi("1.5"));
        assert_eq!(None, go_atoi(""));
    }

    #[test]
    fn extended_protocol_sets_keepalive_and_pong_timeout() {
        let mut c = Config::new();
        c.app.pong_timeout = 6;
        let opts = SessionSettings::new(&c).protocol_options(ACTION_CABLE_V1_EXT_JSON);
        assert_eq!(2, opts.len());
        assert!(SessionSettings::new(&c).protocol_options(ACTION_CABLE_V1_JSON).is_empty());
    }
}
