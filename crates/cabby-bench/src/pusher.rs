//! The Pusher protocol (version 7, as pusher-js speaks it), for comparing Pusher-compatible
//! servers: Cabby and `anycable-go` in Pusher mode, Soketi, Laravel Reverb.
//!
//! Selected with `--protocol=pusher` on both scenarios ([`Protocol`]). The clients then:
//!
//! * dial `/app/<key>?protocol=7&client=js&version=8.4.0&flash=false` and wait for
//!   `pusher:connection_established` (the welcome);
//! * subscribe with `pusher:subscribe` to public channels (a channel is a stream) and wait
//!   for `pusher_internal:subscription_succeeded` (`pusher:subscription_error` and
//!   `pusher_internal:subscription_error` reject);
//! * answer the server's `pusher:ping` with `pusher:pong`, and send `pusher:ping` when
//!   nothing arrived for the advertised `activity_timeout`, as pusher-js does.
//!
//! Broadcasts go through the Pusher HTTP API ([`signed_path`]): `POST /apps/<id>/events`
//! with one `{"name","channel","data"}` event, or `POST /apps/<id>/batch_events` with
//! `{"batch":[..]}` when the publisher coalesces (at most [`MAX_BATCH`] events, Pusher's
//! limit). AnyCable's Pusher API has no `batch_events` endpoint, so a comparison that
//! includes it publishes one event per POST (`--publish-batch=1`). The event data is the
//! ledger payload, so reconciliation, latency and ordering checks are those of the
//! Action Cable mode ([`crate::ledger`]).

use std::borrow::Cow;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::value::RawValue;

use crate::gojson;

/// Pusher's limit on events per `batch_events` request.
pub const MAX_BATCH: usize = 10;

/// Name of every benchmark event.
pub const EVENT_NAME: &str = "bench";

/// The application the servers under test are configured with. The ids are not numeric,
/// so no server (Soketi parses environment values as JSON) turns them into numbers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PusherApp {
    pub app_id: String,
    pub key: String,
    pub secret: String,
}

impl Default for PusherApp {
    fn default() -> Self {
        Self {
            app_id: "cabby-bench".to_owned(),
            key: "cabby-bench-key".to_owned(),
            secret: "cabby-bench-secret".to_owned(),
        }
    }
}

impl PusherApp {
    /// The flags Cabby and `anycable-go` take to enable Pusher mode (the `sut` wrappers of
    /// Soketi and Reverb translate them).
    pub fn server_args(&self) -> Vec<String> {
        vec![
            format!("--pusher_app_id={}", self.app_id),
            format!("--pusher_app_key={}", self.key),
            format!("--pusher_secret={}", self.secret),
        ]
    }

    /// WebSocket path and query, as pusher-js 8 connects.
    pub fn websocket_path(&self) -> String {
        format!("/app/{}?protocol=7&client=js&version=8.4.0&flash=false", self.key)
    }

    /// `POST` path of single events.
    pub fn events_path(&self) -> String {
        format!("/apps/{}/events", self.app_id)
    }

    /// `POST` path of batches.
    pub fn batch_events_path(&self) -> String {
        format!("/apps/{}/batch_events", self.app_id)
    }
}

/// Which protocol the harness speaks (`--protocol`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Protocol {
    #[default]
    ActionCable,
    Pusher(PusherApp),
}

impl Protocol {
    /// Parses the `--protocol` flag: `actioncable` (default, also empty) or `pusher`.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "" | "actioncable" => Ok(Protocol::ActionCable),
            "pusher" => Ok(Protocol::Pusher(PusherApp::default())),
            other => Err(format!("--protocol: unknown protocol {other:?} (actioncable, pusher)")),
        }
    }

    pub fn pusher(&self) -> Option<&PusherApp> {
        match self {
            Protocol::Pusher(app) => Some(app),
            Protocol::ActionCable => None,
        }
    }

    /// The wire format of the clients.
    pub(crate) fn wire(&self) -> Wire {
        match self {
            Protocol::ActionCable => Wire::ActionCable,
            Protocol::Pusher(_) => Wire::Pusher,
        }
    }
}

/// The client-side wire format, without the application credentials.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Wire {
    #[default]
    ActionCable,
    Pusher,
}

pub(crate) const CONNECTION_ESTABLISHED: &str = "pusher:connection_established";
pub(crate) const SUBSCRIPTION_SUCCEEDED: &str = "pusher_internal:subscription_succeeded";
pub(crate) const PING: &str = "pusher:ping";
pub(crate) const PONG: &str = "pusher:pong";
pub(crate) const ERROR: &str = "pusher:error";
pub(crate) const PING_FRAME: &str = r#"{"event":"pusher:ping","data":{}}"#;
pub(crate) const PONG_FRAME: &str = r#"{"event":"pusher:pong","data":{}}"#;

/// Whether `event` rejects a subscription (Pusher's name, and the one AnyCable uses).
pub(crate) fn is_subscription_error(event: &str) -> bool {
    event == "pusher:subscription_error" || event == "pusher_internal:subscription_error"
}

/// `{"event":"pusher:subscribe","data":{"channel":<channel>}}`.
pub(crate) fn subscribe_frame(channel: &str) -> String {
    let mut out = String::from(r#"{"event":"pusher:subscribe","data":{"channel":"#);
    gojson::push_str(&mut out, channel);
    out.push_str("}}");
    out
}

/// A server frame. `data` stays unparsed: a JSON string for events (decoded by the
/// receiver), an object or a string for protocol events.
#[derive(Deserialize)]
pub(crate) struct Frame<'a> {
    #[serde(default, borrow)]
    pub event: Cow<'a, str>,
    #[serde(default, borrow)]
    pub channel: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    pub data: Option<&'a RawValue>,
}

/// The `activity_timeout` of a `pusher:connection_established` frame's data (a JSON
/// string holding an object), if any.
pub(crate) fn activity_timeout(data: Option<&RawValue>) -> Option<u64> {
    #[derive(Deserialize)]
    struct Established {
        activity_timeout: Option<u64>,
    }
    let raw = data?.get();
    let text: Cow<str> = match serde_json::from_str::<String>(raw) {
        Ok(s) => Cow::Owned(s),
        Err(_) => Cow::Borrowed(raw),
    };
    serde_json::from_str::<Established>(&text).ok()?.activity_timeout
}

/// One event in a `POST .../events` or `.../batch_events` body.
pub(crate) fn push_event(out: &mut String, channel: &str, data: &str) {
    out.push_str(r#"{"name":""#);
    out.push_str(EVENT_NAME);
    out.push_str(r#"","channel":"#);
    gojson::push_str(out, channel);
    out.push_str(r#","data":"#);
    gojson::push_str(out, data);
    out.push('}');
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 15)] as char);
    }
    out
}

/// `path?<query>` of a signed Pusher HTTP API `POST` of `body` at `timestamp` (Unix
/// seconds): `auth_key`, `auth_timestamp`, `auth_version=1.0`, `body_md5` and the
/// HMAC-SHA256 `auth_signature` of `POST\n<path>\n<sorted query>`.
pub fn signed_path_at(app: &PusherApp, path: &str, body: &[u8], timestamp: u64) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    use md5::Digest as _;

    let body_md5 = hex(&md5::Md5::digest(body));
    let query = format!("auth_key={}&auth_timestamp={timestamp}&auth_version=1.0&body_md5={body_md5}", app.key);
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(app.secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(format!("POST\n{path}\n{query}").as_bytes());
    let signature = hex(&mac.finalize().into_bytes());
    format!("{path}?{query}&auth_signature={signature}")
}

/// [`signed_path_at`] now.
pub fn signed_path(app: &PusherApp, path: &str, body: &[u8]) -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    signed_path_at(app, path, body, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_like_the_pusher_docs_worked_example() {
        // https://pusher.com/docs/channels/library_auth_reference/rest-api/#worked-authentication-example
        let app =
            PusherApp { app_id: "3".into(), key: "278d425bdf160c739803".into(), secret: "7ad3773142a6692b25b8".into() };
        let body = br#"{"name":"foo","channels":["project-3"],"data":"{\"some\":\"data\"}"}"#;
        assert_eq!(
            "/apps/3/events?auth_key=278d425bdf160c739803&auth_timestamp=1353088179&auth_version=1.0\
             &body_md5=ec365a775a4cd0599faeb73354201b6f\
             &auth_signature=da454824c97ba181a32ccc17a72625ba02771f50b50e1e7430e47a1f3f457e6c",
            signed_path_at(&app, "/apps/3/events", body, 1353088179)
        );
    }

    #[test]
    fn frames() {
        assert_eq!(
            r#"{"event":"pusher:subscribe","data":{"channel":"a<b"}}"#,
            subscribe_frame("a<b").replace("\\u003c", "<")
        );
        let mut e = String::new();
        push_event(&mut e, "7", "7:1|0|5");
        assert_eq!(r#"{"name":"bench","channel":"7","data":"7:1|0|5"}"#, e);
        let f: Frame = serde_json::from_str(
            r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"1.2\",\"activity_timeout\":30}"}"#,
        )
        .unwrap();
        assert_eq!(Some(30), activity_timeout(f.data));
        let f: Frame = serde_json::from_str(r#"{"event":"x","data":{"activity_timeout":12}}"#).unwrap();
        assert_eq!(Some(12), activity_timeout(f.data));
        assert_eq!(Ok(Protocol::ActionCable), Protocol::parse(""));
        assert!(Protocol::parse("stomp").is_err());
    }
}
