//! Client message encoders (upstream package `encoders`).
//!
//! The frame types live here (upstream `ws.FrameType` / `ws.SentFrame`) so that
//! encoders stay in `cabby-core`; `cabby_server::ws` re-exports them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::common::{self, DisconnectMessage, Message, PingMessage, Reply};
use crate::gojson;

/// WebSocket frame kind (upstream `ws.FrameType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FrameType {
    #[default]
    Text,
    Close,
    Binary,
}

/// An encoded frame ready to be written (upstream `ws.SentFrame`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SentFrame {
    pub frame_type: FrameType,
    pub payload: Vec<u8>,
    pub close_code: i32,
    pub close_reason: String,
}

impl SentFrame {
    /// A text frame with the given payload.
    pub fn text(payload: Vec<u8>) -> Self {
        SentFrame { frame_type: FrameType::Text, payload, ..SentFrame::default() }
    }
}

/// A message that can be encoded for clients (upstream `encoders.EncodedMessage`).
pub trait EncodedMessage {
    fn get_type(&self) -> String;
}

impl EncodedMessage for Reply {
    fn get_type(&self) -> String {
        self.kind.clone()
    }
}

impl EncodedMessage for PingMessage {
    fn get_type(&self) -> String {
        common::PING_TYPE.to_owned()
    }
}

impl EncodedMessage for DisconnectMessage {
    fn get_type(&self) -> String {
        common::DISCONNECT_TYPE.to_owned()
    }
}

impl EncodedMessage for common::EncodedMessage {
    fn get_type(&self) -> String {
        match self {
            common::EncodedMessage::Reply(m) => m.get_type(),
            common::EncodedMessage::Ping(m) => m.get_type(),
            common::EncodedMessage::Disconnect(m) => m.get_type(),
        }
    }
}

/// An encoding failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct EncodeError(pub String);

/// A client protocol encoder (upstream `encoders.Encoder`).
///
/// Upstream encoders may return a nil frame with a nil error: nothing to send
/// (e.g. an SSE encoder in raw mode skips protocol events). Such encoders
/// implement [`Encoder::encode_optional`] and
/// [`Encoder::encode_transmission_optional`], returning `Ok(None)`; their
/// [`Encoder::encode`] and [`Encoder::encode_transmission`] report the missing
/// frame as an error, so callers that only know the frame-returning methods
/// skip the message, as upstream does. Callers that want upstream's exact
/// semantics (send nothing, report nothing) use the optional variants.
pub trait Encoder {
    fn id(&self) -> String;
    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError>;
    fn encode_transmission(&self, msg: &str) -> Result<SentFrame, EncodeError>;
    fn decode(&self, payload: &[u8]) -> Result<Message, EncodeError>;

    /// Upstream `Encode` with its nil frame: `Ok(None)` means nothing to send.
    fn encode_optional(&self, msg: &dyn EncodedMessageJson) -> Result<Option<SentFrame>, EncodeError> {
        self.encode(msg).map(Some)
    }

    /// Upstream `EncodeTransmission` with its nil frame: `Ok(None)` means nothing to send.
    fn encode_transmission_optional(&self, msg: &str) -> Result<Option<SentFrame>, EncodeError> {
        self.encode_transmission(msg).map(Some)
    }
}

/// The error an encoder's frame-returning methods report where upstream returns
/// a nil frame (see [`Encoder`]).
pub fn no_frame() -> EncodeError {
    EncodeError(NO_FRAME.to_owned())
}

/// Text of [`no_frame`].
pub const NO_FRAME: &str = "nothing to encode";

/// Messages the JSON encoder can serialize (upstream relies on `json.Marshal` of
/// any `EncodedMessage`).
pub trait EncodedMessageJson: EncodedMessage {
    fn to_json_value(&self) -> serde_json::Value;

    /// The message as a `*common.Reply`, if it is one (upstream encoders use a
    /// type assertion, e.g. `msg.(*common.Reply)`).
    fn as_reply(&self) -> Option<&Reply> {
        None
    }

    /// The message as a `*common.DisconnectMessage`, if it is one.
    fn as_disconnect(&self) -> Option<&DisconnectMessage> {
        None
    }

    /// Appends the Go `json.Marshal` encoding to `out`. The default goes through
    /// [`EncodedMessageJson::to_json_value`]; the `common` types write directly.
    fn write_json(&self, out: &mut Vec<u8>) {
        gojson::write(out, &self.to_json_value());
    }
}

/// Implements [`EncodedMessageJson`] for a `Serialize` type with Go JSON tags.
macro_rules! serialize_json {
    ($($ty:ty),*) => {$(
        impl EncodedMessageJson for $ty {
            fn to_json_value(&self) -> serde_json::Value {
                serde_json::to_value(self).expect("JSON value of a message")
            }

            fn write_json(&self, out: &mut Vec<u8>) {
                gojson::write(out, self);
            }
        }
    )*};
}

/// Implements [`EncodedMessageJson`] for a `Serialize` type with Go JSON tags,
/// with the given type assertions.
macro_rules! serialize_json_as {
    ($ty:ty { $($method:ident -> $target:ty = |$m:ident| $body:expr),* }) => {
        impl EncodedMessageJson for $ty {
            fn to_json_value(&self) -> serde_json::Value {
                serde_json::to_value(self).expect("JSON value of a message")
            }

            fn write_json(&self, out: &mut Vec<u8>) {
                gojson::write(out, self);
            }

            $(fn $method(&self) -> Option<&$target> {
                let $m = self;
                $body
            })*
        }
    };
}

serialize_json!(PingMessage);
serialize_json_as!(Reply { as_reply -> Reply = |m| Some(m) });
serialize_json_as!(DisconnectMessage { as_disconnect -> DisconnectMessage = |m| Some(m) });
serialize_json_as!(common::EncodedMessage {
    as_reply -> Reply = |m| match m {
        common::EncodedMessage::Reply(r) => Some(r),
        _ => None,
    },
    as_disconnect -> DisconnectMessage = |m| match m {
        common::EncodedMessage::Disconnect(d) => Some(d),
        _ => None,
    }
});

/// Identifier of the [`Json`] encoder.
pub const JSON_ENCODER_ID: &str = "json";

/// The Action Cable JSON encoder (upstream `encoders.JSON`).
#[derive(Debug, Clone, Copy, Default)]
pub struct Json;

impl Encoder for Json {
    fn id(&self) -> String {
        JSON_ENCODER_ID.to_owned()
    }

    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        let mut payload = Vec::with_capacity(128);
        msg.write_json(&mut payload);
        Ok(SentFrame::text(payload))
    }

    fn encode_transmission(&self, msg: &str) -> Result<SentFrame, EncodeError> {
        Ok(SentFrame::text(msg.as_bytes().to_vec()))
    }

    fn decode(&self, payload: &[u8]) -> Result<Message, EncodeError> {
        Message::from_json(payload).map_err(|e| EncodeError(e.0))
    }
}

/// Encoding callback: `Ok(None)` mirrors upstream returning a nil frame.
pub type EncodingFunction<'a> = dyn FnMut(&dyn EncodedMessage) -> Result<Option<SentFrame>, EncodeError> + 'a;

/// Caches the encoded form of one message per encoder ID (upstream `EncodingCache`):
/// the callback runs once per encoder, and its frame or error is replayed afterwards.
#[derive(Debug, Default)]
pub struct EncodingCache {
    /// Per encoder: the frame, or `None` when encoding failed (or produced no frame).
    encoded: HashMap<String, Option<SentFrame>>,
    errors: HashMap<String, EncodeError>,
}

impl EncodingCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the cached frame for `encoder`, calling `callback` only on the first fetch.
    pub fn fetch(
        &mut self,
        msg: &dyn EncodedMessage,
        encoder: &str,
        callback: &mut EncodingFunction<'_>,
    ) -> Result<SentFrame, EncodeError> {
        if !self.encoded.contains_key(encoder) {
            let frame = match callback(msg) {
                Ok(frame) => frame,
                Err(err) => {
                    self.errors.insert(encoder.to_owned(), err);
                    None
                }
            };
            self.encoded.insert(encoder.to_owned(), frame);
        }

        match self.encoded.get(encoder) {
            Some(Some(frame)) => Ok(frame.clone()),
            // Upstream: `errorx.Decorate(err, "encoding failed")`.
            _ => Err(EncodeError(match self.errors.get(encoder) {
                Some(err) => format!("encoding failed, cause: {err}"),
                None => "encoding failed".to_owned(),
            })),
        }
    }
}

/// Outcome of encoding a [`CachedEncodedMessage`] for one encoder: a frame shared by
/// every session using that encoder, or the (decorated) error.
pub type SharedFrame = Result<Arc<SentFrame>, EncodeError>;

/// A message whose encoding is computed once per encoder ID and then shared
/// (upstream `encoders.CachedEncodedMessage`).
///
/// The hub builds one per channel identifier and broadcast and passes it to every
/// subscribed session; each session fetches the frame for its encoder. A broadcast
/// to N sessions using one encoder is therefore encoded once, and the frame is
/// shared through an `Arc` instead of being copied per session.
///
/// Unlike upstream's unsynchronized cache this type is `Sync`. The first encoder ID
/// is cached in a lock-free slot, so the common single-encoder case costs one
/// atomic load per fetch; further encoders go to a small mutex-guarded list.
pub struct CachedEncodedMessage {
    target: common::EncodedMessage,
    first: OnceLock<(Box<str>, SharedFrame)>,
    rest: Mutex<Vec<(Box<str>, SharedFrame)>>,
}

fn encode_once(
    target: &common::EncodedMessage,
    callback: impl FnOnce(&common::EncodedMessage) -> Result<Option<SentFrame>, EncodeError>,
) -> SharedFrame {
    match callback(target) {
        Ok(Some(frame)) => Ok(Arc::new(frame)),
        // Upstream: `errorx.Decorate(err, "encoding failed")` (a nil error when the
        // callback returned a nil frame).
        Ok(None) => Err(EncodeError("encoding failed".to_owned())),
        Err(err) => Err(EncodeError(format!("encoding failed, cause: {err}"))),
    }
}

impl CachedEncodedMessage {
    /// Upstream `NewCachedEncodedMessage(msg)`.
    pub fn new(target: common::EncodedMessage) -> Self {
        Self { target, first: OnceLock::new(), rest: Mutex::new(Vec::new()) }
    }

    /// The wrapped message.
    pub fn target(&self) -> &common::EncodedMessage {
        &self.target
    }

    /// Returns the frame cached for `encoder`, calling `callback` only on the first
    /// fetch for that encoder (upstream `Fetch`). A callback error, or `Ok(None)`
    /// (a nil frame upstream), is cached as a failure with upstream's texts.
    pub fn fetch(
        &self,
        encoder: &str,
        callback: impl FnOnce(&common::EncodedMessage) -> Result<Option<SentFrame>, EncodeError>,
    ) -> SharedFrame {
        let mut callback = Some(callback);
        let (id, frame) = self.first.get_or_init(|| {
            let cb = callback.take().expect("callback runs at most once");
            (encoder.into(), encode_once(&self.target, cb))
        });
        if **id == *encoder {
            return frame.clone();
        }

        let mut rest = self.rest.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((_, frame)) = rest.iter().find(|(id, _)| **id == *encoder) {
            return frame.clone();
        }
        // The first slot belongs to another encoder, so the callback is still unused.
        let frame = encode_once(&self.target, callback.take().expect("callback runs at most once"));
        rest.push((encoder.into(), frame.clone()));
        frame
    }

    /// [`fetch`](Self::fetch) with `encoder.encode` as the callback (its nil
    /// frames included, see [`Encoder::encode_optional`]).
    pub fn fetch_with(&self, encoder: &dyn Encoder) -> SharedFrame {
        self.fetch(&encoder.id(), |msg| encoder.encode_optional(msg))
    }

    /// The [`Json`] encoder's (shared) frame for this message.
    pub fn json_frame(&self) -> SharedFrame {
        self.fetch(JSON_ENCODER_ID, |msg| Json.encode(msg).map(Some))
    }

    /// The `encoders.JSON` payload (upstream `MarshalJSON`), encoded once.
    pub fn to_json(&self) -> Vec<u8> {
        match self.json_frame() {
            Ok(frame) => frame.payload.clone(),
            Err(_) => self.target.to_json(),
        }
    }
}

impl std::fmt::Debug for CachedEncodedMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedEncodedMessage").field("target", &self.target).finish_non_exhaustive()
    }
}

impl From<common::EncodedMessage> for CachedEncodedMessage {
    fn from(msg: common::EncodedMessage) -> Self {
        Self::new(msg)
    }
}

impl EncodedMessage for CachedEncodedMessage {
    fn get_type(&self) -> String {
        self.target.get_type()
    }
}

impl EncodedMessageJson for CachedEncodedMessage {
    fn to_json_value(&self) -> serde_json::Value {
        self.target.to_json_value()
    }

    fn write_json(&self, out: &mut Vec<u8>) {
        self.target.write_json(out);
    }

    fn as_reply(&self) -> Option<&Reply> {
        self.target.as_reply()
    }

    fn as_disconnect(&self) -> Option<&DisconnectMessage> {
        self.target.as_disconnect()
    }
}
