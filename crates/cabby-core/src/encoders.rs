//! Client message encoders (upstream package `encoders`).
//!
//! The frame types live here (upstream `ws.FrameType` / `ws.SentFrame`) so that
//! encoders stay in `cabby-core`; `cabby_server::ws` re-exports them.

use std::collections::HashMap;

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
pub trait Encoder {
    fn id(&self) -> String;
    fn encode(&self, msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError>;
    fn encode_transmission(&self, msg: &str) -> Result<SentFrame, EncodeError>;
    fn decode(&self, payload: &[u8]) -> Result<Message, EncodeError>;
}

/// Messages the JSON encoder can serialize (upstream relies on `json.Marshal` of
/// any `EncodedMessage`).
pub trait EncodedMessageJson: EncodedMessage {
    fn to_json_value(&self) -> serde_json::Value;

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

serialize_json!(Reply, PingMessage, DisconnectMessage, common::EncodedMessage);

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
