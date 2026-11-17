//! Client message encoders (upstream package `encoders`).
//!
//! The frame types live here (upstream `ws.FrameType` / `ws.SentFrame`) so that
//! encoders stay in `cabby-core`; `cabby_server::ws` re-exports them.

use std::collections::HashMap;

use crate::common::{Message, Reply};
use crate::pending;

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

/// A message that can be encoded for clients (upstream `encoders.EncodedMessage`).
pub trait EncodedMessage {
    fn get_type(&self) -> String;
}

impl EncodedMessage for Reply {
    fn get_type(&self) -> String {
        pending!("encoders::reply_get_type")
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
}

impl EncodedMessageJson for Reply {
    fn to_json_value(&self) -> serde_json::Value {
        pending!("encoders::reply_to_json")
    }
}

/// The Action Cable JSON encoder (upstream `encoders.JSON`).
#[derive(Debug, Clone, Copy, Default)]
pub struct Json;

impl Encoder for Json {
    fn id(&self) -> String {
        pending!("encoders::json_id")
    }

    fn encode(&self, _msg: &dyn EncodedMessageJson) -> Result<SentFrame, EncodeError> {
        pending!("encoders::json_encode")
    }

    fn encode_transmission(&self, _msg: &str) -> Result<SentFrame, EncodeError> {
        pending!("encoders::json_encode_transmission")
    }

    fn decode(&self, _payload: &[u8]) -> Result<Message, EncodeError> {
        pending!("encoders::json_decode")
    }
}

/// Encoding callback: `Ok(None)` mirrors upstream returning a nil frame.
pub type EncodingFunction<'a> = dyn FnMut(&dyn EncodedMessage) -> Result<Option<SentFrame>, EncodeError> + 'a;

/// Caches the encoded form of one message per encoder ID (upstream `EncodingCache`).
#[derive(Debug, Default)]
pub struct EncodingCache {
    _encoded: HashMap<String, Option<SentFrame>>,
    _errors: HashMap<String, EncodeError>,
}

impl EncodingCache {
    pub fn new() -> Self {
        pending!("encoders::new_encoding_cache")
    }

    /// Returns the cached frame for `encoder`, calling `callback` only on the first fetch.
    pub fn fetch(
        &mut self,
        _msg: &dyn EncodedMessage,
        _encoder: &str,
        _callback: &mut EncodingFunction<'_>,
    ) -> Result<SentFrame, EncodeError> {
        pending!("encoders::encoding_cache_fetch")
    }
}
