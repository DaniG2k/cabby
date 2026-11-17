//! Facade for upstream package `encoders`.

use std::cell::Cell;

use cabby_core::common::{Message, Reply};
use cabby_core::encoders::{EncodeError, EncodedMessage, Encoder, EncodingCache, FrameType, Json, SentFrame};
use serde::Deserialize;
use serde_json::json;

use crate::reference::common::{GoMessage, GoParsed, GoReply};
use crate::{Backend, backend, bridge};

/// The bridge's `sentFrame` (payload as text).
#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoSentFrame {
    frame_type: i64,
    payload: String,
    close_code: i32,
    close_reason: String,
}

impl From<GoSentFrame> for SentFrame {
    fn from(g: GoSentFrame) -> Self {
        SentFrame {
            frame_type: match g.frame_type {
                0 => FrameType::Text,
                1 => FrameType::Close,
                2 => FrameType::Binary,
                other => panic!("unknown Go frame type {other}"),
            },
            payload: g.payload.into_bytes(),
            close_code: g.close_code,
            close_reason: g.close_reason,
        }
    }
}

fn frame_result(p: GoParsed<GoSentFrame>) -> Result<SentFrame, EncodeError> {
    p.into_result(EncodeError).map(|f| f.expect("frame").into())
}

/// `JSON{}.Encode(reply)`.
pub fn json_encode(reply: &Reply) -> Result<SentFrame, EncodeError> {
    match backend() {
        Backend::Rust => Json.encode(reply),
        Backend::Go => frame_result(bridge::call("encoders.JSON.Encode", json!({"Reply": GoReply::from(reply)}))),
    }
}

/// `JSON{}.EncodeTransmission(msg)`.
pub fn json_encode_transmission(msg: &str) -> Result<SentFrame, EncodeError> {
    match backend() {
        Backend::Rust => Json.encode_transmission(msg),
        Backend::Go => frame_result(bridge::call("encoders.JSON.EncodeTransmission", json!({"Msg": msg}))),
    }
}

/// `JSON{}.Decode(raw)`.
pub fn json_decode(raw: &[u8]) -> Result<Message, EncodeError> {
    match backend() {
        Backend::Rust => Json.decode(raw),
        Backend::Go => {
            let raw = std::str::from_utf8(raw).expect("UTF-8 test payload");
            let p: GoParsed<GoMessage> = bridge::call("encoders.JSON.Decode", json!({"Raw": raw}));
            p.into_result(EncodeError).map(|m| m.expect("message").into())
        }
    }
}

/// The upstream test's `MockMessage`: counts how often it was encoded.
pub struct MockMessage {
    pub encoded: Cell<i32>,
    pub value: String,
}

impl EncodedMessage for MockMessage {
    fn get_type(&self) -> String {
        "mock".into()
    }
}

/// Outcome of [`encoding_cache_fetch_mock`].
#[derive(Debug)]
pub struct MockFetches {
    /// One result per fetch, in order.
    pub frames: Vec<Result<SentFrame, EncodeError>>,
    /// The mock's `Encoded` counter after all fetches.
    pub encoded: i32,
}

/// Fetches a `MockMessage{Value: value}` from one fresh `EncodingCache` once per
/// entry of `encoders`, using the upstream test's callback (increments `Encoded` and
/// returns the value as a text frame).
pub fn encoding_cache_fetch_mock(value: &str, encoders: &[&str]) -> MockFetches {
    match backend() {
        Backend::Rust => {
            let msg = MockMessage { encoded: Cell::new(0), value: value.to_owned() };
            let mut cache = EncodingCache::new();
            let mut frames = Vec::new();
            for id in encoders {
                // The callback receives the message as `&dyn EncodedMessage`; like the
                // upstream type switch it only encodes the mock (identified here by
                // pointer, since Rust trait objects are not downcastable by default).
                let target = &msg as *const MockMessage as *const ();
                let mut callback = |m: &dyn EncodedMessage| -> Result<Option<SentFrame>, EncodeError> {
                    if std::ptr::eq(m as *const dyn EncodedMessage as *const (), target) {
                        msg.encoded.set(msg.encoded.get() + 1);
                        Ok(Some(SentFrame {
                            frame_type: FrameType::Text,
                            payload: msg.value.as_bytes().to_vec(),
                            ..SentFrame::default()
                        }))
                    } else {
                        Ok(None)
                    }
                };
                frames.push(cache.fetch(&msg, id, &mut callback));
            }
            MockFetches { frames, encoded: msg.encoded.get() }
        }
        Backend::Go => {
            #[derive(Deserialize)]
            #[serde(rename_all = "PascalCase")]
            struct Out {
                frames: Vec<GoParsed<GoSentFrame>>,
                encoded: i32,
            }
            let out: Out =
                bridge::call("encoders.EncodingCache.FetchMock", json!({"Value": value, "Encoders": encoders}));
            MockFetches { frames: out.frames.into_iter().map(frame_result).collect(), encoded: out.encoded }
        }
    }
}
