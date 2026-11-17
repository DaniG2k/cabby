//! Go `encoding/json` shapes of the `common` types.
//!
//! `Serialize` impls follow the upstream struct tags (field order, `omitempty`);
//! write them with [`crate::gojson`] for byte-exact Go output. Decoders follow Go's
//! `json.Unmarshal` into the upstream structs: case-insensitive field names, unknown
//! fields ignored, `null` a no-op for scalars and structs (and nil for pointers,
//! maps and interfaces), and `interface{}` numbers as `float64`.

use serde::de::{self, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use super::{
    DecodeError, DisconnectMessage, EncodedMessage, HistoryPosition, HistoryRequest, Message, MessageData, PingMessage,
    PresenceEvent, PresenceInfo, PubSubMessage, RemoteCommandMessage, RemoteDisconnectMessage, Reply, StreamMessage,
    StreamMessageMetadata,
};
use crate::gojson::{self, FieldKey};

// --- Encoding -----------------------------------------------------------------------

/// Serializes a field, or skips it when `present` is false (Go `omitempty`).
fn field<S: SerializeStruct, T: Serialize + ?Sized>(
    st: &mut S,
    present: bool,
    name: &'static str,
    value: &T,
) -> Result<(), S::Error> {
    if present { st.serialize_field(name, value) } else { st.skip_field(name) }
}

fn non_nil(v: &Option<Value>) -> bool {
    v.as_ref().is_some_and(|v| !v.is_null())
}

impl Serialize for Reply {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("Reply", 12)?;
        field(&mut st, !self.kind.is_empty(), "type", &self.kind)?;
        field(&mut st, !self.identifier.is_empty(), "identifier", &self.identifier)?;
        field(&mut st, non_nil(&self.message), "message", &self.message)?;
        field(&mut st, self.presence.is_some(), "presence", &self.presence)?;
        field(&mut st, !self.reason.is_empty(), "reason", &self.reason)?;
        field(&mut st, self.reconnect, "reconnect", &self.reconnect)?;
        field(&mut st, !self.stream_id.is_empty(), "stream_id", &self.stream_id)?;
        field(&mut st, !self.epoch.is_empty(), "epoch", &self.epoch)?;
        field(&mut st, self.offset != 0, "offset", &self.offset)?;
        field(&mut st, !self.sid.is_empty(), "sid", &self.sid)?;
        field(&mut st, self.restored, "restored", &self.restored)?;
        field(&mut st, !self.restored_ids.is_empty(), "restored_ids", &self.restored_ids)?;
        st.end()
    }
}

impl Serialize for PresenceEvent {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("PresenceEvent", 3)?;
        field(&mut st, !self.kind.is_empty(), "type", &self.kind)?;
        field(&mut st, non_nil(&self.info), "info", &self.info)?;
        st.serialize_field("id", &self.id)?;
        st.end()
    }
}

impl Serialize for PresenceInfo {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("PresenceInfo", 3)?;
        field(&mut st, !self.kind.is_empty(), "type", &self.kind)?;
        st.serialize_field("total", &self.total)?;
        field(&mut st, !self.records.is_empty(), "records", &self.records)?;
        st.end()
    }
}

impl Serialize for PingMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("PingMessage", 2)?;
        st.serialize_field("type", &self.kind)?;
        field(&mut st, non_nil(&self.message), "message", &self.message)?;
        st.end()
    }
}

impl Serialize for DisconnectMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("DisconnectMessage", 3)?;
        st.serialize_field("type", &self.kind)?;
        st.serialize_field("reason", &self.reason)?;
        st.serialize_field("reconnect", &self.reconnect)?;
        st.end()
    }
}

impl Serialize for EncodedMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            EncodedMessage::Reply(m) => m.serialize(s),
            EncodedMessage::Ping(m) => m.serialize(s),
            EncodedMessage::Disconnect(m) => m.serialize(s),
        }
    }
}

impl EncodedMessage {
    /// The Go JSON encoding (upstream `encoders.JSON.Encode` payload).
    pub fn to_json(&self) -> Vec<u8> {
        gojson::to_vec(self)
    }
}

impl Serialize for StreamMessageMetadata {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("StreamMessageMetadata", 3)?;
        field(&mut st, !self.exclude_socket.is_empty(), "exclude_socket", &self.exclude_socket)?;
        field(&mut st, !self.broadcast_type.is_empty(), "broadcast_type", &self.broadcast_type)?;
        field(&mut st, self.transient, "transient", &self.transient)?;
        st.end()
    }
}

impl Serialize for StreamMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("StreamMessage", 5)?;
        st.serialize_field("stream", &self.stream)?;
        st.serialize_field("data", &self.data)?;
        field(&mut st, self.meta.is_some(), "meta", &self.meta)?;
        field(&mut st, self.offset != 0, "offset", &self.offset)?;
        field(&mut st, !self.epoch.is_empty(), "epoch", &self.epoch)?;
        st.end()
    }
}

impl Serialize for RemoteCommandMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("RemoteCommandMessage", 2)?;
        field(&mut st, !self.command.is_empty(), "command", &self.command)?;
        if self.payload.is_empty() {
            st.skip_field("payload")?;
        } else {
            // Go writes a `json.RawMessage` as is (compacted); invalid JSON is an error.
            let raw = RawValue::from_string(self.payload.clone()).map_err(serde::ser::Error::custom)?;
            st.serialize_field("payload", &raw)?;
        }
        st.end()
    }
}

impl Serialize for RemoteDisconnectMessage {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("RemoteDisconnectMessage", 2)?;
        st.serialize_field("identifier", &self.identifier)?;
        st.serialize_field("reconnect", &self.reconnect)?;
        st.end()
    }
}

/// `utils.ToJSON(Reply{Identifier: identifier, Type: kind})` without building a Reply.
pub(super) fn typed_identifier_message(kind: &str, identifier: &str) -> String {
    #[derive(Serialize)]
    struct TypedIdentifier<'a> {
        #[serde(rename = "type", skip_serializing_if = "str::is_empty")]
        kind: &'a str,
        #[serde(skip_serializing_if = "str::is_empty")]
        identifier: &'a str,
    }
    gojson::to_string(&TypedIdentifier { kind, identifier })
}

// --- Decoding: helpers --------------------------------------------------------------

fn decode_error(e: serde_json::Error) -> DecodeError {
    DecodeError(e.to_string())
}

/// Runs `seed` over the whole input; trailing data is an error (as in Go).
fn decode_with<'de, S: DeserializeSeed<'de>>(raw: &'de [u8], seed: S) -> Result<S::Value, DecodeError> {
    let mut de = serde_json::Deserializer::from_slice(raw);
    let value = seed.deserialize(&mut de).map_err(decode_error)?;
    de.end().map_err(decode_error)?;
    Ok(value)
}

/// Decodes a Go struct value in place: `null` leaves it untouched, an object merges
/// its known fields into it (Go reuses the existing value).
trait GoStruct: Sized {
    const FIELDS: &'static [&'static str];

    /// Decodes the value of field `idx` (an index into [`Self::FIELDS`]).
    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error>;
}

/// Seed decoding into an existing `T` (Go struct field or decode target).
struct InPlace<'a, T>(&'a mut T);

impl<'de, T: GoStruct> DeserializeSeed<'de> for InPlace<'_, T> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_option(self)
    }
}

impl<'de, T: GoStruct> Visitor<'de> for InPlace<'_, T> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an object")
    }

    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(self)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(idx) = map.next_key_seed(FieldKey(T::FIELDS))? {
            match idx {
                Some(idx) => self.0.field(idx, &mut map)?,
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}

/// Seed for a Go pointer-to-struct field: `null` sets nil, an object decodes into
/// the existing pointee (or a new zero value).
struct IntoPtr<'a, T>(&'a mut Option<T>);

impl<'de, T: GoStruct + Default> DeserializeSeed<'de> for IntoPtr<'_, T> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_option(self)
    }
}

impl<'de, T: GoStruct + Default> Visitor<'de> for IntoPtr<'_, T> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an object or null")
    }

    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        *self.0 = None;
        Ok(())
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.visit_none()
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(InPlace(self.0.get_or_insert_with(T::default)))
    }
}

/// A Go `string` / `bool` / number field: `null` keeps the current value.
fn scalar<'de, A: MapAccess<'de>, T: Deserialize<'de>>(map: &mut A, slot: &mut T) -> Result<(), A::Error> {
    if let Some(v) = map.next_value::<Option<T>>()? {
        *slot = v;
    }
    Ok(())
}

/// A Go `interface{}` field: `null` is nil, numbers are `float64`.
fn interface<'de, A: MapAccess<'de>>(map: &mut A) -> Result<Option<Value>, A::Error> {
    let mut v: Value = map.next_value()?;
    if v.is_null() {
        return Ok(None);
    }
    gojson::to_go_interface(&mut v).map_err(de::Error::custom)?;
    Ok(Some(v))
}

// --- Decoding: client messages ----------------------------------------------------------

impl GoStruct for Message {
    const FIELDS: &'static [&'static str] = &["command", "identifier", "data", "history", "presence"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.command),
            1 => scalar(map, &mut self.identifier),
            2 => {
                self.data = interface(map)?.map(MessageData::Json);
                Ok(())
            }
            3 => map.next_value_seed(InPlace(&mut self.history)),
            _ => map.next_value_seed(IntoPtr(&mut self.presence)),
        }
    }
}

impl GoStruct for HistoryRequest {
    const FIELDS: &'static [&'static str] = &["since", "streams"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.since),
            _ => map.next_value_seed(HistoryStreams(&mut self.streams)),
        }
    }
}

/// `map[string]HistoryPosition`: `null` clears it, an object adds entries (each
/// decoded into a fresh value).
struct HistoryStreams<'a>(&'a mut std::collections::HashMap<String, HistoryPosition>);

impl<'de> DeserializeSeed<'de> for HistoryStreams<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_option(self)
    }
}

impl<'de> Visitor<'de> for HistoryStreams<'_> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an object or null")
    }

    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        self.0.clear();
        Ok(())
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.visit_none()
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_map(self)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            let mut pos = HistoryPosition::default();
            map.next_value_seed(InPlace(&mut pos))?;
            self.0.insert(key, pos);
        }
        Ok(())
    }
}

impl GoStruct for HistoryPosition {
    const FIELDS: &'static [&'static str] = &["epoch", "offset"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.epoch),
            _ => scalar(map, &mut self.offset),
        }
    }
}

impl GoStruct for PresenceEvent {
    const FIELDS: &'static [&'static str] = &["type", "info", "id"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.kind),
            1 => {
                self.info = interface(map)?;
                Ok(())
            }
            _ => scalar(map, &mut self.id),
        }
    }
}

pub(super) fn decode_message(raw: &[u8]) -> Result<Message, DecodeError> {
    let mut msg = Message::default();
    decode_with(raw, InPlace(&mut msg))?;
    Ok(msg)
}

// --- Decoding: pub/sub messages -------------------------------------------------------

impl GoStruct for StreamMessage {
    const FIELDS: &'static [&'static str] = &["stream", "data", "meta", "offset", "epoch"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.stream),
            1 => scalar(map, &mut self.data),
            2 => map.next_value_seed(IntoPtr(&mut self.meta)),
            3 => scalar(map, &mut self.offset),
            _ => scalar(map, &mut self.epoch),
        }
    }
}

impl GoStruct for StreamMessageMetadata {
    const FIELDS: &'static [&'static str] = &["exclude_socket", "broadcast_type", "transient"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.exclude_socket),
            1 => scalar(map, &mut self.broadcast_type),
            _ => scalar(map, &mut self.transient),
        }
    }
}

impl GoStruct for RemoteCommandMessage {
    const FIELDS: &'static [&'static str] = &["command", "payload"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.command),
            _ => {
                // `json.RawMessage` keeps the raw text, `null` included.
                let raw: &RawValue = map.next_value()?;
                self.payload = raw.get().to_owned();
                Ok(())
            }
        }
    }
}

impl GoStruct for RemoteDisconnectMessage {
    const FIELDS: &'static [&'static str] = &["identifier", "reconnect"];

    fn field<'de, A: MapAccess<'de>>(&mut self, idx: usize, map: &mut A) -> Result<(), A::Error> {
        match idx {
            0 => scalar(map, &mut self.identifier),
            _ => scalar(map, &mut self.reconnect),
        }
    }
}

/// `[]*StreamMessage` (a JSON `null` element decodes as an empty message).
struct Batch;

impl<'de> DeserializeSeed<'de> for Batch {
    type Value = Vec<StreamMessage>;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for Batch {
    type Value = Vec<StreamMessage>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("an array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut batch = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        loop {
            let mut msg = StreamMessage::default();
            if seq.next_element_seed(InPlace(&mut msg))?.is_none() {
                return Ok(batch);
            }
            batch.push(msg);
        }
    }
}

fn first_byte(raw: &[u8]) -> Option<u8> {
    raw.iter().copied().find(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
}

pub(super) fn decode_pub_sub_message(raw: &[u8]) -> Result<PubSubMessage, DecodeError> {
    // Upstream tries a stream message, a batch, then a remote command, and returns the
    // last error. Only an object can be a stream message (or a remote command) and only
    // an array a batch, so the input is parsed at most twice.
    match first_byte(raw) {
        Some(b'{') => {
            let mut msg = StreamMessage::default();
            if decode_with(raw, InPlace(&mut msg)).is_ok() && !msg.stream.is_empty() {
                return Ok(PubSubMessage::Stream(msg));
            }
        }
        Some(b'[') => {
            if let Ok(batch) = decode_with(raw, Batch)
                && batch.first().is_some_and(|m| !m.stream.is_empty())
            {
                return Ok(PubSubMessage::Batch(batch));
            }
            return Err(DecodeError(
                "json: cannot unmarshal array into Go value of type common.RemoteCommandMessage".to_owned(),
            ));
        }
        _ => {}
    }

    let mut cmd = RemoteCommandMessage::default();
    decode_with(raw, InPlace(&mut cmd))?;
    if cmd.command.is_empty() {
        return Err(DecodeError("invalid publication format".to_owned()));
    }
    Ok(PubSubMessage::RemoteCommand(cmd))
}

pub(super) fn decode_remote_disconnect(raw: &[u8]) -> Result<RemoteDisconnectMessage, DecodeError> {
    if first_byte(raw).is_none() {
        return Err(DecodeError("unexpected end of JSON input".to_owned()));
    }
    let mut msg = RemoteDisconnectMessage::default();
    decode_with(raw, InPlace(&mut msg))?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reply_omits_empty_fields() {
        let reply = Reply { kind: "t".into(), message: Some(Value::Null), offset: 0, ..Reply::default() };
        assert_eq!(r#"{"type":"t"}"#, gojson::to_string(&reply));
        let reply = Reply {
            identifier: "<id>".into(),
            message: Some(json!({"b": 1, "a": "&"})),
            presence: Some(PresenceEvent { kind: "join".into(), info: None, id: "1".into() }),
            reconnect: true,
            stream_id: "s".into(),
            epoch: "e".into(),
            offset: 3,
            sid: "x".into(),
            restored: true,
            restored_ids: vec!["a".into()],
            ..Reply::default()
        };
        assert_eq!(
            r#"{"identifier":"\u003cid\u003e","message":{"a":"\u0026","b":1},"presence":{"type":"join","id":"1"},"reconnect":true,"stream_id":"s","epoch":"e","offset":3,"sid":"x","restored":true,"restored_ids":["a"]}"#,
            gojson::to_string(&reply)
        );
    }

    #[test]
    fn decodes_messages_like_go() {
        let msg = decode_message(br#"{"COMMAND":"message","identifier":null,"data":{"n":1.50},"extra":[1]}"#).unwrap();
        assert_eq!("message", msg.command);
        assert_eq!(Some(MessageData::Json(json!({"n": 1.5}))), msg.data);
        assert!(decode_message(br#"{"command":1}"#).is_err());
        assert!(decode_message(br#"[]"#).is_err());
        assert!(decode_message(br#"{"command":"x"} x"#).is_err());
        assert_eq!(Message::default(), decode_message(b"null").unwrap());
        let msg =
            decode_message(br#"{"presence":{"id":"1","info":"x"},"history":{"streams":{"a":{"offset":2}}}}"#).unwrap();
        assert_eq!(Some(json!("x")), msg.presence.unwrap().info);
        assert_eq!(2, msg.history.streams["a"].offset);
    }

    #[test]
    fn decodes_pub_sub_messages_like_go() {
        let batch = decode_pub_sub_message(br#"[{"stream":"a","data":"1"},{"stream":"b","data":"2","offset":3}]"#);
        assert!(matches!(batch, Ok(PubSubMessage::Batch(b)) if b.len() == 2 && b[1].offset == 3));
        assert!(decode_pub_sub_message(b"[]").is_err());
        assert_eq!(
            Err(DecodeError("invalid publication format".into())),
            decode_pub_sub_message(br#"{"stream":"","data":"x"}"#)
        );
        let Ok(PubSubMessage::RemoteCommand(cmd)) =
            decode_pub_sub_message(br#"{"command":"disconnect","payload": {"identifier" : "1"}}"#)
        else {
            panic!("remote command")
        };
        assert_eq!(r#"{"identifier" : "1"}"#, cmd.payload);
        assert_eq!(
            RemoteDisconnectMessage { identifier: "1".into(), reconnect: false },
            cmd.to_remote_disconnect_message().unwrap()
        );
        let meta = decode_pub_sub_message(br#"{"stream":"s","data":"d","meta":{"transient":true}}"#);
        assert!(matches!(meta, Ok(PubSubMessage::Stream(m)) if m.meta.as_ref().is_some_and(|m| m.transient)));
    }

    fn reply_json(data: &str) -> String {
        let sm = StreamMessage { stream: "s".into(), data: data.into(), ..StreamMessage::default() };
        gojson::to_string(&sm.to_reply_for("id"))
    }

    /// Go re-encodes stream data decoded with `UseNumber`: keys sorted, number text kept.
    #[test]
    fn stream_data_reencoding() {
        assert_eq!(
            r#"{"identifier":"id","message":{"a":[1.50,-0],"b":null}}"#,
            reply_json(r#"{"b":null, "a":[1.50, -0]}"#)
        );
        assert_eq!(r#"{"identifier":"id","message":"null"}"#, reply_json("null"));
        assert_eq!(r#"{"identifier":"id","message":"123abc"}"#, reply_json("123abc"));
    }

    /// Proposed deviation D-010: serde_json normalizes the exponent marker of number
    /// text (`1E2` and `1e2` become `1e+2`); Go writes `1E2` / `1e2` back verbatim.
    #[test]
    fn deviation_d006_exponent_text_is_normalized() {
        assert_eq!(r#"{"identifier":"id","message":[1e+2,1e+2,1e-2]}"#, reply_json("[1E2,1e2,1e-2]"));
    }

    /// Proposed deviation D-011: data Go would decode is sent as a string when it nests
    /// deeper than 128 levels or contains a lone UTF-16 surrogate escape.
    #[test]
    fn deviation_d007_undecodable_data_is_sent_as_string() {
        let deep = format!("{}{}", "[".repeat(129), "]".repeat(129));
        assert_eq!(format!(r#"{{"identifier":"id","message":"{deep}"}}"#), reply_json(&deep));
        assert_eq!(r#"{"identifier":"id","message":"\"\\ud800\""}"#, reply_json(r#""\ud800""#));
    }
}
