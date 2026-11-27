//! Converting Ruby values to Rust data (go-mruby `decode.go`).
//!
//! Go decodes by reflection into whatever `out` points to. Rust uses serde: the
//! target type's `Deserialize` impl drives the same conversions.
//!
//! | Go target | Ruby input | Rust target |
//! |---|---|---|
//! | `bool` | `true`/`false` | `bool` |
//! | `float64` | Float (narrowed through C `float`) | `f64` |
//! | `int` | Fixnum, or a String of digits | `i64` |
//! | `string` | String, or a Fixnum (decimal) | `String` |
//! | `map[string]T` | Hash (keys decoded as strings) | `HashMap<String, T>` / `BTreeMap` |
//! | `[]T` | Array | `Vec<T>` |
//! | `*T` | as `T` | `Box<T>` |
//! | struct | Hash (key per field) or object (method per field) | `#[derive(Deserialize)]` struct |
//! | `interface{}` | any of the above | [`Decoded`] via [`decode_value`] |
//!
//! Go matches struct fields by their lower-cased name or an `mruby:"name"` tag;
//! the Rust equivalent is the serde field name or `#[serde(rename = "name")]`.
//! Error texts follow Go's (`root.<entry 0>: unknown type to string: 4294967295`),
//! with the type printed as its go-mruby number.

use std::collections::BTreeMap;
use std::fmt;

use cabby_core::toml_config::go_quote;
use serde::de::{self, DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor};

use super::{MrbValue, Value, ValueType};

/// A dynamically typed decode result (Go: decoding into `interface{}`).
#[derive(Debug, Clone, PartialEq)]
pub enum Decoded {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Map(BTreeMap<String, Decoded>),
    Slice(Vec<Decoded>),
}

/// Why a value could not be decoded (Go returns `fmt.Errorf` values that name
/// the path, e.g. `root.<entry 0>: ...`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError {
    pub message: String,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DecodeError {}

impl de::Error for DecodeError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        DecodeError { message: msg.to_string() }
    }
}

fn err(message: String) -> DecodeError {
    DecodeError { message }
}

/// Decodes `v` into `T` (`Decode(&out, v)`).
pub fn decode<T: DeserializeOwned>(v: &MrbValue) -> Result<T, DecodeError> {
    T::deserialize(Decoder { v: v.clone(), name: "root".to_owned() })
}

/// Decodes `v` without a target type (`Decode(&iface, v)`).
pub fn decode_value(v: &MrbValue) -> Result<Decoded, DecodeError> {
    decode_interface("root", v)
}

fn type_number(v: &MrbValue) -> u32 {
    v.value_type().number()
}

/// go-mruby `decodeString`.
fn decode_string(name: &str, v: &MrbValue) -> Result<String, DecodeError> {
    match v.value_type() {
        ValueType::Fixnum => Ok(v.fixnum().to_string()),
        ValueType::String => Ok(v.to_string()),
        _ => Err(err(format!("{name}: unknown type to string: {}", type_number(v)))),
    }
}

/// go-mruby `decodeInt`.
fn decode_int(name: &str, v: &MrbValue) -> Result<i64, DecodeError> {
    match v.value_type() {
        ValueType::Fixnum => Ok(v.fixnum()),
        ValueType::String => parse_go_int(&v.to_string()).map_err(err),
        _ => Err(err(format!("{name}: unknown type {}", type_number(v)))),
    }
}

/// go-mruby `decodeBool`.
fn decode_bool(name: &str, v: &MrbValue) -> Result<bool, DecodeError> {
    match v.value_type() {
        ValueType::False => Ok(false),
        ValueType::True => Ok(true),
        _ => Err(err(format!("{name}: unknown type {}", type_number(v)))),
    }
}

/// go-mruby `decodeFloat`.
fn decode_float(name: &str, v: &MrbValue) -> Result<f64, DecodeError> {
    match v.value_type() {
        ValueType::Float => Ok(v.float()),
        _ => Err(err(format!("{name}: unknown type {}", type_number(v)))),
    }
}

/// go-mruby `decodeInterface`.
fn decode_interface(name: &str, v: &MrbValue) -> Result<Decoded, DecodeError> {
    match v.value_type() {
        ValueType::Hash => {
            let m = v.mrb();
            let ai = m.arena_save();
            let out = (|| {
                let hash = v.hash();
                let keys = hash.keys().map_err(|e| err(e.message))?.array();
                let mut out = BTreeMap::new();
                for i in 0..keys.len() {
                    let key = array_get(&keys, i)?;
                    let value = hash.get(&Value::Mrb(key.clone())).map_err(|e| err(e.message))?;
                    let field = format!("{name}.<entry {i}>");
                    out.insert(decode_string(&field, &key)?, decode_interface(&field, &value)?);
                }
                Ok(Decoded::Map(out))
            })();
            m.arena_restore(ai);
            out
        }
        ValueType::Array => {
            let array = v.array();
            let mut out = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                out.push(decode_interface(&format!("{name}[{i}]"), &array_get(&array, i)?)?);
            }
            Ok(Decoded::Slice(out))
        }
        ValueType::False | ValueType::True => decode_bool(name, v).map(Decoded::Bool),
        ValueType::Fixnum => decode_int(name, v).map(Decoded::Int),
        ValueType::Float => decode_float(name, v).map(Decoded::Float),
        ValueType::String => decode_string(name, v).map(Decoded::String),
        _ => Err(err(format!("{name}: cannot decode into interface: {}", type_number(v)))),
    }
}

/// `Array.Get(i)` with nil elements as a nil value (Go keeps the nil
/// `*MrbValue` and fails on it later; a nil value fails the same checks).
fn array_get(array: &super::Array, i: usize) -> Result<MrbValue, DecodeError> {
    let idx = i64::try_from(i).unwrap_or(i64::MAX);
    match array.get(idx).map_err(|e| err(e.message))? {
        Some(v) => Ok(v),
        None => Ok(array.value().mrb().nil_value()),
    }
}

/// Go `strconv.ParseInt(s, 0, 0)` on a 64-bit platform, with its error texts.
fn parse_go_int(s: &str) -> Result<i64, String> {
    let fail = |what: &str| format!("strconv.ParseInt: parsing {}: {what}", go_quote(s));
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let lower = body.to_ascii_lowercase();
    let (base, digits, prefixed) = if lower.starts_with("0x") {
        (16, &body[2..], true)
    } else if lower.starts_with("0o") {
        (8, &body[2..], true)
    } else if lower.starts_with("0b") {
        (2, &body[2..], true)
    } else if body.len() > 1 && body.starts_with('0') {
        (8, &body[1..], true)
    } else {
        (10, body, false)
    };
    if digits.is_empty() || !underscores_ok(body, prefixed) {
        return Err(fail("invalid syntax"));
    }
    let mut n: u64 = 0;
    let mut overflow = false;
    for c in digits.chars().filter(|&c| c != '_') {
        let d = c.to_digit(base).ok_or_else(|| fail("invalid syntax"))?;
        match n.checked_mul(u64::from(base)).and_then(|n| n.checked_add(u64::from(d))) {
            Some(v) => n = v,
            None => overflow = true,
        }
    }
    let limit = if neg { 1u64 << 63 } else { (1u64 << 63) - 1 };
    if overflow || n > limit {
        return Err(fail("value out of range"));
    }
    if neg { Ok(0i64.wrapping_sub_unsigned(n)) } else { Ok(i64::try_from(n).unwrap_or(i64::MAX)) }
}

/// Go `strconv.underscoreOK`: underscores only between digits (or after a
/// base prefix), and only with a base prefix.
fn underscores_ok(s: &str, prefixed: bool) -> bool {
    if !s.contains('_') {
        return true;
    }
    if !prefixed {
        return false;
    }
    let bytes = s.as_bytes();
    let mut saw = b'^';
    let mut i = 0;
    if bytes.len() >= 2 && bytes[0] == b'0' && bytes[1].is_ascii_alphabetic() {
        i = 2;
        saw = b'0';
    }
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_alphanumeric() {
            saw = b'0';
        } else if c == b'_' {
            if saw != b'0' {
                return false;
            }
            saw = b'_';
        } else {
            if saw == b'_' {
                return false;
            }
            saw = b'!';
        }
        i += 1;
    }
    saw != b'_'
}

/// A serde deserializer over one Ruby value; `name` is the Go-style path used
/// in error messages.
struct Decoder {
    v: MrbValue,
    name: String,
}

impl Decoder {
    fn unknown_kind(&self, kind: &str) -> DecodeError {
        err(format!("{}: unknown kind to decode into: {kind}", self.name))
    }
}

macro_rules! ints {
    ($($method:ident),*) => {
        $(
            fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
                visitor.visit_i64(decode_int(&self.name, &self.v)?)
            }
        )*
    };
}

impl<'de> de::Deserializer<'de> for Decoder {
    type Error = DecodeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        match self.v.value_type() {
            ValueType::Hash => self.deserialize_map(visitor),
            ValueType::Array => self.deserialize_seq(visitor),
            ValueType::False | ValueType::True => self.deserialize_bool(visitor),
            ValueType::Fixnum => self.deserialize_i64(visitor),
            ValueType::Float => self.deserialize_f64(visitor),
            ValueType::String => self.deserialize_string(visitor),
            ValueType::Nil => visitor.visit_unit(),
            _ => Err(err(format!("{}: cannot decode into interface: {}", self.name, type_number(&self.v)))),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        visitor.visit_bool(decode_bool(&self.name, &self.v)?)
    }

    ints!(
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64
    );

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_f64(visitor)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        visitor.visit_f64(decode_float(&self.name, &self.v)?)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_string(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        visitor.visit_string(decode_string(&self.name, &self.v)?)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, DecodeError> {
        Err(self.unknown_kind("bytes"))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, DecodeError> {
        Err(self.unknown_kind("bytes"))
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        if self.v.value_type() == ValueType::Nil { visitor.visit_none() } else { visitor.visit_some(self) }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, DecodeError> {
        Err(self.unknown_kind("unit"))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, DecodeError> {
        Err(self.unknown_kind("unit"))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        visitor.visit_newtype_struct(self)
    }

    /// go-mruby `decodeSlice` (which reads any value as an array; Cabby
    /// requires an Array).
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        if self.v.value_type() != ValueType::Array {
            return Err(err(format!("{}: not an array type for slice ({})", self.name, type_number(&self.v))));
        }
        let array = self.v.array();
        visitor.visit_seq(Seq { len: array.len(), array, next: 0, name: self.name })
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.deserialize_seq(visitor)
    }

    /// go-mruby `decodeMap`.
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        if self.v.value_type() != ValueType::Hash {
            return Err(err(format!("{}: not a hash type for map ({})", self.name, type_number(&self.v))));
        }
        let m = self.v.mrb();
        let ai = m.arena_save();
        let out = (|| {
            let hash = self.v.hash();
            let keys = hash.keys().map_err(|e| err(e.message))?.array();
            visitor.visit_map(Entries { hash, len: keys.len(), keys, next: 0, value: None, name: self.name })
        })();
        m.arena_restore(ai);
        out
    }

    /// go-mruby `decodeStruct`: a Hash is read by string key, an object by
    /// calling the method named like the field.
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        let source = match self.v.value_type() {
            ValueType::Hash => Source::Hash(self.v.hash()),
            ValueType::Object => Source::Object(self.v.clone()),
            _ => return Err(err(format!("{}: not an object type for struct ({})", self.name, type_number(&self.v)))),
        };
        let m = self.v.mrb();
        let ai = m.arena_save();
        let out = visitor.visit_map(Fields { source, fields, next: 0, name: self.name });
        m.arena_restore(ai);
        out
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, DecodeError> {
        Err(self.unknown_kind("enum"))
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.deserialize_string(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        visitor.visit_unit()
    }
}

struct Seq {
    array: super::Array,
    len: usize,
    next: usize,
    name: String,
}

impl<'de> SeqAccess<'de> for Seq {
    type Error = DecodeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>, DecodeError> {
        if self.next >= self.len {
            return Ok(None);
        }
        let i = self.next;
        self.next += 1;
        let v = array_get(&self.array, i)?;
        seed.deserialize(Decoder { v, name: format!("{}[{i}]", self.name) }).map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len - self.next)
    }
}

struct Entries {
    hash: super::Hash,
    keys: super::Array,
    len: usize,
    next: usize,
    /// The value of the key last returned, with its path.
    value: Option<(MrbValue, String)>,
    name: String,
}

impl<'de> MapAccess<'de> for Entries {
    type Error = DecodeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, DecodeError> {
        if self.next >= self.len {
            return Ok(None);
        }
        let i = self.next;
        self.next += 1;
        let key = array_get(&self.keys, i)?;
        let value = self.hash.get(&Value::Mrb(key.clone())).map_err(|e| err(e.message))?;
        let field = format!("{}.<entry {i}>", self.name);
        self.value = Some((value, field.clone()));
        seed.deserialize(Decoder { v: key, name: field }).map(Some)
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, DecodeError> {
        let (v, name) = self.value.take().ok_or_else(|| err(format!("{}: value without key", self.name)))?;
        seed.deserialize(Decoder { v, name })
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len - self.next)
    }
}

enum Source {
    Hash(super::Hash),
    Object(MrbValue),
}

struct Fields {
    source: Source,
    fields: &'static [&'static str],
    next: usize,
    name: String,
}

impl<'de> MapAccess<'de> for Fields {
    type Error = DecodeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, DecodeError> {
        let Some(field) = self.fields.get(self.next) else {
            return Ok(None);
        };
        seed.deserialize((*field).into_deserializer()).map(Some)
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, DecodeError> {
        let field = self.fields[self.next];
        self.next += 1;
        let m = match &self.source {
            Source::Hash(h) => h.value().mrb(),
            Source::Object(o) => o.mrb(),
        };
        let ai = m.arena_save();
        let value = match &self.source {
            Source::Hash(h) => h.get(&Value::String(field.to_owned())),
            Source::Object(o) => o.call(field, &[]),
        };
        let out = value
            .map_err(|e| err(e.message))
            .and_then(|v| seed.deserialize(Decoder { v, name: format!("{}.{field}", self.name) }));
        m.arena_restore(ai);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Error texts quote the input as Go's `strconv.Quote` does (the shared
    /// `go_quote`): `\a`, `\v` and characters Go does not print get Go's escapes.
    /// Before, this module had its own quoting, which wrote U+0007 as `\x07` and
    /// U+0085 as `\x85` (Go: `\a`, `\u0085`).
    #[test]
    fn parse_errors_quote_the_input_like_strconv_quote() {
        for (input, quoted) in
            [("12x", r#""12x""#), ("\u{7}", r#""\a""#), ("1\u{b}", r#""1\v""#), ("\u{85}", r#""\u0085""#)]
        {
            assert_eq!(
                Err(format!("strconv.ParseInt: parsing {quoted}: invalid syntax")),
                parse_go_int(input),
                "{input:?}"
            );
        }
    }
}
