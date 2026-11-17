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

use std::collections::BTreeMap;
use std::fmt;

use cabby_core::pending;
use serde::de::DeserializeOwned;

use super::MrbValue;

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
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("mrb::DecodeError::fmt")
    }
}

impl std::error::Error for DecodeError {}

/// Decodes `v` into `T` (`Decode(&out, v)`).
pub fn decode<T: DeserializeOwned>(_v: &MrbValue) -> Result<T, DecodeError> {
    pending!("mrb::decode")
}

/// Decodes `v` without a target type (`Decode(&iface, v)`).
pub fn decode_value(_v: &MrbValue) -> Result<Decoded, DecodeError> {
    pending!("mrb::decode_value")
}
