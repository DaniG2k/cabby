//! Log value helpers and logging configuration (upstream package `logger`).
//!
//! Upstream builds on Go's `log/slog`: values implement `slog.LogValuer` and are
//! rendered by slog handlers. Cabby models the resolved value tree ([`LogValue`])
//! and the two handler formats used upstream ([`format_text`], [`format_json`]).
//! `Display` of a [`LogValue`] corresponds to Go's `slog.Value.String()`.

use std::collections::BTreeMap;
use std::fmt;

use crate::pending;

/// Values longer than this are truncated by the compact wrappers (upstream `maxValueLength`).
pub const MAX_VALUE_LENGTH: usize = 100;

/// Environment variable that disables sensitive data filtering when set to a false value.
pub const FILTER_LOGS_ENV: &str = "ANYCABLE_FILTER_LOGS";

/// A resolved log value (Go `slog.Value` after resolving `LogValuer`s).
#[derive(Debug, Clone, PartialEq)]
pub enum LogValue {
    String(String),
    Bool(bool),
    Int(i64),
    Uint(u64),
    /// A list of values (Go slices passed to `slog.Any`).
    List(Vec<LogValue>),
    /// A string-keyed map (Go maps passed to `slog.Any`).
    Map(BTreeMap<String, LogValue>),
    /// A nested group of attributes (Go `slog.GroupValue`).
    Group(Vec<Attr>),
}

impl fmt::Display for LogValue {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("logger::log_value_string")
    }
}

/// A key/value log attribute (Go `slog.Attr`).
#[derive(Debug, Clone, PartialEq)]
pub struct Attr {
    pub key: String,
    pub value: LogValue,
}

impl Attr {
    pub fn new(key: &str, value: LogValue) -> Self {
        Attr { key: key.to_owned(), value }
    }
}

impl fmt::Display for Attr {
    /// Go `slog.Attr.String()`: `key=value`.
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        pending!("logger::attr_string")
    }
}

/// Something that renders itself as a log value (Go `slog.LogValuer`).
pub trait LogValuer {
    fn log_value(&self) -> LogValue;
}

/// Log record level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

/// Renders a record the way upstream's text handler does (`slog.NewTextHandler`).
pub fn format_text(_level: Level, _msg: &str, _attrs: &[Attr]) -> String {
    pending!("logger::format_text")
}

/// Renders a record the way upstream's JSON handler does (`slog.NewJSONHandler`).
pub fn format_json(_level: Level, _msg: &str, _attrs: &[Attr]) -> String {
    pending!("logger::format_json")
}

/// A string or byte value shown truncated in logs (upstream `compactValue`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactValue {
    _val: Vec<u8>,
}

impl LogValuer for CompactValue {
    fn log_value(&self) -> LogValue {
        pending!("logger::compact_value")
    }
}

/// Upstream `CompactValue` for a string.
pub fn compact_value(v: &str) -> CompactValue {
    CompactValue { _val: v.as_bytes().to_vec() }
}

/// Upstream `CompactValue` for a byte slice.
pub fn compact_value_bytes(v: &[u8]) -> CompactValue {
    CompactValue { _val: v.to_vec() }
}

/// Upstream `CompactValues` for strings.
pub fn compact_values(_v: &[&str]) -> Vec<CompactValue> {
    pending!("logger::compact_values")
}

/// Upstream `CompactValues` for byte slices.
pub fn compact_values_bytes(_v: &[&[u8]]) -> Vec<CompactValue> {
    pending!("logger::compact_values")
}

/// A dynamically typed value as upstream passes to `CompactAny` (Go `interface{}`):
/// decoded JSON, or a struct printed with Go's `%+v` verb.
#[derive(Debug, Clone, PartialEq)]
pub enum AnyValue {
    Json(serde_json::Value),
    /// A struct with named fields, in declaration order.
    Struct(Vec<(String, AnyValue)>),
}

/// Any value shown truncated in logs (upstream `compactAny`).
#[derive(Debug, Clone, PartialEq)]
pub struct CompactAny {
    _val: AnyValue,
}

impl LogValuer for CompactAny {
    fn log_value(&self) -> LogValue {
        pending!("logger::compact_any")
    }
}

/// Upstream `CompactAny`.
pub fn compact_any(val: AnyValue) -> CompactAny {
    CompactAny { _val: val }
}

/// Upstream `filteringEnabledFromEnv`: parses the `ANYCABLE_FILTER_LOGS` value
/// (anything unparsable keeps filtering enabled).
pub fn filtering_enabled_from_env(_val: &str) -> bool {
    pending!("logger::filtering_enabled_from_env")
}

/// Sensitive data filtering policy. Upstream keeps it in a package global read from
/// `ANYCABLE_FILTER_LOGS` at start; Cabby passes it explicitly so tests do not mutate
/// process state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogFilter {
    pub enabled: bool,
}

impl LogFilter {
    /// The process-wide policy (upstream `FilteringEnabled()`).
    pub fn global() -> LogFilter {
        pending!("logger::filtering_enabled")
    }

    /// Upstream `FilteredURL` under this policy.
    pub fn filtered_url(&self, val: &str) -> FilteredUrl {
        FilteredUrl { _val: val.to_owned(), _filter: *self }
    }

    /// Upstream `MaskValue` under this policy.
    pub fn mask_value(&self, _val: &str) -> String {
        pending!("logger::mask_value")
    }

    /// Upstream `FilteredMap` under this policy (`None`: a nil map).
    pub fn filtered_map(&self, val: Option<&BTreeMap<String, String>>) -> FilteredMap {
        FilteredMap { _val: val.cloned(), _filter: *self }
    }
}

/// A URL shown with masked query values and user info (upstream `filteredURL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredUrl {
    _val: String,
    _filter: LogFilter,
}

impl LogValuer for FilteredUrl {
    fn log_value(&self) -> LogValue {
        pending!("logger::filtered_url")
    }
}

/// A map shown by its keys only (upstream `filteredMap`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredMap {
    _val: Option<BTreeMap<String, String>>,
    _filter: LogFilter,
}

impl LogValuer for FilteredMap {
    fn log_value(&self) -> LogValue {
        pending!("logger::filtered_map")
    }
}

/// Upstream `FilteredURL` (process-wide policy).
pub fn filtered_url(val: &str) -> FilteredUrl {
    LogFilter::global().filtered_url(val)
}

/// Upstream `MaskValue` (process-wide policy).
pub fn mask_value(val: &str) -> String {
    LogFilter::global().mask_value(val)
}

/// Upstream `FilteredMap` (process-wide policy).
pub fn filtered_map(val: Option<&BTreeMap<String, String>>) -> FilteredMap {
    LogFilter::global().filtered_map(val)
}

/// Logging settings (upstream `logger.Config`, TOML table `[logging]`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Config {
    /// `level`
    pub log_level: String,
    /// `format`
    pub log_format: String,
    /// `debug`
    pub debug: bool,
}

/// A TOML decoding failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TomlError(pub String);

impl Config {
    /// Defaults (upstream `logger.NewConfig`).
    pub fn new() -> Self {
        pending!("logger::new_config")
    }

    /// Renders the annotated TOML section (upstream `Config.ToToml`).
    pub fn to_toml(&self) -> String {
        pending!("logger::config_to_toml")
    }

    /// Decodes TOML over the current values (upstream `toml.Decode(s, &conf)`).
    pub fn decode_toml(&mut self, _toml: &str) -> Result<(), TomlError> {
        pending!("logger::config_decode_toml")
    }
}
