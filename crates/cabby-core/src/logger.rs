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
    /// Go `slog.Value.String()`: groups print as `[k=v ...]`, lists (Go slices via
    /// `%+v`) as `[a b]` and maps as `map[k:v ...]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn join<T>(
            f: &mut fmt::Formatter<'_>,
            items: impl IntoIterator<Item = T>,
            mut each: impl FnMut(&mut fmt::Formatter<'_>, T) -> fmt::Result,
        ) -> fmt::Result {
            for (i, item) in items.into_iter().enumerate() {
                if i > 0 {
                    f.write_str(" ")?;
                }
                each(f, item)?;
            }
            Ok(())
        }

        match self {
            LogValue::String(s) => f.write_str(s),
            LogValue::Bool(b) => write!(f, "{b}"),
            LogValue::Int(n) => write!(f, "{n}"),
            LogValue::Uint(n) => write!(f, "{n}"),
            LogValue::List(items) => {
                f.write_str("[")?;
                join(f, items, |f, v| write!(f, "{v}"))?;
                f.write_str("]")
            }
            LogValue::Map(map) => {
                f.write_str("map[")?;
                join(f, map, |f, (k, v)| write!(f, "{k}:{v}"))?;
                f.write_str("]")
            }
            LogValue::Group(attrs) => {
                f.write_str("[")?;
                join(f, attrs, |f, a| write!(f, "{a}"))?;
                f.write_str("]")
            }
        }
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.key, self.value)
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

impl Level {
    /// Go `slog.Level.String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

/// Renders a record the way upstream's text handler does (`slog.NewTextHandler`):
/// `level=INFO msg=... key=value ...` and a newline, with groups flattened into
/// dotted keys and values quoted when needed. The `time` attribute is omitted
/// (records carry no timestamp here).
pub fn format_text(level: Level, msg: &str, attrs: &[Attr]) -> String {
    let mut out = String::with_capacity(64);
    out.push_str("level=");
    out.push_str(level.as_str());
    out.push_str(" msg=");
    push_text_string(&mut out, msg);
    let mut prefix = String::new();
    for attr in attrs {
        push_text_attr(&mut out, &mut prefix, attr);
    }
    out.push('\n');
    out
}

fn push_text_attr(out: &mut String, prefix: &mut String, attr: &Attr) {
    match &attr.value {
        LogValue::Group(attrs) => {
            // Empty groups print nothing; groups with an empty key are inlined.
            let len = prefix.len();
            if !attr.key.is_empty() {
                prefix.push_str(&attr.key);
                prefix.push('.');
            }
            for a in attrs {
                push_text_attr(out, prefix, a);
            }
            prefix.truncate(len);
        }
        value => {
            out.push(' ');
            push_text_string(out, &format!("{prefix}{}", attr.key));
            out.push('=');
            match value {
                LogValue::Bool(_) | LogValue::Int(_) | LogValue::Uint(_) => out.push_str(&value.to_string()),
                LogValue::String(s) => push_text_string(out, s),
                other => push_text_string(out, &other.to_string()),
            }
        }
    }
}

/// slog's text handler quotes (Go `strconv.Quote`) empty strings and strings with
/// spaces, `=`, `"`, control or non-printable characters.
fn push_text_string(out: &mut String, s: &str) {
    let needs_quoting = s.is_empty()
        || s.chars().any(|c| {
            if c.is_ascii() {
                c == ' ' || c == '=' || c == '"' || c.is_ascii_control()
            } else {
                c.is_whitespace() || c.is_control()
            }
        });
    if !needs_quoting {
        out.push_str(s);
        return;
    }
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{b}' => out.push_str("\\v"),
            '\u{c}' => out.push_str("\\f"),
            c if c.is_ascii_control() => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Renders a record the way upstream's JSON handler does (`slog.NewJSONHandler`).
pub fn format_json(_level: Level, _msg: &str, _attrs: &[Attr]) -> String {
    pending!("logger::format_json")
}

/// A string or byte value shown truncated in logs (upstream `compactValue`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactValue {
    val: Vec<u8>,
}

impl CompactValue {
    /// The value truncated to [`MAX_VALUE_LENGTH`] bytes plus `...(<cut bytes>)`
    /// (upstream `String()`).
    pub fn compacted(&self) -> String {
        if self.val.len() > MAX_VALUE_LENGTH {
            let head = String::from_utf8_lossy(&self.val[..MAX_VALUE_LENGTH]);
            format!("{head}...({})", self.val.len() - MAX_VALUE_LENGTH)
        } else {
            String::from_utf8_lossy(&self.val).into_owned()
        }
    }
}

impl LogValuer for CompactValue {
    fn log_value(&self) -> LogValue {
        LogValue::String(self.compacted())
    }
}

/// Upstream `CompactValue` for a string.
pub fn compact_value(v: &str) -> CompactValue {
    CompactValue { val: v.as_bytes().to_vec() }
}

/// Upstream `CompactValue` for a byte slice.
pub fn compact_value_bytes(v: &[u8]) -> CompactValue {
    CompactValue { val: v.to_vec() }
}

/// Upstream `CompactValues` for strings.
pub fn compact_values(v: &[&str]) -> Vec<CompactValue> {
    v.iter().map(|s| compact_value(s)).collect()
}

/// Upstream `CompactValues` for byte slices.
pub fn compact_values_bytes(v: &[&[u8]]) -> Vec<CompactValue> {
    v.iter().map(|b| compact_value_bytes(b)).collect()
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
pub fn filtering_enabled_from_env(val: &str) -> bool {
    // Go `strconv.ParseBool`: only its false spellings disable filtering.
    !matches!(val, "0" | "f" | "F" | "false" | "FALSE" | "False")
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
        static GLOBAL: std::sync::OnceLock<LogFilter> = std::sync::OnceLock::new();
        *GLOBAL.get_or_init(|| LogFilter {
            enabled: filtering_enabled_from_env(&std::env::var(FILTER_LOGS_ENV).unwrap_or_default()),
        })
    }

    /// Upstream `FilteredURL` under this policy.
    pub fn filtered_url(&self, val: &str) -> FilteredUrl {
        FilteredUrl { _val: val.to_owned(), _filter: *self }
    }

    /// Upstream `MaskValue` under this policy.
    pub fn mask_value(&self, val: &str) -> String {
        /// Values up to this length are masked completely.
        const FULL_MASK_MAX_LENGTH: usize = 5;
        /// Values up to this length keep only the first and the last characters visible.
        const SHORT_MASK_MAX_LENGTH: usize = 8;
        const MASK: &str = "***";

        if val.is_empty() || !self.enabled {
            return val.to_owned();
        }
        let len = val.len();
        if len <= FULL_MASK_MAX_LENGTH {
            return MASK.to_owned();
        }
        let visible = if len <= SHORT_MASK_MAX_LENGTH { 1 } else { 2 };
        // Go slices bytes; a cut inside a multi-byte character shows as U+FFFD here.
        let bytes = val.as_bytes();
        let head = String::from_utf8_lossy(&bytes[..visible]);
        let tail = String::from_utf8_lossy(&bytes[len - visible..]);
        format!("{head}{MASK}{tail}")
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
        let entries = self._val.iter().flatten();
        if self._filter.enabled {
            // Keys only, sorted (BTreeMap order).
            LogValue::List(entries.map(|(k, _)| LogValue::String(k.clone())).collect())
        } else {
            LogValue::Map(entries.map(|(k, v)| (k.clone(), LogValue::String(v.clone()))).collect())
        }
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
