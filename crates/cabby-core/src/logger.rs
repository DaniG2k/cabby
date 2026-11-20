//! Log value helpers and logging configuration (upstream package `logger`).
//!
//! Upstream builds on Go's `log/slog`: values implement `slog.LogValuer` and are
//! rendered by slog handlers. Cabby models the resolved value tree ([`LogValue`])
//! and the two handler formats used upstream ([`format_text`], [`format_json`]).
//! `Display` of a [`LogValue`] corresponds to Go's `slog.Value.String()`.
//!
//! Sensitive data filtering is a value ([`LogFilter`]) rather than a mutable package
//! global: [`LogFilter::global`] reads `ANYCABLE_FILTER_LOGS` once per process (and
//! never writes the environment), and everything else takes the policy explicitly, so
//! tests can exercise both policies in parallel.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};

mod config;
mod format;
mod gofmt;
mod url;
mod values;

pub use config::{Config, TomlError};
pub use format::{format_json, format_text};
pub use values::{
    AnyValue, CompactAny, CompactValue, FilteredMap, FilteredUrl, compact_any, compact_value, compact_value_bytes,
    compact_values, compact_values_bytes,
};

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
    /// Go `slog.Value.String()`: scalars as is, slices as `[a b]`, maps as
    /// `map[k:v]` (sorted keys), groups as `[k=v k2=v2]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogValue::String(s) => f.write_str(s),
            LogValue::Bool(b) => write!(f, "{b}"),
            LogValue::Int(i) => write!(f, "{i}"),
            LogValue::Uint(u) => write!(f, "{u}"),
            LogValue::List(items) => {
                f.write_str("[")?;
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" ")?;
                    }
                    write!(f, "{v}")?;
                }
                f.write_str("]")
            }
            LogValue::Map(m) => {
                f.write_str("map[")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" ")?;
                    }
                    write!(f, "{k}:{v}")?;
                }
                f.write_str("]")
            }
            LogValue::Group(attrs) => {
                f.write_str("[")?;
                for (i, a) in attrs.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" ")?;
                    }
                    write!(f, "{a}")?;
                }
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

/// Upstream `filteringEnabledFromEnv`: parses the `ANYCABLE_FILTER_LOGS` value
/// (anything unparsable keeps filtering enabled).
pub fn filtering_enabled_from_env(val: &str) -> bool {
    // Go strconv.ParseBool
    match val {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => true,
        "0" | "f" | "F" | "FALSE" | "false" | "False" => false,
        _ => true,
    }
}

const FILTERED_MASK: &str = "***";
/// Values up to this length are masked completely.
const FULL_MASK_MAX_LENGTH: usize = 5;
/// Values up to this length keep only the first and the last characters visible.
const SHORT_MASK_MAX_LENGTH: usize = 8;

/// Sensitive data filtering policy. Upstream keeps it in a package global read from
/// `ANYCABLE_FILTER_LOGS` at start; Cabby passes it explicitly so tests do not mutate
/// process state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogFilter {
    pub enabled: bool,
}

impl LogFilter {
    /// The process-wide policy (upstream `FilteringEnabled()`), read from
    /// `ANYCABLE_FILTER_LOGS` on first use.
    pub fn global() -> LogFilter {
        static GLOBAL: OnceLock<LogFilter> = OnceLock::new();
        *GLOBAL.get_or_init(|| {
            let val = std::env::var(FILTER_LOGS_ENV).unwrap_or_default();
            LogFilter { enabled: filtering_enabled_from_env(&val) }
        })
    }

    /// Upstream `FilteredURL` under this policy.
    pub fn filtered_url(&self, val: &str) -> FilteredUrl {
        FilteredUrl { val: val.to_owned(), filter: *self }
    }

    /// Upstream `MaskValue` under this policy: hides the value, keeping only the first
    /// and the last characters visible if the value is long enough. Lengths count
    /// bytes, as upstream.
    pub fn mask_value(&self, val: &str) -> String {
        if val.is_empty() || !self.enabled {
            return val.to_owned();
        }
        let b = val.as_bytes();
        if b.len() <= FULL_MASK_MAX_LENGTH {
            return FILTERED_MASK.to_owned();
        }
        let visible = if b.len() <= SHORT_MASK_MAX_LENGTH { 1 } else { 2 };
        let mut out = String::from_utf8_lossy(&b[..visible]).into_owned();
        out.push_str(FILTERED_MASK);
        out.push_str(&String::from_utf8_lossy(&b[b.len() - visible..]));
        out
    }

    /// Upstream `FilteredMap` under this policy (`None`: a nil map).
    pub fn filtered_map(&self, val: Option<&BTreeMap<String, String>>) -> FilteredMap {
        FilteredMap { val: val.cloned(), filter: *self }
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

/// One record handed to the process logger.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    pub level: Level,
    pub msg: &'a str,
    /// The logger's context attributes (upstream `logger.With(...)`), before `attrs`.
    pub context: &'a [Attr],
    /// The call's own attributes.
    pub attrs: &'a [Attr],
    /// Whether the process-wide attributes (upstream `slog.With("nodeid", id)`,
    /// which the runner passes to most components) come first. Upstream
    /// components that log through `slog.With(...)` directly do not get them.
    pub with_base: bool,
}

/// Where records go (upstream's default `slog` logger). The process logger
/// installs itself with [`set_sink`]; until then records are dropped.
pub type Sink = fn(&Record<'_>);

static SINK: OnceLock<Sink> = OnceLock::new();

/// Lowest level written ([`Level::rank`]); [`LEVEL_OFF`] discards everything.
static MIN_LEVEL: AtomicU8 = AtomicU8::new(1);
const LEVEL_OFF: u8 = 4;

impl Level {
    /// Ordering of levels (slog: debug < info < warn < error).
    pub fn rank(self) -> u8 {
        match self {
            Level::Debug => 0,
            Level::Info => 1,
            Level::Warn => 2,
            Level::Error => 3,
        }
    }
}

/// Installs the process logger (the first call wins).
pub fn set_sink(sink: Sink) {
    let _ = SINK.set(sink);
}

/// Sets the lowest level that is written (`None`: nothing is). The process
/// logger keeps it in sync with its configuration.
pub fn set_min_level(level: Option<Level>) {
    MIN_LEVEL.store(level.map_or(LEVEL_OFF, Level::rank), Ordering::Relaxed);
}

/// Whether a record at `level` would be written: an atomic load and a
/// comparison, so callers check it before building attributes (debug records
/// on hot paths cost next to nothing when debug is off).
#[inline]
pub fn enabled(level: Level) -> bool {
    level.rank() >= MIN_LEVEL.load(Ordering::Relaxed) && SINK.get().is_some()
}

/// Writes a record without context to the process logger.
pub fn log(level: Level, msg: &str, attrs: &[Attr]) {
    Logger::default().log(level, msg, attrs);
}

/// A logger with context attributes (upstream `*slog.Logger` values built with
/// `With(...)`). Cloning is cheap; [`Logger::with`] allocates the new context.
#[derive(Debug, Clone)]
pub struct Logger {
    context: Arc<[Attr]>,
    with_base: bool,
}

impl Default for Logger {
    /// The runner's logger (upstream `slog.With("nodeid", id)`).
    fn default() -> Self {
        Logger { context: Arc::from(Vec::new()), with_base: true }
    }
}

impl Logger {
    /// A logger without the process-wide attributes (upstream `slog.With(...)`
    /// on the default logger, bypassing the runner's).
    pub fn root() -> Self {
        Logger { with_base: false, ..Logger::default() }
    }

    /// A logger with one more context attribute (upstream `With(key, value)`).
    pub fn with(&self, key: &str, value: impl Into<LogValue>) -> Logger {
        let mut context = Vec::with_capacity(self.context.len() + 1);
        context.extend_from_slice(&self.context);
        context.push(Attr::new(key, value.into()));
        Logger { context: Arc::from(context), with_base: self.with_base }
    }

    /// The context attributes.
    pub fn context(&self) -> &[Attr] {
        &self.context
    }

    /// See [`enabled`].
    #[inline]
    pub fn enabled(&self, level: Level) -> bool {
        enabled(level)
    }

    /// Writes a record (context attributes first). Prefer the `log_*!` macros,
    /// which skip building the attributes of disabled records.
    pub fn log(&self, level: Level, msg: &str, attrs: &[Attr]) {
        if !enabled(level) {
            return;
        }
        if let Some(sink) = SINK.get() {
            sink(&Record { level, msg, context: &self.context, attrs, with_base: self.with_base });
        }
    }
}

/// Logs through a [`Logger`] at a level, building the attributes only when the
/// level is enabled: `log_at!(logger, Level::Debug, "msg", "key" => value, ...)`.
/// Values are anything `Into<LogValue>`.
#[macro_export]
macro_rules! log_at {
    ($logger:expr, $level:expr, $msg:expr $(, $key:expr => $value:expr)* $(,)?) => {{
        let level = $level;
        if $crate::logger::enabled(level) {
            $logger.log(level, $msg, &[$($crate::logger::Attr::new($key, $crate::logger::LogValue::from($value))),*]);
        }
    }};
}

/// [`log_at!`] at debug level.
#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Debug, $($rest)*) };
}

/// [`log_at!`] at info level.
#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Info, $($rest)*) };
}

/// [`log_at!`] at warn level.
#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Warn, $($rest)*) };
}

/// [`log_at!`] at error level.
#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($rest:tt)*) => { $crate::log_at!($logger, $crate::logger::Level::Error, $($rest)*) };
}

impl From<&str> for LogValue {
    fn from(v: &str) -> Self {
        LogValue::String(v.to_owned())
    }
}

impl From<String> for LogValue {
    fn from(v: String) -> Self {
        LogValue::String(v)
    }
}

impl From<&String> for LogValue {
    fn from(v: &String) -> Self {
        LogValue::String(v.clone())
    }
}

impl From<bool> for LogValue {
    fn from(v: bool) -> Self {
        LogValue::Bool(v)
    }
}

impl From<i32> for LogValue {
    fn from(v: i32) -> Self {
        LogValue::Int(v.into())
    }
}

impl From<i64> for LogValue {
    fn from(v: i64) -> Self {
        LogValue::Int(v)
    }
}

impl From<u32> for LogValue {
    fn from(v: u32) -> Self {
        LogValue::Uint(v.into())
    }
}

impl From<u64> for LogValue {
    fn from(v: u64) -> Self {
        LogValue::Uint(v)
    }
}

impl From<usize> for LogValue {
    fn from(v: usize) -> Self {
        LogValue::Uint(v as u64)
    }
}

/// Go's `slog.Any(key, err)` for an `error` that may be nil: its message, or `<nil>`.
pub fn error_value<E: fmt::Display + ?Sized>(err: Option<&E>) -> LogValue {
    match err {
        Some(e) => LogValue::String(e.to_string()),
        None => LogValue::String("<nil>".to_owned()),
    }
}

/// Go's `slog.Any(key, v)` for a nil-able pointer to a `LogValuer`: its value,
/// or `<nil>`.
pub fn optional_value<T: LogValuer>(v: Option<&T>) -> LogValue {
    match v {
        Some(v) => v.log_value(),
        None => LogValue::String("<nil>".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_like_slog_values() {
        let v = LogValue::Group(vec![
            Attr::new("a", LogValue::Int(-1)),
            Attr::new("l", LogValue::List(vec![LogValue::String("x".into()), LogValue::Bool(true)])),
            Attr::new("m", LogValue::Map([("k".to_owned(), LogValue::Uint(2))].into())),
        ]);
        assert_eq!("[a=-1 l=[x true] m=map[k:2]]", v.to_string());
    }

    #[test]
    fn masks_by_length() {
        let f = LogFilter { enabled: true };
        assert_eq!("", f.mask_value(""));
        assert_eq!("***", f.mask_value("12345"));
        assert_eq!("1***6", f.mask_value("123456"));
        assert_eq!("12***89", f.mask_value("123456789"));
    }
}
