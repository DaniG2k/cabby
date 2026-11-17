//! Facade for upstream package `logger`.
//!
//! Values are returned as Go's `LogValue().String()`. Upstream reads the filtering
//! switch (`ANYCABLE_FILTER_LOGS`) once at process start, so Go calls under a given
//! filtering setting run in a bridge process with that environment.

use std::collections::BTreeMap;

use cabby_core::logger::{self as rs, AnyValue, Attr, Level, LogFilter, LogValue, LogValuer};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::reference::common::GoParsed;
use crate::{Backend, backend, bridge};

/// Calls a filtering-dependent op: `None` uses the shared bridge (default policy,
/// filtering enabled), `Some(v)` a bridge started with `ANYCABLE_FILTER_LOGS=v`.
fn call_filtered<R: serde::de::DeserializeOwned>(filter_env: Option<&str>, op: &str, args: serde_json::Value) -> R {
    match filter_env {
        None => bridge::call(op, args),
        Some(v) => bridge::call_isolated(&[(rs::FILTER_LOGS_ENV, v)], op, args),
    }
}

fn filter_env(filter: LogFilter) -> Option<&'static str> {
    if filter.enabled { None } else { Some("false") }
}

/// `CompactValue(v).LogValue().String()` for a string.
pub fn compact_value(v: &str) -> String {
    match backend() {
        Backend::Rust => rs::compact_value(v).log_value().to_string(),
        Backend::Go => bridge::call("logger.CompactValue", json!({"Value": v, "Bytes": false})),
    }
}

/// `CompactValue(v).LogValue().String()` for a byte slice.
pub fn compact_value_bytes(v: &[u8]) -> String {
    match backend() {
        Backend::Rust => rs::compact_value_bytes(v).log_value().to_string(),
        Backend::Go => {
            let s = std::str::from_utf8(v).expect("UTF-8 test bytes");
            bridge::call("logger.CompactValue", json!({"Value": s, "Bytes": true}))
        }
    }
}

/// `CompactValues(v)[i].LogValue().String()` for strings.
pub fn compact_values(v: &[&str]) -> Vec<String> {
    match backend() {
        Backend::Rust => rs::compact_values(v).iter().map(|c| c.log_value().to_string()).collect(),
        Backend::Go => bridge::call("logger.CompactValues", json!({"Values": v, "Bytes": false})),
    }
}

/// `CompactValues(v)[i].LogValue().String()` for byte slices.
pub fn compact_values_bytes(v: &[&[u8]]) -> Vec<String> {
    match backend() {
        Backend::Rust => rs::compact_values_bytes(v).iter().map(|c| c.log_value().to_string()).collect(),
        Backend::Go => bridge::call("logger.CompactValues", json!({"Values": utf8_all(v), "Bytes": true})),
    }
}

/// `slog.Any(key, CompactValues(v)).String()` for byte slices.
pub fn compact_values_bytes_attr(key: &str, v: &[&[u8]]) -> String {
    match backend() {
        Backend::Rust => {
            let list = rs::compact_values_bytes(v).iter().map(|c| c.log_value()).collect();
            Attr::new(key, LogValue::List(list)).to_string()
        }
        Backend::Go => {
            bridge::call("logger.CompactValues.AttrString", json!({"Key": key, "Values": utf8_all(v), "Bytes": true}))
        }
    }
}

fn utf8_all(v: &[&[u8]]) -> Vec<String> {
    v.iter().map(|b| std::str::from_utf8(b).expect("UTF-8 test bytes").to_owned()).collect()
}

/// `CompactAny(struct{ val string }{val}).LogValue().String()` (the upstream fixture).
pub fn compact_any_struct_val(val: &str) -> String {
    match backend() {
        Backend::Rust => {
            let value = AnyValue::Struct(vec![("val".into(), AnyValue::Json(json!(val)))]);
            rs::compact_any(value).log_value().to_string()
        }
        Backend::Go => bridge::call("logger.CompactAny.StructVal", json!({"Val": val})),
    }
}

/// The JSON handler (debug level) output of
/// `Debug(msg, "b", CompactValue([]byte(short)), "l", CompactValues([]string{short, long}))`.
pub fn json_handler_compact_values(msg: &str, short: &str, long: &str) -> String {
    match backend() {
        Backend::Rust => {
            let list = rs::compact_values(&[short, long]).iter().map(|c| c.log_value()).collect();
            rs::format_json(
                Level::Debug,
                msg,
                &[
                    Attr::new("b", rs::compact_value_bytes(short.as_bytes()).log_value()),
                    Attr::new("l", LogValue::List(list)),
                ],
            )
        }
        Backend::Go => {
            bridge::call("logger.JSONHandler.CompactValues", json!({"Msg": msg, "Short": short, "Long": long}))
        }
    }
}

/// `FilteredURL(val).LogValue().String()` under `filter`.
pub fn filtered_url(filter: LogFilter, val: &str) -> String {
    match backend() {
        Backend::Rust => filter.filtered_url(val).log_value().to_string(),
        Backend::Go => call_filtered(filter_env(filter), "logger.FilteredURL", json!({"Val": val})),
    }
}

/// `MaskValue(val)` under `filter`.
pub fn mask_value(filter: LogFilter, val: &str) -> String {
    match backend() {
        Backend::Rust => filter.mask_value(val),
        Backend::Go => call_filtered(filter_env(filter), "logger.MaskValue", json!({"Val": val})),
    }
}

/// `FilteredMap(m).LogValue().String()` under `filter` (`None`: a nil map).
pub fn filtered_map(filter: LogFilter, m: Option<&BTreeMap<String, String>>) -> String {
    match backend() {
        Backend::Rust => filter.filtered_map(m).log_value().to_string(),
        Backend::Go => call_filtered(filter_env(filter), "logger.FilteredMap", json!({"Map": m})),
    }
}

/// Upstream `filteringEnabledFromEnv(val)`. The Go function is unexported; it is
/// observed as `FilteringEnabled()` in a process started with `ANYCABLE_FILTER_LOGS=val`.
pub fn filtering_enabled_from_env(val: &str) -> bool {
    match backend() {
        Backend::Rust => rs::filtering_enabled_from_env(val),
        Backend::Go => bridge::call_isolated(&[(rs::FILTER_LOGS_ENV, val)], "logger.FilteringEnabled", json!({})),
    }
}

// --- Config -------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct GoLoggerConfig {
    log_level: String,
    log_format: String,
    debug: bool,
}

impl From<&rs::Config> for GoLoggerConfig {
    fn from(c: &rs::Config) -> Self {
        GoLoggerConfig { log_level: c.log_level.clone(), log_format: c.log_format.clone(), debug: c.debug }
    }
}

impl From<GoLoggerConfig> for rs::Config {
    fn from(g: GoLoggerConfig) -> Self {
        rs::Config { log_level: g.log_level, log_format: g.log_format, debug: g.debug }
    }
}

pub fn new_config() -> rs::Config {
    match backend() {
        Backend::Rust => rs::Config::new(),
        Backend::Go => bridge::call::<_, GoLoggerConfig>("logger.NewConfig", json!({})).into(),
    }
}

pub fn config_to_toml(c: &rs::Config) -> String {
    match backend() {
        Backend::Rust => c.to_toml(),
        Backend::Go => bridge::call("logger.Config.ToToml", json!({"Config": GoLoggerConfig::from(c)})),
    }
}

/// `toml.Decode(s, &conf)`.
pub fn config_decode_toml(c: &mut rs::Config, toml: &str) -> Result<(), rs::TomlError> {
    match backend() {
        Backend::Rust => c.decode_toml(toml),
        Backend::Go => {
            let p: GoParsed<GoLoggerConfig> =
                bridge::call("logger.Config.DecodeToml", json!({"Config": GoLoggerConfig::from(&*c), "Toml": toml}));
            *c = p.result.expect("decoded config").into();
            p.error.map_or(Ok(()), |e| Err(rs::TomlError(e)))
        }
    }
}
