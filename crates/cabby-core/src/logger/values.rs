//! Log value wrappers (upstream `logger/values.go`).

use std::collections::BTreeMap;

use super::gofmt::write_plus_v;
use super::url::GoUrl;
use super::{LogFilter, LogValue, LogValuer, MAX_VALUE_LENGTH};

/// Truncates `val` to [`MAX_VALUE_LENGTH`] bytes plus a `...(N)` suffix with the
/// number of dropped bytes (upstream `compactValue.String`).
fn compact(val: &[u8]) -> String {
    if val.len() > MAX_VALUE_LENGTH {
        let mut out = String::from_utf8_lossy(&val[..MAX_VALUE_LENGTH]).into_owned();
        out.push_str(&format!("...({})", val.len() - MAX_VALUE_LENGTH));
        out
    } else {
        String::from_utf8_lossy(val).into_owned()
    }
}

/// A string or byte value shown truncated in logs (upstream `compactValue`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactValue {
    val: Vec<u8>,
}

impl LogValuer for CompactValue {
    fn log_value(&self) -> LogValue {
        LogValue::String(compact(&self.val))
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
    val: AnyValue,
}

impl LogValuer for CompactAny {
    fn log_value(&self) -> LogValue {
        let mut s = String::new();
        write_plus_v(&mut s, &self.val);
        LogValue::String(compact(s.as_bytes()))
    }
}

/// Upstream `CompactAny`.
pub fn compact_any(val: AnyValue) -> CompactAny {
    CompactAny { val }
}

/// A URL shown with masked query values and user info (upstream `filteredURL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredUrl {
    pub(super) val: String,
    pub(super) filter: LogFilter,
}

impl FilteredUrl {
    fn render(&self) -> String {
        if !self.filter.enabled {
            return self.val.clone();
        }
        let Ok(mut u) = GoUrl::parse(&self.val) else {
            return "[invalid url]".to_owned();
        };

        // User info is rebuilt by hand: the URL encoder would escape the mask characters.
        let mut userinfo = String::new();
        if let Some(user) = u.user.take() {
            userinfo = self.filter.mask_value(&user.username);
            if let Some(password) = &user.password {
                userinfo.push(':');
                userinfo.push_str(&self.filter.mask_value(password));
            }
        }

        if !u.raw_query.is_empty() {
            let params: Vec<String> = u
                .raw_query
                .split('&')
                .map(|param| match param.split_once('=') {
                    Some((key, value)) => format!("{key}={}", self.filter.mask_value(value)),
                    None => param.to_owned(),
                })
                .collect();
            u.raw_query = params.join("&");
        }

        let out = u.to_string();
        if userinfo.is_empty() { out } else { out.replacen("//", &format!("//{userinfo}@"), 1) }
    }
}

impl LogValuer for FilteredUrl {
    fn log_value(&self) -> LogValue {
        LogValue::String(self.render())
    }
}

/// A map shown by its keys only (upstream `filteredMap`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilteredMap {
    pub(super) val: Option<BTreeMap<String, String>>,
    pub(super) filter: LogFilter,
}

impl LogValuer for FilteredMap {
    fn log_value(&self) -> LogValue {
        let map = self.val.as_ref();
        if !self.filter.enabled {
            let entries = map.into_iter().flatten().map(|(k, v)| (k.clone(), LogValue::String(v.clone())));
            return LogValue::Map(entries.collect());
        }
        // BTreeMap keys are already sorted.
        LogValue::List(map.into_iter().flat_map(|m| m.keys()).map(|k| LogValue::String(k.clone())).collect())
    }
}
