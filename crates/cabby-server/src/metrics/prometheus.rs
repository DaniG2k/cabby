//! Prometheus text exposition (upstream `metrics/prometheus.go`).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use cabby_core::common::StringMap;

use super::{Counter, Gauge, PROMETHEUS_NAMESPACE, sorted};

/// Default connection cap for a dedicated metrics server when `server.max_conn`
/// is 0 (unlimited): prevents an exposed metrics port from exhausting FDs.
pub(super) const DEFAULT_DEDICATED_MAX_CONN: usize = 1024;

/// Escapes a Prometheus label value per the text exposition format (`\`, `"`, `\n`).
fn escape_label_value(v: &str) -> String {
    if !v.contains(['\\', '"', '\n']) {
        return v.to_owned();
    }
    let mut out = String::with_capacity(v.len() + 4);
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

fn prom_tags(tags: Option<&StringMap>) -> String {
    let Some(tags) = tags else {
        return String::new();
    };
    // Upstream iterates a Go map (random order); sorted here.
    let mut sorted: Vec<_> = tags.iter().collect();
    sorted.sort();
    let parts: Vec<String> = sorted.into_iter().map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v))).collect();
    format!("{{{}}}", parts.join(", "))
}

/// Upstream `Metrics.Prometheus`: counters, then gauges (each in name order).
pub(super) fn render(
    counters: &HashMap<String, Arc<Counter>>,
    gauges: &HashMap<String, Arc<Gauge>>,
    tags: Option<&StringMap>,
) -> String {
    let tags = prom_tags(tags);
    let mut buf = String::with_capacity(128 * (counters.len() + gauges.len()));
    let mut metric = |name: &str, desc: &str, kind: &str, value: u64| {
        let _ = write!(
            buf,
            "\n# HELP {PROMETHEUS_NAMESPACE}_{name} {desc}\n# TYPE {PROMETHEUS_NAMESPACE}_{name} {kind}\n\
             {PROMETHEUS_NAMESPACE}_{name}{tags} {value}\n"
        );
    };
    for c in sorted(counters) {
        metric(c.name(), c.desc(), "counter", c.value());
    }
    for g in sorted(gauges) {
        metric(g.name(), g.desc(), "gauge", g.value());
    }
    buf
}

/// The handler's response: 200 with the exposition text (Go `fmt.Fprint`, whose
/// content type is sniffed as plain text).
pub(super) fn response(body: String) -> http::Response<String> {
    http::Response::builder()
        .status(200)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body)
        .expect("a valid response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_prometheus_label_values() {
        let tags = StringMap::from([("env".into(), "prod".into()), ("note".into(), "a\"b\\c\nd".into())]);
        assert_eq!(r#"{env="prod", note="a\"b\\c\nd"}"#, prom_tags(Some(&tags)));
    }
}
