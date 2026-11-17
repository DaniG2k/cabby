//! Record rendering as Go's `slog.NewTextHandler` and `slog.NewJSONHandler` do it.
//!
//! Timestamps are UTC with millisecond precision (slog uses the local zone; servers
//! usually run in UTC). In JSON, lists and maps are rendered from their resolved values;
//! slog marshals the unresolved Go value, which shows wrappers such as `CompactValues`
//! as `{}` (see `compat/deviations.md`, D-006).

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use super::gofmt::{is_print, quote};
use super::{Attr, Level, LogValue};

/// Renders a record the way upstream's text handler does (`slog.NewTextHandler`).
pub fn format_text(level: Level, msg: &str, attrs: &[Attr]) -> String {
    let mut out = String::with_capacity(64);
    out.push_str("time=");
    write_time(&mut out);
    out.push_str(" level=");
    out.push_str(level.as_str());
    out.push_str(" msg=");
    text_string(&mut out, msg);
    for a in attrs {
        text_attr(&mut out, "", a);
    }
    out.push('\n');
    out
}

/// Renders a record the way upstream's JSON handler does (`slog.NewJSONHandler`).
pub fn format_json(level: Level, msg: &str, attrs: &[Attr]) -> String {
    let mut out = String::with_capacity(64);
    out.push_str("{\"time\":\"");
    write_time(&mut out);
    out.push_str("\",\"level\":\"");
    out.push_str(level.as_str());
    out.push_str("\",\"msg\":");
    json_string(&mut out, msg, false);
    for a in attrs {
        json_attr(&mut out, a);
    }
    out.push_str("}\n");
    out
}

fn is_empty_group(v: &LogValue) -> bool {
    matches!(v, LogValue::Group(attrs) if attrs.is_empty())
}

fn text_attr(out: &mut String, prefix: &str, a: &Attr) {
    if let LogValue::Group(attrs) = &a.value {
        if attrs.is_empty() {
            return;
        }
        // Groups flatten into dotted keys; an empty key inlines the group.
        let prefix = if a.key.is_empty() { prefix.to_owned() } else { format!("{prefix}{}.", a.key) };
        for inner in attrs {
            text_attr(out, &prefix, inner);
        }
        return;
    }
    out.push(' ');
    text_string(out, &format!("{prefix}{}", a.key));
    out.push('=');
    match &a.value {
        LogValue::String(s) => text_string(out, s),
        other => text_string(out, &other.to_string()),
    }
}

/// slog `needsQuoting`.
fn needs_quoting(s: &str) -> bool {
    s.is_empty()
        || s.chars().any(|c| {
            if c.is_ascii() {
                c != '\\' && (c == ' ' || c == '=' || c == '"' || c.is_ascii_control())
            } else {
                c.is_whitespace() || !is_print(c)
            }
        })
}

fn text_string(out: &mut String, s: &str) {
    if needs_quoting(s) { quote(out, s) } else { out.push_str(s) }
}

fn json_attr(out: &mut String, a: &Attr) {
    if is_empty_group(&a.value) {
        return;
    }
    if let (true, LogValue::Group(attrs)) = (a.key.is_empty(), &a.value) {
        for inner in attrs {
            json_attr(out, inner);
        }
        return;
    }
    out.push(',');
    json_string(out, &a.key, false);
    out.push(':');
    json_value(out, &a.value, false);
}

/// `html` selects `encoding/json` escaping (used for values slog hands to
/// `json.Marshal`); slog's own strings skip HTML escaping.
fn json_value(out: &mut String, v: &LogValue, html: bool) {
    match v {
        LogValue::String(s) => json_string(out, s, html),
        LogValue::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        LogValue::Int(i) => {
            let _ = write!(out, "{i}");
        }
        LogValue::Uint(u) => {
            let _ = write!(out, "{u}");
        }
        LogValue::List(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_value(out, item, true);
            }
            out.push(']');
        }
        LogValue::Map(m) => {
            out.push('{');
            for (i, (k, item)) in m.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_string(out, k, true);
                out.push(':');
                json_value(out, item, true);
            }
            out.push('}');
        }
        LogValue::Group(attrs) => {
            out.push('{');
            let start = out.len();
            for a in attrs {
                json_attr(out, a);
            }
            // json_attr writes a leading comma per attribute; drop the first one.
            if out.len() > start {
                out.remove(start);
            }
            out.push('}');
        }
    }
}

fn json_string(out: &mut String, s: &str, html: bool) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' | '>' | '&' if !html => out.push(c),
            '<' | '>' | '&' | '\u{2028}' | '\u{2029}' | '\0'..='\u{1f}' => {
                let n = c as u32;
                out.push_str("\\u");
                for shift in [12, 8, 4, 0] {
                    out.push(HEX[((n >> shift) & 0xf) as usize] as char);
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The current time as `2006-01-02T15:04:05.000Z`.
fn write_time(out: &mut String) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let millis = now.subsec_millis();
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    let _ = write!(out, "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{millis:03}Z", rem / 3600, (rem % 3600) / 60, rem % 60);
}

/// Days since 1970-01-01 to a proleptic Gregorian date (H. Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strip_time(s: &str) -> String {
        let s = s.trim_end();
        if let Some(rest) = s.strip_prefix("time=") {
            rest.split_once(' ').map(|(_, r)| r.to_owned()).unwrap_or_default()
        } else {
            let i = s.find("\"level\"").unwrap();
            format!("{{{}", &s[i..])
        }
    }

    fn attrs() -> Vec<Attr> {
        vec![
            Attr::new("s", LogValue::String("a b".into())),
            Attr::new(
                "res",
                LogValue::Group(vec![
                    Attr::new("istate", LogValue::List(vec![LogValue::String("user_email".into())])),
                    Attr::new("n", LogValue::Int(3)),
                    Attr::new("empty", LogValue::Group(vec![])),
                ]),
            ),
            Attr::new("e", LogValue::String(String::new())),
        ]
    }

    #[test]
    fn renders_text_like_slog() {
        assert_eq!(
            r#"level=INFO msg=result s="a b" res.istate=[user_email] res.n=3 e="""#,
            strip_time(&format_text(Level::Info, "result", &attrs()))
        );
    }

    #[test]
    fn renders_json_like_slog() {
        assert_eq!(
            r#"{"level":"DEBUG","msg":"<m>","s":"a b","res":{"istate":["user_email"],"n":3},"e":""}"#,
            strip_time(&format_json(Level::Debug, "<m>", &attrs()))
        );
    }

    #[test]
    fn converts_days_to_dates() {
        assert_eq!((1970, 1, 1), civil_from_days(0));
        assert_eq!((2000, 2, 29), civil_from_days(11_016));
        assert_eq!((2026, 11, 17), civil_from_days(20_774));
    }
}
