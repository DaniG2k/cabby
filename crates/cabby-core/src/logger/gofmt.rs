//! The bits of Go's `fmt` and `strconv` formatting that log values depend on.

use std::fmt::Write as _;

use serde_json::Value;

use super::AnyValue;

/// Go `fmt.Sprintf("%+v", v)` for a struct or a JSON-decoded `interface{}`.
pub(super) fn write_plus_v(out: &mut String, v: &AnyValue) {
    match v {
        AnyValue::Json(j) => write_json_v(out, j),
        AnyValue::Struct(fields) => {
            out.push('{');
            for (i, (name, value)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                out.push_str(name);
                out.push(':');
                write_plus_v(out, value);
            }
            out.push('}');
        }
    }
}

/// `%+v` of a value decoded by `encoding/json` into `interface{}` (numbers are float64).
fn write_json_v(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("<nil>"),
        Value::Bool(b) => {
            let _ = write!(out, "{b}");
        }
        Value::Number(n) => write_float_v(out, n.as_f64().unwrap_or_default()),
        Value::String(s) => out.push_str(s),
        Value::Array(items) => {
            out.push('[');
            for (i, e) in items.iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                write_json_v(out, e);
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut entries: Vec<_> = m.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            out.push_str("map[");
            for (i, (k, e)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(' ');
                }
                out.push_str(k);
                out.push(':');
                write_json_v(out, e);
            }
            out.push(']');
        }
    }
}

/// `%v` of a float64: `strconv.FormatFloat(f, 'g', -1, 64)`, with `+Inf`/`-Inf`.
fn write_float_v(out: &mut String, f: f64) {
    if f.is_nan() {
        out.push_str("NaN");
        return;
    }
    if f.is_infinite() {
        out.push_str(if f > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    if f == 0.0 {
        let _ = write!(out, "{f}");
        return;
    }
    let e = format!("{f:e}");
    let (mantissa, exp) = e.split_once('e').expect("exponent form");
    let exp: i32 = exp.parse().expect("exponent");
    if !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        let _ = write!(out, "{mantissa}e{sign}{:02}", exp.abs());
    } else {
        let _ = write!(out, "{f}");
    }
}

/// Go `unicode.IsPrint`, approximated with the character classes Rust exposes.
pub(super) fn is_print(c: char) -> bool {
    c == ' '
        || !(c.is_control()
            || c.is_whitespace()
            || matches!(c, '\u{ad}' | '\u{200b}'..='\u{200f}' | '\u{2028}'..='\u{202e}' | '\u{2060}'..='\u{2064}' | '\u{feff}'))
}

/// Go `strconv.Quote`.
pub(super) fn quote(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{b}' => out.push_str("\\v"),
            c if is_print(c) => out.push(c),
            c if (c as u32) < 0x80 => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c if (c as u32) < 0x10000 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => {
                let _ = write!(out, "\\U{:08x}", c as u32);
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn v(j: Value) -> String {
        let mut s = String::new();
        write_plus_v(&mut s, &AnyValue::Json(j));
        s
    }

    #[test]
    fn formats_like_go() {
        assert_eq!("map[a:1 b:[x <nil> true]]", v(json!({"b": ["x", null, true], "a": 1})));
        assert_eq!("1e+06", v(json!(1_000_000)));
        assert_eq!("123456", v(json!(123456)));
        assert_eq!("1.5e-05", v(json!(0.000015)));
        assert_eq!("0.0001", v(json!(0.0001)));
        let mut q = String::new();
        quote(&mut q, "a\"b\n\u{1}é");
        assert_eq!(r#""a\"b\n\x01é""#, q);
    }
}
