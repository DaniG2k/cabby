//! Go `encoding/json` compatibility helpers.
//!
//! Output written with [`to_vec`] / [`to_string`] is byte-identical to Go's
//! `json.Marshal` for the same shapes: compact, HTML-escaped (`<`, `>`, `&`,
//! U+2028, U+2029) and with Go's float formatting. Key order is whatever the
//! `Serialize` impl produces; `serde_json::Map` (no `preserve_order`) and
//! `BTreeMap` sort keys bytewise, as Go does for maps.
//!
//! Decoding helpers model Go's struct decoding: case-insensitive field names
//! ([`field_index`]) and `interface{}` values decoded as `float64`
//! ([`to_go_interface`]).

use std::io;

use serde::Serialize;
use serde::de::{DeserializeSeed, Visitor};
use serde_json::ser::Formatter;
use serde_json::{Number, Value};

/// A compact formatter matching Go's `json.Marshal` escaping and float output.
#[derive(Debug, Clone, Copy, Default)]
pub struct GoFormatter;

impl Formatter for GoFormatter {
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let bytes = fragment.as_bytes();
        let mut start = 0;
        let mut i = 0;
        while i < bytes.len() {
            let (escape, len): (&[u8], usize) = match bytes[i] {
                b'<' => (b"\\u003c", 1),
                b'>' => (b"\\u003e", 1),
                b'&' => (b"\\u0026", 1),
                // U+2028 / U+2029 are E2 80 A8 / E2 80 A9 in UTF-8.
                0xE2 if bytes.get(i + 1) == Some(&0x80) && matches!(bytes.get(i + 2), Some(0xA8 | 0xA9)) => {
                    (if bytes[i + 2] == 0xA8 { b"\\u2028" } else { b"\\u2029" }, 3)
                }
                _ => {
                    i += 1;
                    continue;
                }
            };
            writer.write_all(&bytes[start..i])?;
            writer.write_all(escape)?;
            i += len;
            start = i;
        }
        writer.write_all(&bytes[start..])
    }

    fn write_f64<W>(&mut self, writer: &mut W, value: f64) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(format_float(value).as_bytes())
    }

    fn write_f32<W>(&mut self, writer: &mut W, value: f32) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(format_float32(value).as_bytes())
    }
}

/// Serializes `value` exactly as Go's `json.Marshal` would.
pub fn to_vec<T: Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let mut out = Vec::with_capacity(128);
    write(&mut out, value);
    out
}

/// Appends the Go JSON encoding of `value` to `out`.
pub fn write<T: Serialize + ?Sized>(out: &mut Vec<u8>, value: &T) {
    let mut ser = serde_json::Serializer::with_formatter(out, GoFormatter);
    // Writing to a Vec cannot fail, and the shapes serialized here have string keys only.
    value.serialize(&mut ser).expect("JSON encoding into a buffer");
}

/// Serializes `value` exactly as Go's `json.Marshal` would, as a string.
pub fn to_string<T: Serialize + ?Sized>(value: &T) -> String {
    // The serializer only writes valid UTF-8.
    String::from_utf8(to_vec(value)).expect("JSON is UTF-8")
}

/// Go's `float64` JSON formatting (`encoding/json` `floatEncoder`): shortest
/// round-trip digits, plain notation for `1e-6 <= |f| < 1e21`, exponent otherwise.
pub fn format_float(f: f64) -> String {
    if !f.is_finite() {
        // Go refuses to encode these; `null` keeps the output valid JSON.
        return "null".to_owned();
    }
    let abs = f.abs();
    if abs == 0.0 || (1e-6..1e21).contains(&abs) {
        // Rust's Display is the shortest round-trip form without an exponent; Go
        // prints negative zero as "-0", which Display does too.
        format!("{f}")
    } else {
        exponent_form(format!("{f:e}"))
    }
}

/// Go's `float32` JSON formatting.
pub fn format_float32(f: f32) -> String {
    if !f.is_finite() {
        return "null".to_owned();
    }
    let abs = f.abs();
    if abs == 0.0 || (1e-6..1e21).contains(&abs) { format!("{f}") } else { exponent_form(format!("{f:e}")) }
}

/// Rust writes `1e21` / `1e-7`; Go writes `1e+21` / `1e-7`.
fn exponent_form(s: String) -> String {
    match s.find('e') {
        Some(pos) if !s[pos + 1..].starts_with('-') => format!("{}e+{}", &s[..pos], &s[pos + 1..]),
        _ => s,
    }
}

/// Converts the numbers of a decoded value to Go's `interface{}` semantics: Go
/// decodes JSON numbers into `float64` (no `UseNumber`), so their text becomes
/// Go's float formatting and out-of-range numbers are an error.
pub fn to_go_interface(value: &mut Value) -> Result<(), String> {
    match value {
        Value::Number(n) => {
            let text = n.as_str();
            let f: f64 = text.parse().map_err(|_| number_error(text))?;
            if !f.is_finite() {
                return Err(number_error(text));
            }
            let go = format_float(f);
            if go != text {
                *n = serde_json::from_str::<Number>(&go).map_err(|e| e.to_string())?;
            }
            Ok(())
        }
        Value::Array(items) => items.iter_mut().try_for_each(to_go_interface),
        Value::Object(map) => map.values_mut().try_for_each(to_go_interface),
        _ => Ok(()),
    }
}

fn number_error(text: &str) -> String {
    format!("json: cannot unmarshal number {text} into Go value of type float64")
}

/// The index of the struct field `key` names, matched the way Go's decoder does:
/// exactly or case-insensitively (Go `foldName`). `fields` are ASCII lowercase.
pub fn field_index(key: &str, fields: &[&str]) -> Option<usize> {
    fields.iter().position(|f| *f == key).or_else(|| fields.iter().position(|f| fold_eq(key, f)))
}

/// Go folds names with `unicode.ToUpper(unicode.ToLower(r))`; besides ASCII, only
/// a few code points fold into ASCII letters.
fn fold_eq(key: &str, field: &str) -> bool {
    let mut expected = field.bytes();
    for c in key.chars() {
        let folded = match c {
            c if c.is_ascii() => c.to_ascii_uppercase(),
            '\u{17F}' => 'S',             // LATIN SMALL LETTER LONG S
            '\u{212A}' => 'K',            // KELVIN SIGN
            '\u{130}' | '\u{131}' => 'I', // dotted capital I, dotless small i
            _ => return false,
        };
        match expected.next() {
            Some(b) if b.to_ascii_uppercase() as char == folded => {}
            _ => return false,
        }
    }
    expected.next().is_none()
}

/// Deserializes a map key into the index of the matching struct field (`None`:
/// unknown key, which Go ignores). Borrowed keys are matched without allocating.
#[derive(Debug, Clone, Copy)]
pub struct FieldKey(pub &'static [&'static str]);

impl<'de> DeserializeSeed<'de> for FieldKey {
    type Value = Option<usize>;

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl<'de> Visitor<'de> for FieldKey {
    type Value = Option<usize>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a field name")
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(field_index(v, self.0))
    }
}

/// Decodes `input` the way Go's `json.Unmarshal` fills a struct whose fields are
/// all `string`s, named (ASCII lowercase) by `fields`: keys match exactly or
/// case-insensitively, in document order (a later key overwrites), unknown keys
/// and `null` values are ignored, and `null` input leaves every field empty.
/// A non-string value for a field is an error after the whole object was decoded
/// (Go's `UnmarshalTypeError`); so is input that is not an object or not JSON.
/// `type_name` is the Go struct name used in error texts.
pub fn decode_string_fields<const N: usize>(
    input: &str,
    type_name: &'static str,
    fields: &'static [&'static str; N],
) -> Result<[String; N], String> {
    use serde::Deserializer as _;
    use serde::de::{IgnoredAny, MapAccess};

    struct StringFields<const N: usize> {
        type_name: &'static str,
        fields: &'static [&'static str; N],
    }

    fn kind(v: &Value) -> &'static str {
        match v {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    impl<'de, const N: usize> Visitor<'de> for StringFields<N> {
        type Value = Result<[String; N], String>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "a JSON object for Go type {}", self.type_name)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(Ok(std::array::from_fn(|_| String::new())))
        }

        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut out: [String; N] = std::array::from_fn(|_| String::new());
            let mut type_error: Option<String> = None;
            while let Some(key) = map.next_key_seed(FieldKey(self.fields))? {
                let Some(i) = key else {
                    map.next_value::<IgnoredAny>()?;
                    continue;
                };
                match map.next_value::<Value>()? {
                    Value::String(s) => out[i] = s,
                    Value::Null => {}
                    other => {
                        type_error.get_or_insert_with(|| {
                            format!(
                                "json: cannot unmarshal {} into Go struct field {}.{} of type string",
                                kind(&other),
                                self.type_name,
                                self.fields[i]
                            )
                        });
                    }
                }
            }
            Ok(type_error.map_or(Ok(out), Err))
        }
    }

    // Objects (and `null`) are visited straight from the text, so keys are seen in
    // document order. Anything else is only classified for the error: with
    // `arbitrary_precision` a number would otherwise reach `visit_map`.
    let starts_object = matches!(input.trim_start().as_bytes().first(), Some(b'{' | b'n'));
    let mut de = serde_json::Deserializer::from_str(input);
    if !starts_object {
        let value: Value = serde::Deserialize::deserialize(&mut de).map_err(|e| e.to_string())?;
        de.end().map_err(|e| e.to_string())?;
        return Err(format!("json: cannot unmarshal {} into Go value of type {type_name}", kind(&value)));
    }
    let decoded = de.deserialize_any(StringFields { type_name, fields }).map_err(|e| e.to_string())?;
    de.end().map_err(|e| e.to_string())?;
    decoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decodes_string_fields_like_go() {
        const F: &[&str; 2] = &["stream_name", "signed_stream_name"];
        let d = |s: &str| decode_string_fields(s, "T", F);
        assert_eq!(Ok(["a".to_owned(), String::new()]), d(r#"{"stream_name":"a","x":[1]}"#));
        assert_eq!(Ok(["b".to_owned(), String::new()]), d(r#"{"stream_name":"a","STREAM_NAME":"b"}"#));
        assert_eq!(Ok([String::new(), String::new()]), d("null"));
        assert_eq!(Ok(["a".to_owned(), String::new()]), d(r#"{"stream_name":"a","signed_stream_name":null}"#));
        assert!(d(r#"{"stream_name":1}"#).is_err());
        assert!(d(r#"["a"]"#).is_err());
        assert!(d(r#""a""#).is_err());
        assert!(d("{").is_err());
        assert!(d(r#"{} x"#).is_err());
    }

    #[test]
    fn escapes_html_like_go() {
        assert_eq!(
            "\"\\u003ca\\u003e \\u0026 \\u2028\\u2029 \u{e9}\\n\"",
            to_string("<a> & \u{2028}\u{2029} \u{e9}\n")
        );
        assert_eq!("{\"\\u003c\":1}", to_string(&json!({"<": 1})));
    }

    #[test]
    fn formats_floats_like_go() {
        assert_eq!("1", format_float(1.0));
        assert_eq!("-0", format_float(-0.0));
        assert_eq!("0.1", format_float(0.1));
        assert_eq!("2675291807195072000", format_float(2675291807195071852.0));
        assert_eq!("1e+21", format_float(1e21));
        assert_eq!("1.5e-7", format_float(1.5e-7));
        assert_eq!("0.000001", format_float(1e-6));
    }

    #[test]
    /// Digits are kept exactly; serde_json normalizes the exponent marker (D-010).
    fn keeps_number_text() {
        let v: Value = serde_json::from_str(r#"{"b":1.10,"a":2675291807195071852123,"c":1e2}"#).unwrap();
        assert_eq!(r#"{"a":2675291807195071852123,"b":1.10,"c":1e+2}"#, to_string(&v));
    }

    #[test]
    fn interface_numbers_are_floats() {
        let mut v: Value = serde_json::from_str(r#"[1.10, 1e2, 2675291807195071852, 3]"#).unwrap();
        to_go_interface(&mut v).unwrap();
        assert_eq!("[1.1,100,2675291807195072000,3]", to_string(&v));
        assert_eq!(json!([1.1, 100, 2675291807195072000u64, 3]), v);
        let mut v: Value = serde_json::from_str("1e999").unwrap();
        assert!(to_go_interface(&mut v).is_err());
    }

    #[test]
    fn folds_field_names() {
        let fields = &["command", "streams", "identifier"];
        assert_eq!(Some(0), field_index("command", fields));
        assert_eq!(Some(0), field_index("COMMAND", fields));
        assert_eq!(Some(1), field_index("\u{17F}treams", fields));
        assert_eq!(Some(2), field_index("\u{131}dentifier", fields));
        assert_eq!(None, field_index("commands", fields));
        assert_eq!(None, field_index("comman", fields));
        assert_eq!(None, field_index("çommand", fields));
    }
}
