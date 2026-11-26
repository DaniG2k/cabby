//! TOML configuration decoding and generation helpers shared by every `Config`.
//!
//! Decoding follows upstream's `toml.Decode(s, &conf)` (BurntSushi/toml) over Go
//! structs: values are decoded *over* the current ones (omitted keys keep their
//! values), keys match a field's TOML name exactly or, failing that,
//! case-insensitively (untagged Go fields use the Go field name), nested tables
//! decode into nested sections, pointer sections are allocated on demand, maps
//! merge, arrays replace, and keys that match no field are collected as
//! undecoded instead of failing. Parsing uses the `toml` crate; decoding over
//! the parsed tree is implemented here.

use std::collections::HashMap;
use std::path::PathBuf;

pub use toml::de::{DeTable, DeValue};

/// Invalid TOML or a value of the wrong type for its setting.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct DecodeError(pub String);

/// A struct whose fields decode from a TOML table (upstream: a Go struct with
/// `toml` tags).
pub trait TomlSection {
    /// The Go type name, for type-mismatch errors (e.g. `server.Config`).
    const GO_TYPE: &'static str;
    /// TOML names of the decodable fields, in declaration order: the `toml` tag,
    /// or the Go field name for untagged fields.
    const FIELDS: &'static [&'static str];
    /// Decodes `value` into the field at `index` of [`Self::FIELDS`].
    fn decode_field(&mut self, index: usize, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError>;
}

/// A value that decodes from a TOML value.
pub trait FromToml {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError>;
}

/// Implements [`TomlSection`] for a struct: `"toml_name" => field` pairs.
#[macro_export]
macro_rules! toml_section {
    ($ty:ty, $go:literal { $($name:literal => $field:ident),* $(,)? }) => {
        impl $crate::toml_config::TomlSection for $ty {
            const GO_TYPE: &'static str = $go;
            const FIELDS: &'static [&'static str] = &[$($name),*];

            fn decode_field(
                &mut self,
                index: usize,
                value: &$crate::toml_config::DeValue<'_>,
                d: &mut $crate::toml_config::Decoder<'_>,
            ) -> ::std::result::Result<(), $crate::toml_config::DecodeError> {
                let names: &[&str] = &[$($name),*];
                $(
                    if names[index] == $name {
                        return $crate::toml_config::FromToml::decode_value(&mut self.$field, value, d);
                    }
                )*
                ::std::unreachable!("field index {index} out of range")
            }
        }
    };
}

/// Decoding state: the input (for line numbers), the current key path and the
/// keys that matched no setting.
pub struct Decoder<'i> {
    input: &'i str,
    /// Current key path with the byte offset of each key.
    context: Vec<(String, usize)>,
    undecoded: Vec<String>,
}

/// Decodes `input` over `target`. Returns the dotted paths of the keys that match
/// no setting, in document order; keys under an unknown table are not listed
/// separately (upstream `config.unknownKeys(md.Undecoded())`).
pub fn decode<T: TomlSection>(target: &mut T, input: &str) -> Result<Vec<String>, DecodeError> {
    let root = DeTable::parse(input).map_err(|e| syntax_error(input, &e))?;
    let mut d = Decoder { input, context: Vec::new(), undecoded: Vec::new() };
    d.table(target, root.get_ref())?;
    Ok(d.undecoded)
}

fn syntax_error(input: &str, e: &toml::de::Error) -> DecodeError {
    let message = e.message().trim_end();
    match e.span() {
        Some(span) => DecodeError(format!("toml: line {}: {message}", line_of(input, span.start))),
        None => DecodeError(format!("toml: {message}")),
    }
}

fn line_of(input: &str, offset: usize) -> usize {
    input.as_bytes()[..offset.min(input.len())].iter().filter(|&&b| b == b'\n').count() + 1
}

/// Resolves a document key to a field: an exact name match wins, otherwise the
/// first case-insensitive match (BurntSushi/toml `unifyStruct`).
fn resolve(fields: &[&str], key: &str) -> Option<usize> {
    fields.iter().position(|f| *f == key).or_else(|| fields.iter().position(|f| f.eq_ignore_ascii_case(key)))
}

impl Decoder<'_> {
    /// Decodes the entries of `table` into the fields of `target`.
    pub fn table<T: TomlSection + ?Sized>(&mut self, target: &mut T, table: &DeTable<'_>) -> Result<(), DecodeError> {
        for (key, value) in table.iter() {
            let name: &str = key.get_ref();
            match resolve(T::FIELDS, name) {
                Some(index) => {
                    self.context.push((name.to_owned(), key.span().start));
                    target.decode_field(index, value.get_ref(), self)?;
                    self.context.pop();
                }
                None => {
                    let mut path: Vec<String> = self.context.iter().map(|(k, _)| k.clone()).collect();
                    path.push(name.to_owned());
                    self.unknown(&mut path, key.span(), value.get_ref());
                }
            }
        }
        Ok(())
    }

    /// Records an unknown key. Upstream lists the keys and table headers as they
    /// appear in the document, so a table that only exists as the parent of a
    /// dotted key or of a nested header (`a.b = 1`, `[a.b]`) is not listed itself:
    /// its defined children are.
    fn unknown(&mut self, path: &mut Vec<String>, key_span: std::ops::Range<usize>, value: &DeValue<'_>) {
        let implicit =
            self.input.get(key_span.end..).is_some_and(|rest| rest.trim_start_matches([' ', '\t']).starts_with('.'));
        match value {
            DeValue::Table(table) if implicit && !table.is_empty() => {
                for (key, value) in table.iter() {
                    path.push(key.get_ref().to_string());
                    self.unknown(path, key.span(), value.get_ref());
                    path.pop();
                }
            }
            _ => {
                let parts: Vec<&str> = path.iter().map(String::as_str).collect();
                self.undecoded.push(key_string(&parts));
            }
        }
    }

    /// An error located at the current key (BurntSushi/toml `MetaData.e`).
    pub fn error(&self, message: impl std::fmt::Display) -> DecodeError {
        match self.context.last() {
            None => DecodeError(format!("toml: {message}")),
            Some((_, offset)) => {
                let path: Vec<&str> = self.context.iter().map(|(k, _)| k.as_str()).collect();
                DecodeError(format!(
                    "toml: line {} (last key {}): {message}",
                    line_of(self.input, *offset),
                    go_quote(&key_string(&path))
                ))
            }
        }
    }

    /// `incompatible types` error for a value of the wrong type.
    pub fn bad_type(&self, destination: &str, value: &DeValue<'_>) -> DecodeError {
        self.error(format_args!(
            "incompatible types: TOML value has type {}; destination has type {destination}",
            go_type(value)
        ))
    }
}

/// The Go `%T` of the value BurntSushi/toml decodes a TOML value into.
fn go_type(value: &DeValue<'_>) -> &'static str {
    match value {
        DeValue::String(_) => "string",
        DeValue::Integer(_) => "int64",
        DeValue::Float(_) => "float64",
        DeValue::Boolean(_) => "bool",
        DeValue::Datetime(_) => "time.Time",
        DeValue::Array(_) => "[]any",
        DeValue::Table(_) => "map[string]any",
    }
}

/// A dotted key with parts quoted where they are not bare keys (BurntSushi/toml
/// `Key.String`).
fn key_string(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|p| {
            if !p.is_empty() && p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
                (*p).to_owned()
            } else {
                let mut s = String::with_capacity(p.len() + 2);
                s.push('"');
                for c in p.chars() {
                    match c {
                        '"' => s.push_str("\\\""),
                        '\\' => s.push_str("\\\\"),
                        '\n' => s.push_str("\\n"),
                        '\t' => s.push_str("\\t"),
                        '\r' => s.push_str("\\r"),
                        c if c.is_control() => s.push_str(&format!("\\u{:04X}", c as u32)),
                        c => s.push(c),
                    }
                }
                s.push('"');
                s
            }
        })
        .collect::<Vec<_>>()
        .join(".")
}

impl<T: TomlSection> FromToml for T {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        match value {
            DeValue::Table(table) => d.table(self, table),
            other => Err(d.error(format_args!(
                "type mismatch for {}: expected table but found {}",
                T::GO_TYPE,
                go_type(other)
            ))),
        }
    }
}

/// A Go pointer to a section: allocated (zero value) when the document sets it.
impl<T: TomlSection + Default> FromToml for Option<T> {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        match value {
            DeValue::Table(table) => d.table(self.get_or_insert_with(T::default), table),
            other => Err(d.error(format_args!(
                "type mismatch for {}: expected table but found {}",
                T::GO_TYPE,
                go_type(other)
            ))),
        }
    }
}

impl FromToml for String {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        match value {
            DeValue::String(s) => {
                *self = s.to_string();
                Ok(())
            }
            other => Err(d.bad_type("string", other)),
        }
    }
}

/// An untagged Go `string` field holding a path (`ConfigFilePath`).
impl FromToml for Option<PathBuf> {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        let mut s = String::new();
        s.decode_value(value, d)?;
        *self = (!s.is_empty()).then(|| PathBuf::from(s));
        Ok(())
    }
}

impl FromToml for bool {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        match value {
            DeValue::Boolean(b) => {
                *self = *b;
                Ok(())
            }
            other => Err(d.bad_type("bool", other)),
        }
    }
}

fn integer(value: &DeValue<'_>, d: &Decoder<'_>) -> Result<i64, DecodeError> {
    match value {
        DeValue::Integer(i) => i64::from_str_radix(i.as_str(), i.radix())
            .map_err(|_| d.error(format_args!("{} is out of range for int64", i.as_str()))),
        other => Err(d.bad_type("integer", other)),
    }
}

macro_rules! integer_from_toml {
    ($($ty:ty => $go:literal),*) => {$(
        impl FromToml for $ty {
            fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
                let n = integer(value, d)?;
                *self = <$ty>::try_from(n).map_err(|_| d.error(format_args!("{n} is out of range for {}", $go)))?;
                Ok(())
            }
        }
    )*};
}

integer_from_toml!(i64 => "int64", i32 => "int32", u64 => "uint64", u32 => "uint32", u16 => "uint16", usize => "uint");

impl FromToml for Vec<String> {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        match value {
            DeValue::Array(items) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items.iter() {
                    let mut s = String::new();
                    s.decode_value(item.get_ref(), d)?;
                    out.push(s);
                }
                *self = out;
                Ok(())
            }
            other => Err(d.bad_type("slice", other)),
        }
    }
}

impl FromToml for Option<Vec<String>> {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        let mut v = Vec::new();
        v.decode_value(value, d)?;
        *self = Some(v);
        Ok(())
    }
}

impl FromToml for HashMap<String, String> {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        match value {
            DeValue::Table(table) => {
                for (key, value) in table.iter() {
                    d.context.push((key.get_ref().to_string(), key.span().start));
                    let mut s = String::new();
                    s.decode_value(value.get_ref(), d)?;
                    d.context.pop();
                    self.insert(key.get_ref().to_string(), s);
                }
                Ok(())
            }
            other => Err(d.bad_type("map", other)),
        }
    }
}

impl FromToml for Option<HashMap<String, String>> {
    fn decode_value(&mut self, value: &DeValue<'_>, d: &mut Decoder<'_>) -> Result<(), DecodeError> {
        self.get_or_insert_with(HashMap::new).decode_value(value, d)
    }
}

// The `[logging]` table of the main configuration file (upstream `logger.Config`
// tags). Declared here so that `logger` itself stays with its own implementation.
crate::toml_section!(crate::logger::Config, "logger.Config" {
    "level" => log_level,
    "format" => log_format,
    "debug" => debug,
});

// ------------------------------------------------------------------ generation

/// Go `%q` of a string (`strconv.Quote`), for error texts that quote a value
/// as Go does. Not always valid TOML (`\a`, `\v`, `\x7f`): configuration files
/// are written with [`toml_quote`].
pub fn go_quote(s: &str) -> String {
    quote(s, Escapes::Go)
}

/// A TOML basic string holding `s`. Wherever Go's `%q` is valid TOML the text is
/// the same: `"` and `\` are escaped, `\b \t \n \f \r` are used for those
/// characters, and characters Go does not print (see [`is_go_printable`]) become
/// `\uXXXX` or `\UXXXXXXXX` (lowercase hex). The escapes Go has and TOML does not
/// are replaced: U+0007 and U+000B, which Go writes `\a` and `\v`, and the other
/// control characters below U+0020 and U+007F, which Go writes `\xXX`, become
/// `\u00XX`. A value with nothing to escape prints as `"value"`.
pub fn toml_quote(s: &str) -> String {
    quote(s, Escapes::Toml)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Escapes {
    Go,
    Toml,
}

fn quote(s: &str, escapes: Escapes) -> String {
    use std::fmt::Write as _;

    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{07}' if escapes == Escapes::Go => out.push_str("\\a"),
            '\u{0b}' if escapes == Escapes::Go => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => match escapes {
                Escapes::Go => write!(out, "\\x{:02x}", c as u32).expect("writing to a String"),
                Escapes::Toml => write!(out, "\\u{:04x}", c as u32).expect("writing to a String"),
            },
            c if is_go_printable(c) => out.push(c),
            c if (c as u32) <= 0xffff => write!(out, "\\u{:04x}", c as u32).expect("writing to a String"),
            c => write!(out, "\\U{:08x}", c as u32).expect("writing to a String"),
        }
    }
    out.push('"');
    out
}

/// Go's `strconv.IsPrint` for every assigned character: letters, marks, numbers,
/// punctuation, symbols and the ASCII space. Not printable outside ASCII: control
/// characters (Cc), separators (Zs, Zl, Zp: `char::is_whitespace` outside ASCII
/// is those and U+0085), format characters (Cf, [`FORMAT_CHARACTERS`]), private
/// use characters (Co) and noncharacters. Other unassigned code points count as
/// printable here, where Go does not print them.
fn is_go_printable(c: char) -> bool {
    if c.is_ascii() {
        return matches!(c, ' '..='~');
    }
    let cp = c as u32;
    let private_use = matches!(cp, 0xe000..=0xf8ff | 0xf_0000..=0xf_fffd | 0x10_0000..=0x10_fffd);
    let noncharacter = matches!(cp, 0xfdd0..=0xfdef) || cp & 0xfffe == 0xfffe;
    !(c.is_control()
        || c.is_whitespace()
        || private_use
        || noncharacter
        || FORMAT_CHARACTERS.iter().any(|&(first, last)| (first..=last).contains(&cp)))
}

/// The format characters (general category Cf) outside ASCII, as ranges: Unicode
/// 15.1 (Python's `unicodedata`), whose format characters are those of 15.0.
const FORMAT_CHARACTERS: [(u32, u32); 21] = [
    (0x00ad, 0x00ad),
    (0x0600, 0x0605),
    (0x061c, 0x061c),
    (0x06dd, 0x06dd),
    (0x070f, 0x070f),
    (0x0890, 0x0891),
    (0x08e2, 0x08e2),
    (0x180e, 0x180e),
    (0x200b, 0x200f),
    (0x202a, 0x202e),
    (0x2060, 0x2064),
    (0x2066, 0x206f),
    (0xfeff, 0xfeff),
    (0xfff9, 0xfffb),
    (0x1_10bd, 0x1_10bd),
    (0x1_10cd, 0x1_10cd),
    (0x1_3430, 0x1_343f),
    (0x1_bca0, 0x1_bca3),
    (0x1_d173, 0x1_d17a),
    (0xe_0001, 0xe_0001),
    (0xe_0020, 0xe_007f),
];

/// A TOML key: bare when it is a bare key (`A-Za-z0-9_-`), otherwise quoted with
/// [`toml_quote`], so that a key containing `.` stays one key.
pub fn toml_key(key: &str) -> String {
    if !key.is_empty() && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        key.to_owned()
    } else {
        toml_quote(key)
    }
}

/// The items of a string list, each quoted with [`toml_quote`] and separated by
/// `, ` (upstream `strings.Join(v, "\", \"")` between quotes).
pub fn string_list_items(items: &[String]) -> String {
    items.iter().map(|item| toml_quote(item)).collect::<Vec<_>>().join(", ")
}

/// `["a", "b"]` as upstream writes string lists (`"[\"" + strings.Join(v, "\", \"") + "\"]"`),
/// with each item quoted with [`toml_quote`]. An empty list is written `[""]`, as
/// upstream writes it.
pub fn string_list(items: &[String]) -> String {
    if items.is_empty() {
        return "[\"\"]".to_owned();
    }
    format!("[{}]", string_list_items(items))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, PartialEq)]
    struct Inner {
        name: String,
    }

    crate::toml_section!(Inner, "test.Inner" { "name" => name });

    #[derive(Debug, Default, PartialEq)]
    struct Outer {
        port: i64,
        rate: i64,
        tags: HashMap<String, String>,
        inner: Inner,
        ptr: Option<Inner>,
        list: Vec<String>,
    }

    crate::toml_section!(Outer, "test.Outer" {
        "port" => port, "Rate" => rate, "tags" => tags, "inner" => inner, "ptr" => ptr, "list" => list,
    });

    #[test]
    fn decodes_over_current_values_and_reports_unknown_keys() {
        let mut o = Outer { port: 1, list: vec!["x".into()], ..Default::default() };
        let unknown = decode(
            &mut o,
            "rate = 5\nlist = [\"a\"]\nzzz = 1\n[inner]\nname = \"n\"\nbad = 2\n[ptr]\nname = \"p\"\n[other]\nk = 1\n",
        )
        .unwrap();
        assert_eq!(o.port, 1);
        assert_eq!(o.rate, 5);
        assert_eq!(o.list, vec!["a".to_owned()]);
        assert_eq!(o.inner.name, "n");
        assert_eq!(o.ptr, Some(Inner { name: "p".into() }));
        assert_eq!(unknown, vec!["zzz", "inner.bad", "other"]);
    }

    #[test]
    fn type_errors_name_the_line_and_key() {
        let mut o = Outer::default();
        let err = decode(&mut o, "\n[inner]\nname = 1\n").unwrap_err();
        assert_eq!(
            err.0,
            "toml: line 3 (last key \"inner.name\"): incompatible types: TOML value has type int64; destination has type string"
        );
        let err = decode(&mut o, "inner = 1\n").unwrap_err();
        assert_eq!(
            err.0,
            "toml: line 1 (last key \"inner\"): type mismatch for test.Inner: expected table but found int64"
        );
    }

    #[test]
    fn implicit_parent_tables_are_not_reported() {
        let mut o = Outer::default();
        let unknown = decode(&mut o, "foo.bar = 1\n[baz.qux]\nx = 1\n[inner.deep]\ny = 2\n").unwrap();
        assert_eq!(unknown, vec!["foo.bar", "baz.qux", "inner.deep"]);
    }

    /// Pins D-012: syntax errors carry the line and the `toml` crate's message.
    #[test]
    fn syntax_errors_name_the_line() {
        let err = decode(&mut Outer::default(), "port = 1\nlist = \n").unwrap_err();
        assert!(err.0.starts_with("toml: line 2: "), "{}", err.0);
    }

    #[test]
    fn toml_quote_decodes_back() {
        for value in [
            "",
            "/etc/ssl/ca.pem",
            "C:\\certs\\ca \"x\".pem",
            "-----BEGIN CERTIFICATE-----\nMIIB\r\n-----END CERTIFICATE-----\n",
            "a\tb\u{1}\u{7f}\u{e9}",
        ] {
            let quoted = toml_quote(value);
            let mut decoded = Inner::default();
            decode(&mut decoded, &format!("name = {quoted}\n")).unwrap();
            assert_eq!(decoded.name, value, "{quoted}");
        }
        assert_eq!(toml_quote("a\"b\\c\n\u{1}"), "\"a\\\"b\\\\c\\n\\u0001\"");
    }

    #[test]
    fn go_quote_escapes() {
        assert_eq!(go_quote("a\"b\\c\n\u{1}"), "\"a\\\"b\\\\c\\n\\x01\"");
    }

    /// `strconv.Quote` output for characters Go escapes, as Go prints them.
    #[test]
    fn go_quote_matches_strconv_quote() {
        for (value, go) in [
            ("\u{7}\u{b}\u{7f}\u{1b}", r#""\a\v\x7f\x1b""#),
            ("\u{8}\u{c}\r\t", r#""\b\f\r\t""#),
            ("\u{85}\u{a0}\u{ad}", r#""\u0085\u00a0\u00ad""#),
            ("\u{200b}\u{2028}\u{3000}\u{feff}", r#""\u200b\u2028\u3000\ufeff""#),
            ("\u{e000}\u{fdd0}\u{10ffff}\u{e0041}", r#""\ue000\ufdd0\U0010ffff\U000e0041""#),
            ("é日本\u{301}\u{1f600}", "\"é日本\u{301}\u{1f600}\""),
        ] {
            assert_eq!(go_quote(value), go, "{value:?}");
        }
    }

    /// Go's `strconv.IsPrint` for characters of each category outside ASCII.
    #[test]
    fn printable_characters_are_those_go_prints() {
        for c in ['é', '日', '\u{301}', '\u{20dd}', '\u{1f3fb}', '\u{ff9e}', '\u{1f600}', '\u{fffd}', '€', '\u{2014}']
        {
            assert!(is_go_printable(c), "{c:?} (U+{:04X}) is printable", c as u32);
        }
        for c in ['\u{80}', '\u{9f}', '\u{a0}', '\u{ad}', '\u{200b}', '\u{2028}', '\u{2029}', '\u{3000}', '\u{feff}']
            .into_iter()
            .chain(['\u{e000}', '\u{f0000}', '\u{fdd0}', '\u{fffe}', '\u{10ffff}', '\u{e0041}', '\u{1680}'])
        {
            assert!(!is_go_printable(c), "U+{:04X} is not printable", c as u32);
        }
        assert!((' '..='~').all(is_go_printable));
        assert!(!('\0'..' ').any(is_go_printable) && !is_go_printable('\u{7f}'));
    }

    /// A value with nothing to escape prints between quotes as is.
    #[test]
    fn toml_quote_keeps_plain_values() {
        for value in ["", "value", "redis://localhost:6379/1", "*.example.com,example.org", "$pubsub", "a#b", "é日本"]
        {
            assert_eq!(toml_quote(value), format!("\"{value}\""));
        }
    }

    /// Pins D-102: escapes TOML has no equivalent for are written `\u00XX`.
    #[test]
    fn deviation_d102_toml_quote_uses_toml_escapes() {
        for (value, quoted) in [
            ("q\"x", r#""q\"x""#),
            ("b\\x", r#""b\\x""#),
            ("n\nx\r\t\u{8}\u{c}", r#""n\nx\r\t\b\f""#),
            ("\u{7}\u{b}\u{0}\u{1b}\u{1f}\u{7f}", r#""\u0007\u000b\u0000\u001b\u001f\u007f""#),
            ("\u{a0}\u{10ffff}", r#""\u00a0\U0010ffff""#),
        ] {
            assert_eq!(toml_quote(value), quoted, "{value:?}");
        }
    }

    fn parse_string(document: &str) -> String {
        let table = DeTable::parse(document).unwrap_or_else(|e| panic!("{document:?}: {e}"));
        match table.get_ref().get("k").map(|v| v.get_ref()) {
            Some(DeValue::String(s)) => s.to_string(),
            other => panic!("{document:?}: {other:?}"),
        }
    }

    /// Pins D-102: every Unicode scalar value quotes to a TOML string that loads
    /// back as itself.
    #[test]
    fn deviation_d102_toml_quote_round_trips_every_character() {
        let all: Vec<char> = ('\0'..=char::MAX).collect();
        for chunk in all.chunks(4096) {
            let value: String = chunk.iter().collect();
            let quoted = toml_quote(&value);
            assert!(!quoted.contains(|c: char| c.is_control()), "only escapes for control characters: {:?}", chunk[0]);
            assert_eq!(parse_string(&format!("k = {quoted}\n")), value, "block from U+{:04X}", chunk[0] as u32);
        }
    }

    /// Where Go's `%q` is valid TOML, the TOML text is Go's.
    #[test]
    fn toml_quote_is_go_quote_where_that_is_valid_toml() {
        for c in ('\0'..='\u{3000}').chain(['\u{feff}', '\u{e000}', '\u{1f600}', '\u{10ffff}']) {
            let value = format!("a{c}b");
            let go = go_quote(&value);
            if go.contains("\\a") || go.contains("\\v") || go.contains("\\x") {
                assert_ne!(toml_quote(&value), go);
            } else {
                assert_eq!(toml_quote(&value), go, "U+{:04X}", c as u32);
            }
        }
    }

    /// Pins D-102: keys that are not bare keys are quoted, and stay one key.
    #[test]
    fn deviation_d102_toml_keys_are_bare_or_quoted() {
        assert_eq!(toml_key("env"), "env");
        assert_eq!(toml_key("region-1_a"), "region-1_a");
        assert_eq!(toml_key("a b"), r#""a b""#);
        assert_eq!(toml_key("a.b"), r#""a.b""#);
        assert_eq!(toml_key(""), r#""""#);
        assert_eq!(toml_key("é"), r#""é""#);
        let document = format!("t.{} = 1\n", toml_key("a.b"));
        let table = DeTable::parse(&document).unwrap();
        let Some(DeValue::Table(t)) = table.get_ref().get("t").map(|v| v.get_ref()) else { panic!("{document:?}") };
        assert!(t.get("a.b").is_some(), "one key");
    }

    /// Lists as upstream writes them, with each item quoted.
    #[test]
    fn string_lists_quote_each_item() {
        let list = |items: &[&str]| string_list(&items.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>());
        assert_eq!(list(&["http", "redis"]), r#"["http", "redis"]"#);
        assert_eq!(list(&["a,b", "q\"x"]), r#"["a,b", "q\"x"]"#);
        assert_eq!(list(&[]), r#"[""]"#, "an empty list as upstream writes it");
    }
}
