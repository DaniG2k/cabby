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

/// Go `%q` of a string (`strconv.Quote`).
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{07}' => out.push_str("\\a"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0b}' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\u{7f}' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if c.is_control() => {
                if (c as u32) <= 0xffff {
                    out.push_str(&format!("\\u{:04x}", c as u32));
                } else {
                    out.push_str(&format!("\\U{:08x}", c as u32));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `["a", "b"]` as upstream writes string lists (`"[\"" + strings.Join(v, "\", \"") + "\"]"`).
pub fn string_list(items: &[String]) -> String {
    format!("[\"{}\"]", items.join("\", \""))
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

    /// Pins D-006: syntax errors carry the line and the `toml` crate's message.
    #[test]
    fn syntax_errors_name_the_line() {
        let err = decode(&mut Outer::default(), "port = 1\nlist = \n").unwrap_err();
        assert!(err.0.starts_with("toml: line 2: "), "{}", err.0);
    }

    #[test]
    fn go_quote_escapes() {
        assert_eq!(go_quote("a\"b\\c\n\u{1}"), "\"a\\\"b\\\\c\\n\\x01\"");
    }
}
