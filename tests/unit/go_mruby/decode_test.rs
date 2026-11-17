//! upstream file: vendorlib/go-mruby/decode_test.go
//!
//! Runtime adaptation (Go reflection -> serde): Go's `Decode(out interface{}, v)`
//! fills whatever `out` points to by reflection. The Rust equivalent decodes into
//! a `serde::Deserialize` target type (`decode::<T>`), or into the dynamic
//! [`Decoded`] tree where Go decodes into `interface{}` (`decode_value`). Each row
//! names the Go target type; the Rust target is the type with the same meaning
//! (`int` -> `i64`, `*int` -> `Box<i64>`, `map[string]string` -> `HashMap`,
//! struct with field `Foo` -> struct with serde field `foo`, since Go matches the
//! lower-cased field name). Go reuses one output variable for several rows; every
//! reused variable is fully overwritten by its next row (bool, int, string,
//! struct), so a fresh target per row is equivalent.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use cabby_testkit::reference::mruby::*;
use serde::Deserialize;

/// `type structString struct { Foo string }`.
#[derive(Debug, PartialEq, Deserialize)]
struct StructString {
    foo: String,
}

impl DecodeTarget for StructString {
    const GO_TYPE: &'static str = "structString";
}

const TEST_DECODE_OBJECT_METHODS: &str = r#"
class Foo
	def foo
		"bar"
	end
end

Foo.new
"#;

/// One TestDecode row: a fresh VM, LoadString, Decode, Close, then compare (the
/// decoded value must outlive the VM, as in Go).
fn decode_case<T: DecodeTarget + PartialEq + Debug>(input: &str, expected: T) {
    let mrb = Mrb::new();
    let value = match mrb.load_string(input) {
        Ok(v) => v,
        Err(e) => {
            mrb.close();
            panic!("err: {}\n\n{input}", e.error());
        }
    };

    let result = decode::<T>(&value);
    mrb.close();
    let actual = result.unwrap_or_else(|e| panic!("err: {e}"));

    assert_eq!(actual, expected, "bad: {actual:?}\n\n{expected:?}");
}

fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())).collect()
}

/// upstream: vendorlib/go-mruby/decode_test.go#TestDecode
/// note: runtime adaptation (Go reflection -> serde): Decode(out interface{}, v) fills `out` by reflection; Rust decodes into a serde Deserialize target (int->i64, *int->Box<i64>, map[string]string->HashMap, struct field Foo->serde field foo since Go matches the lower-cased name). Go reuses output variables across rows; each is fully overwritten, so a fresh target per row is equivalent. Go verifies the same rows via the bridge decoding into the named Go type.
#[test]
fn test_decode() {
    // Booleans
    decode_case("true", true);
    decode_case("false", false);

    // Float (go-mruby reads floats through a C `float`)
    decode_case("1.2", 1.2000000476837158_f64);

    // Int
    decode_case("32", 32_i64);
    decode_case(r#""32""#, 32_i64);

    // Map
    decode_case(r#"{"foo" => "bar"}"#, map(&[("foo", "bar")]));
    decode_case(r#"{32 => "bar"}"#, map(&[("32", "bar")]));

    // Slice
    decode_case(r#"["foo", "bar"]"#, vec!["foo".to_owned(), "bar".to_owned()]);

    // Ptr: Go compares the pointee (`reflect.Indirect`) with 32.
    decode_case("32", Box::new(32_i64));

    // String
    decode_case("32", "32".to_owned());
    decode_case(r#""32""#, "32".to_owned());

    // Struct from Hash
    decode_case(r#"{"foo" => "bar"}"#, StructString { foo: "bar".into() });

    // Struct from object with methods
    decode_case(TEST_DECODE_OBJECT_METHODS, StructString { foo: "bar".into() });
}

fn decode_interface_case(input: &str, expected: Decoded) {
    let mrb = Mrb::new();
    let value = match mrb.load_string(input) {
        Ok(v) => v,
        Err(e) => {
            mrb.close();
            panic!("err: {}\n\n{input}", e.error());
        }
    };

    let result = decode_value(&value);
    mrb.close();
    let result = result.unwrap_or_else(|e| panic!("err: {e}"));

    assert_eq!(result, expected, "bad: \n\n{input}\n\n{result:?}\n\n{expected:?}");
}

fn string(s: &str) -> Decoded {
    Decoded::String(s.to_owned())
}

/// upstream: vendorlib/go-mruby/decode_test.go#TestDecodeInterface
/// note: runtime adaptation (Go reflection -> dynamic tree): decoding into interface{} maps to decode_value returning Decoded (Bool/Int/Float/String/Map/Slice); the bridge tags Go dynamic types so int and float stay distinct.
#[test]
fn test_decode_interface() {
    // Booleans
    decode_interface_case("true", Decoded::Bool(true));
    decode_interface_case("false", Decoded::Bool(false));

    // Float
    decode_interface_case("1.2", Decoded::Float(1.2000000476837158));

    // Int
    decode_interface_case("32", Decoded::Int(32));

    // Map
    decode_interface_case(r#"{"foo" => "bar"}"#, Decoded::Map(BTreeMap::from([("foo".to_owned(), string("bar"))])));
    decode_interface_case(r#"{32 => "bar"}"#, Decoded::Map(BTreeMap::from([("32".to_owned(), string("bar"))])));

    // Slice
    decode_interface_case(r#"["foo", "bar"]"#, Decoded::Slice(vec![string("foo"), string("bar")]));

    // String
    decode_interface_case(r#""32""#, string("32"));
}
