//! upstream file: vendorlib/go-mruby/hash_test.go

use cabby_testkit::reference::mruby::*;

fn s(v: &str) -> Value {
    Value::String(v.to_owned())
}

/// upstream: vendorlib/go-mruby/hash_test.go#TestHash
#[test]
fn test_hash() {
    let mrb = Mrb::new();

    let value = mrb.load_string(r#"{"foo" => "bar", "baz" => false}"#).unwrap_or_else(|e| panic!("err: {}", e.error()));

    let h = value.hash();

    // Get
    let value = h.get(&s("foo")).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), "bar", "bad");

    // Get false type
    let value = h.get(&s("baz")).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.value_type(), ValueType::False, "bad type");
    assert_eq!(value.string(), "false", "bad");

    // Set
    if let Err(e) = h.set(&s("foo"), &s("baz")) {
        panic!("err: {}", e.error());
    }
    let value = h.get(&s("foo")).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), "baz", "bad");

    // Keys
    let value = h.keys().unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.value_type(), ValueType::Array, "bad");
    assert_eq!(value.string(), r#"["foo", "baz"]"#, "bad");

    // Delete
    let value = h.delete(&s("foo")).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.expect("deleted value").string(), "baz", "bad");

    let value = h.keys().unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), r#"["baz"]"#, "bad");

    // Delete non-existing: Go returns a nil *MrbValue.
    let value = h.delete(&s("nope")).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert!(value.is_none(), "bad: {value:?}");

    mrb.close();
}
