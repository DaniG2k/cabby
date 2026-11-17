//! upstream file: vendorlib/go-mruby/array_test.go

use cabby_testkit::reference::mruby::*;

/// upstream: vendorlib/go-mruby/array_test.go#TestArray
#[test]
fn test_array() {
    let mrb = Mrb::new();

    let value = mrb.load_string(r#"["foo", "bar", "baz", false]"#).unwrap_or_else(|e| panic!("err: {}", e.error()));

    let v = value.array();

    // Len
    assert_eq!(v.len(), 4, "bad");

    // Get. Go returns a nil *MrbValue for nil elements; here `None`.
    let value = v.get(1).unwrap_or_else(|e| panic!("err: {}", e.error())).expect("non-nil element");
    assert_eq!(value.string(), "bar", "bad");

    // Get bool
    let value = v.get(3).unwrap_or_else(|e| panic!("err: {}", e.error())).expect("non-nil element");
    assert_eq!(value.value_type(), ValueType::False, "bad type");
    assert_eq!(value.string(), "false", "bad");

    mrb.close();
}
