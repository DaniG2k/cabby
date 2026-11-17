//! upstream file: vendorlib/go-mruby/func_test.go (helpers only; it has no cases)

use cabby_testkit::reference::mruby::*;

/// `testCallback`: returns `Int(42)`.
pub fn test_callback(_m: &Mrb, _self: &MrbValue) -> Result<Option<Value>, Value> {
    Ok(Some(Value::Int(42)))
}

/// `testCallbackResult`.
pub fn test_callback_result(v: &MrbValue) {
    assert_eq!(v.value_type(), ValueType::Fixnum, "bad type");
    assert_eq!(v.fixnum(), 42, "bad");
}

/// `testCallbackException`: raises from Ruby and returns the exception value.
/// Go's `e.(*Exception)` type assertion panics without an error; so does this.
pub fn test_callback_exception(m: &Mrb, _self: &MrbValue) -> Result<Option<Value>, Value> {
    let e = m.load_string("raise 'Exception'").expect_err("LoadString should raise");
    Err(Value::Mrb(e.value))
}
