//! upstream file: vendorlib/go-mruby/class_test.go

use cabby_testkit::reference::mruby::*;

use crate::func_test::{test_callback, test_callback_exception, test_callback_result};

/// upstream: vendorlib/go-mruby/class_test.go#TestClassDefineClassMethod
#[test]
fn test_class_define_class_method() {
    let mrb = Mrb::new();

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_class_method("foo", test_callback, ArgSpec::none());
    let value = mrb.load_string("Hello.foo").unwrap_or_else(|e| panic!("err: {}", e.error()));

    test_callback_result(&value);
    mrb.close();
}

/// upstream: vendorlib/go-mruby/class_test.go#TestClassDefineConst
#[test]
fn test_class_define_const() {
    let mrb = Mrb::new();

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_const("FOO", &Value::String("bar".into()));
    let value = mrb.load_string("Hello::FOO").unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), "bar", "bad");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/class_test.go#TestClassDefineMethod
#[test]
fn test_class_define_method() {
    let mrb = Mrb::new();

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_method("foo", test_callback, ArgSpec::none());
    let value = mrb.load_string("Hello.new.foo").unwrap_or_else(|e| panic!("err: {}", e.error()));

    test_callback_result(&value);
    mrb.close();
}

/// upstream: vendorlib/go-mruby/class_test.go#TestClassNew
#[test]
fn test_class_new() {
    let mrb = Mrb::new();

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_method("foo", test_callback, ArgSpec::none());

    let instance = class.new_instance(&[]).unwrap_or_else(|e| panic!("err: {}", e.error()));

    let value = instance.call("foo", &[]).unwrap_or_else(|e| panic!("err: {}", e.error()));

    test_callback_result(&value);
    mrb.close();
}

/// upstream: vendorlib/go-mruby/class_test.go#TestClassNewException
#[test]
fn test_class_new_exception() {
    let mrb = Mrb::new();

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_method("initialize", test_callback_exception, ArgSpec::none());

    assert!(class.new_instance(&[]).is_err(), "expected exception");

    // Verify exception is cleared
    let val = mrb.load_string(r#""test""#).unwrap_or_else(|e| panic!("unexpected exception: {}", e.error()));

    assert_eq!(val.string(), "test", "expected val 'test'");
    mrb.close();
}

/// upstream: vendorlib/go-mruby/class_test.go#TestClassValue
#[test]
fn test_class_value() {
    let mrb = Mrb::new();

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    let value = class.mrb_value(&mrb);
    assert_eq!(value.value_type(), ValueType::Class, "bad");

    mrb.close();
}
