//! upstream file: vendorlib/go-mruby/value_test.go

use cabby_testkit::reference::mruby::*;

/// upstream: vendorlib/go-mruby/value_test.go#TestExceptionString_afterClose
///
/// Runtime adaptation (cgo lifetime -> owned exception data): upstream guards a
/// fix where `err.Error()` read the closed VM and panicked. The Rust invariant:
/// an exception's text is owned data, readable after its VM is closed. As in Go,
/// LoadString must have failed (Go's `err.Error()` on a nil error panics).
#[test]
fn test_exception_string_after_close() {
    let mrb = Mrb::new();
    let err = mrb.load_string("clearly a syntax error").expect_err("syntax error expected");
    mrb.close();
    // This panics before the bug fix that this test tests
    let _ = err.error();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestExceptionBacktrace
#[test]
fn test_exception_backtrace() {
    let mrb = Mrb::new();

    let mut parser = Parser::new(&mrb);
    let mut context = CompileContext::new(&mrb);
    context.set_filename("hello.rb");

    // Upstream ignores Parse's result.
    let _ = parser.parse(
        r#"
				def do_error
					raise "Exception"
				end

				def hop1
					do_error
				end

				def hop2
					hop1
				end

				hop2
			"#,
        Some(&context),
    );

    let proc = parser.generate_code();
    let exc = mrb.run(&proc.into(), None).expect_err("expected exception");

    assert_eq!(exc.message, "Exception", "bad exception message");

    assert_eq!(exc.file, "hello.rb", "bad file");

    assert_eq!(exc.line, 3, "bad line");

    assert_eq!(exc.backtrace.len(), 4, "bad backtrace: {:?}", exc.backtrace);

    context.close();
    parser.close();
    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueCall
#[test]
fn test_mrb_value_call() {
    let mrb = Mrb::new();

    let value = mrb.load_string(r#""foo""#).unwrap_or_else(|e| panic!("err: {}", e.error()));

    assert!(value.call("some_function_that_doesnt_exist", &[]).is_err(), "expected exception");

    let result = value.call("==", &[Value::String("foo".into())]).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(result.value_type(), ValueType::True, "bad type");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueCallBlock
#[test]
fn test_mrb_value_call_block() {
    let mrb = Mrb::new();

    let value = mrb.load_string(r#""foo""#).unwrap_or_else(|e| panic!("err: {}", e.error()));

    let block = mrb.load_string(r#"Proc.new { |_| "bar" }"#).unwrap_or_else(|e| panic!("err: {}", e.error()));

    let result = value
        .call_block("gsub", &[Value::String("foo".into())], &block.into())
        .unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(result.value_type(), ValueType::String, "bad type");
    assert_eq!(result.string(), "bar", "bad");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueValue
///
/// Runtime adaptation (Go pointer identity -> value identity): upstream checks
/// `falseV.MrbValue(mrb) == falseV`, i.e. converting an `*MrbValue` to a value
/// returns the very same pointer. The Rust invariant: `mrb_value` of a value
/// returns a handle to the same value (`same`). The Go run compares the actual
/// pointers (bridge handles are interned by pointer).
#[test]
fn test_mrb_value_value() {
    let mrb = Mrb::new();

    let false_v = mrb.false_value();
    assert!(false_v.mrb_value(&mrb).same(&false_v), "should be the same");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueValue_impl
///
/// Runtime adaptation (Go interface satisfaction -> conversion): upstream only
/// checks at compile time that `*MrbValue` implements `Value`
/// (`var _ Value = mrb.FalseValue()`). The Rust invariant: an `MrbValue`
/// converts into a `Value` that denotes the same value.
#[test]
fn test_mrb_value_value_impl() {
    let mrb = Mrb::new();

    let false_v = mrb.false_value();
    let value: Value = false_v.clone().into();
    assert!(value.mrb_value(&mrb).same(&false_v), "Value must denote the same value");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueFixnum
#[test]
fn test_mrb_value_fixnum() {
    let mrb = Mrb::new();

    let value = mrb.load_string("42").unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.fixnum(), 42, "bad fixnum");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueString
#[test]
fn test_mrb_value_string() {
    let mrb = Mrb::new();

    let value = mrb.load_string(r#""foo""#).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), "foo", "bad string");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestMrbValueType
#[test]
fn test_mrb_value_type() {
    let mrb = Mrb::new();

    let cases: &[(&str, ValueType)] = &[
        ("false", ValueType::False),
        // TypeFree - Type of value after GC collection
        ("true", ValueType::True),
        ("1", ValueType::Fixnum),
        (":test", ValueType::Symbol),
        // TypeUndef - Internal value used by mruby for undefined things (instance vars etc)
        // These all seem to get converted to exceptions before hitting userland
        ("1.1", ValueType::Float),
        // TypeCptr
        ("Object.new", ValueType::Object),
        ("Object", ValueType::Class),
        ("module T; end; T", ValueType::Module),
        // TypeIClass
        // TypeSClass
        ("Proc.new { 1 }", ValueType::Proc),
        ("[]", ValueType::Array),
        ("{}", ValueType::Hash),
        (r#""string""#, ValueType::String),
        ("1..2", ValueType::Range),
        ("Exception.new", ValueType::Exception),
        // TypeFile
        // TypeEnv
        // TypeData
        // TypeFiber
        // TypeMaxDefine
        ("nil", ValueType::Nil),
    ];

    for (input, expected) in cases {
        let r =
            mrb.load_string(input).unwrap_or_else(|e| panic!("loadstring failed for case {input:?}: {}", e.error()));
        let c_type = r.value_type();
        assert_eq!(c_type, *expected, "bad type: got {c_type:?}, expected {expected:?}");
    }

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestIntMrbValue
#[test]
fn test_int_mrb_value() {
    let mrb = Mrb::new();

    let value = Value::Int(42);
    let v = value.mrb_value(&mrb);
    assert_eq!(v.fixnum(), 42, "bad value");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestStringMrbValue
#[test]
fn test_string_mrb_value() {
    let mrb = Mrb::new();

    let value = Value::String("foo".into());
    let v = value.mrb_value(&mrb);
    assert_eq!(v.string(), "foo", "bad value");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestValueClass
///
/// Runtime adaptation (Go `reflect.DeepEqual` -> class equality): upstream
/// compares the two `*Class` structs deeply (same `RClass` pointer and VM). The
/// Rust invariant is `Class: PartialEq`, true when both denote the same class.
/// The Go run uses `reflect.DeepEqual` itself.
#[test]
fn test_value_class() {
    let mrb = Mrb::new();

    let val = mrb
        .object_class()
        .new_instance(&[])
        .unwrap_or_else(|e| panic!("Error constructing object of type Object: {}", e.error()));

    assert!(val.class() == mrb.object_class(), "Class of value was not equivalent to constructed class");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/value_test.go#TestValueSingletonClass
#[test]
fn test_value_singleton_class() {
    let mrb = Mrb::new();

    let f = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
        let args = m.get_args();
        Ok(Some(Value::Int(args[0].fixnum() + args[1].fixnum())))
    };

    mrb.top_self().singleton_class().define_method("add", f, ArgSpec::req(2));

    let result = mrb.load_string("add(46, 2)").unwrap_or_else(|e| panic!("Error parsing ruby code: {}", e.error()));

    assert_eq!(result.string(), "48", "Result was not equal to the target value of 48");

    mrb.close();
}
