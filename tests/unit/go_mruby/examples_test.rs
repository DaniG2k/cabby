//! upstream file: vendorlib/go-mruby/examples_test.go
//!
//! Go examples pass when their printed output matches the `// Output:` comment.
//! The translations build the same output string and compare it.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use cabby_testkit::reference::mruby::*;

/// upstream: vendorlib/go-mruby/examples_test.go#Example_CustomFunction
/// note: runtime adaptation (Go example // Output: check -> string comparison): the printed line "Result: 42\n" is built with format! and compared.
#[test]
fn example_custom_function() {
    let mrb = Mrb::new();

    // Our custom function we'll expose to Ruby
    let add_func = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
        let args = m.get_args();
        Ok(Some(Value::Int(args[0].fixnum() + args[1].fixnum())))
    };

    // Lets define a custom class and a class method we can call.
    let class = mrb.define_class("Example", None);
    class.define_class_method("add", add_func, ArgSpec::req(2));

    // Let's call it and inspect the result
    let result = mrb.load_string("Example.add(12, 30)").unwrap_or_else(|e| panic!("{}", e.error()));

    let output = format!("Result: {}\n", result.string());
    // Output:
    // Result: 42
    assert_eq!(output, "Result: 42\n");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/examples_test.go#Example_Decode
/// note: runtime adaptation (Go fmt %v rendering -> structural equality): Go prints the decoded interface{} as "Result: map[foo:bar]", which is Go map formatting, not mruby behavior; the Rust invariant is that the value decoded inside the callback equals the map {"foo" => "bar"}.
///
/// Runtime adaptation (Go `fmt` rendering -> structural equality): Go prints the
/// decoded `interface{}` with `%v`, giving `Result: map[foo:bar]`. That text is
/// Go's map formatting, not mruby behavior; the Rust invariant is that the value
/// decoded inside the callback is the map `{"foo" => "bar"}`.
#[test]
fn example_decode() {
    let mrb = Mrb::new();

    // Our custom function we'll expose to Ruby
    let log_data: Rc<RefCell<Option<Decoded>>> = Rc::default();
    let log_func = {
        let log_data = Rc::clone(&log_data);
        move |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
            let args = m.get_args();
            let decoded = decode_value(&args[0]).unwrap_or_else(|e| panic!("{e}"));
            *log_data.borrow_mut() = Some(decoded);
            Ok(None)
        }
    };

    // Lets define a custom class and a class method we can call.
    let class = mrb.define_class("Example", None);
    class.define_class_method("log", log_func, ArgSpec::req(1));

    // Let's call it and inspect the result
    if let Err(e) = mrb.load_string(r#"Example.log({"foo" => "bar"})"#) {
        panic!("{}", e.error());
    }

    // Output:
    // Result: map[foo:bar]
    let expected = Decoded::Map(BTreeMap::from([("foo".to_owned(), Decoded::String("bar".into()))]));
    assert_eq!(log_data.borrow().as_ref(), Some(&expected));

    mrb.close();
}

/// upstream: vendorlib/go-mruby/examples_test.go#Example_SimulateFiles
/// note: runtime adaptation (Go example // Output: check -> string comparison): the printed line "Result: 42\n" is built with format! and compared.
#[test]
fn example_simulate_files() {
    let mrb = Mrb::new();

    let mut ctx1 = CompileContext::new(&mrb);
    ctx1.set_filename("foo.rb");

    let mut ctx2 = CompileContext::new(&mrb);
    ctx2.set_filename("bar.rb");

    let mut parser = Parser::new(&mrb);

    if let Err(e) = parser.parse("def foo; bar; end", Some(&ctx1)) {
        panic!("{}", e.error());
    }
    let code1 = parser.generate_code();

    if let Err(e) = parser.parse("def bar; 42; end", Some(&ctx2)) {
        panic!("{}", e.error());
    }
    let code2 = parser.generate_code();

    if let Err(e) = mrb.run(&code1.into(), None) {
        panic!("{}", e.error());
    }

    if let Err(e) = mrb.run(&code2.into(), None) {
        panic!("{}", e.error());
    }

    let result = mrb.load_string("foo").unwrap_or_else(|e| panic!("{}", e.error()));

    let output = format!("Result: {result}\n");
    // Output:
    // Result: 42
    assert_eq!(output, "Result: 42\n");

    parser.close();
    ctx2.close();
    ctx1.close();
    mrb.close();
}
