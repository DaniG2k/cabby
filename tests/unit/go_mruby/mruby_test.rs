//! upstream file: vendorlib/go-mruby/mruby_test.go

use std::cell::RefCell;
use std::rc::Rc;

use cabby_testkit::reference::mruby::*;

use crate::join;

/// upstream: vendorlib/go-mruby/mruby_test.go#TestNewMrb
#[test]
fn test_new_mrb() {
    let mrb = Mrb::new();
    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbArena
#[test]
fn test_mrb_arena() {
    let mrb = Mrb::new();

    let idx = mrb.arena_save();
    mrb.arena_restore(idx);

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbModule
#[test]
fn test_mrb_module() {
    let mrb = Mrb::new();

    let module = mrb.module("Kernel");
    assert!(module.is_some(), "module was nil and should not be");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbClass
#[test]
fn test_mrb_class() {
    let mrb = Mrb::new();

    let class = mrb.class("Object", None);
    assert!(class.is_some(), "class should not be nil");

    mrb.define_class("Hello", Some(&mrb.object_class()));
    let class = mrb.class("Hello", Some(&mrb.object_class()));
    assert!(class.is_some(), "class should not be nil");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbConstDefined
#[test]
fn test_mrb_const_defined() {
    let mrb = Mrb::new();

    assert!(mrb.const_defined("Object", &mrb.object_class().into()), "Object should be defined");

    mrb.define_class("Hello", Some(&mrb.object_class()));
    assert!(mrb.const_defined("Hello", &mrb.object_class().into()), "Hello should be defined");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbDefineClass
#[test]
fn test_mrb_define_class() {
    let mrb = Mrb::new();

    mrb.define_class("Hello", Some(&mrb.object_class()));
    if let Err(e) = mrb.load_string("Hello") {
        panic!("err: {}", e.error());
    }

    mrb.define_class("World", None);
    if let Err(e) = mrb.load_string("World") {
        panic!("err: {}", e.error());
    }

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbDefineClass_methodException
#[test]
fn test_mrb_define_class_method_exception() {
    let mrb = Mrb::new();

    let cb = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
        match m.load_string(r#"raise "exception""#) {
            Err(exc) => Err(Value::Mrb(exc.value)),
            Ok(v) => Ok(Some(Value::Mrb(v))),
        }
    };

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_class_method("foo", cb, ArgSpec::none());
    assert!(mrb.load_string("Hello.foo").is_err(), "should error");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbDefineClassUnder
#[test]
fn test_mrb_define_class_under() {
    let mrb = Mrb::new();

    // Define an outer
    let hello = mrb.define_class("Hello", Some(&mrb.object_class()));
    if let Err(e) = mrb.load_string("Hello") {
        panic!("err: {}", e.error());
    }

    // Inner
    mrb.define_class_under("World", None, Some(&hello));
    if let Err(e) = mrb.load_string("Hello::World") {
        panic!("err: {}", e.error());
    }

    // Inner defaults
    mrb.define_class_under("Another", None, None);
    if let Err(e) = mrb.load_string("Another") {
        panic!("err: {}", e.error());
    }

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbDefineModule
#[test]
fn test_mrb_define_module() {
    let mrb = Mrb::new();

    mrb.define_module("Hello");
    if let Err(e) = mrb.load_string("Hello") {
        panic!("err: {}", e.error());
    }

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbDefineModuleUnder
#[test]
fn test_mrb_define_module_under() {
    let mrb = Mrb::new();

    // Define an outer
    let hello = mrb.define_module("Hello");
    if let Err(e) = mrb.load_string("Hello") {
        panic!("err: {}", e.error());
    }

    // Inner
    mrb.define_module_under("World", Some(&hello));
    if let Err(e) = mrb.load_string("Hello::World") {
        panic!("err: {}", e.error());
    }

    // Inner defaults
    mrb.define_module_under("Another", None);
    if let Err(e) = mrb.load_string("Another") {
        panic!("err: {}", e.error());
    }

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbFixnumValue
#[test]
fn test_mrb_fixnum_value() {
    let mrb = Mrb::new();

    let value = mrb.fixnum_value(42);
    assert_eq!(value.value_type(), ValueType::Fixnum, "should be fixnum");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbFullGC
#[test]
fn test_mrb_full_gc() {
    let mrb = Mrb::new();

    let ai = mrb.arena_save();
    let value = mrb.string_value("foo");
    assert!(!value.is_dead(), "should not be dead");

    mrb.arena_restore(ai);
    mrb.full_gc();
    assert!(value.is_dead(), "should be dead");

    mrb.close();
}

struct GetArgsCase {
    args: &'static str,
    types: &'static [ValueType],
    result: Option<&'static [&'static str]>,
}

const GET_ARGS_CASES: &[GetArgsCase] = &[
    GetArgsCase { args: r#"("foo")"#, types: &[ValueType::String], result: Some(&[r#""foo""#]) },
    GetArgsCase { args: "(true)", types: &[ValueType::True], result: Some(&["true"]) },
    GetArgsCase { args: "(Hello)", types: &[ValueType::Class], result: Some(&["Hello"]) },
    GetArgsCase { args: "() { }", types: &[ValueType::Proc], result: None },
    GetArgsCase {
        args: r#"(Hello, "bar", true)"#,
        types: &[ValueType::Class, ValueType::String, ValueType::True],
        result: Some(&["Hello", r#""bar""#, "true"]),
    },
    GetArgsCase {
        args: r#"("bar", true) {}"#,
        types: &[ValueType::String, ValueType::True, ValueType::Proc],
        result: None,
    },
];

/// One TestMrbGetArgs goroutine body: its own VM, a class method that captures
/// its arguments, and the type/inspect checks.
fn get_args_case(tc: &GetArgsCase) -> Result<(), String> {
    let actual: Rc<RefCell<Vec<MrbValue>>> = Rc::default();
    let test_func = {
        let actual = Rc::clone(&actual);
        move |m: &Mrb, this: &MrbValue| -> Result<Option<Value>, Value> {
            *actual.borrow_mut() = m.get_args();
            Ok(Some(Value::Mrb(this.clone())))
        }
    };

    let mrb = Mrb::new();
    let checked = (|| {
        let class = mrb.define_class("Hello", Some(&mrb.object_class()));
        class.define_class_method("test", test_func, ArgSpec::any());
        mrb.load_string(&format!("Hello.test{}", tc.args)).map_err(|e| format!("err: {}", e.error()))?;

        let actual = actual.borrow().clone();
        if let Some(result) = tc.result
            && actual.len() != result.len()
        {
            return Err(format!("{}: expected {}, got {}", tc.args, result.len(), actual.len()));
        }

        let mut actual_strings = Vec::with_capacity(actual.len());
        let mut actual_types = Vec::with_capacity(actual.len());
        for v in &actual {
            let s = v.call("inspect", &[]).map_err(|e| e.error())?;
            actual_strings.push(s.string());
            actual_types.push(v.value_type());
        }

        if actual_types != tc.types {
            return Err(format!("code: {}\nexpected: {:?}\nactual: {actual_types:?}", tc.args, tc.types));
        }

        if let Some(result) = tc.result
            && actual_strings != result
        {
            return Err(format!("expected: {result:?}\nactual: {actual_strings:?}"));
        }
        Ok(())
    })();
    mrb.close();
    checked
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbGetArgs
///
/// Runtime adaptation (goroutines -> threads): upstream runs every case on its
/// own goroutine with its own VM, 1000 rounds, to exercise go-mruby's global
/// argument accumulator under concurrency. The Rust invariant: independent VMs
/// on separate OS threads run concurrently without shared state and each
/// method sees exactly its own arguments. Each round spawns one thread per case
/// and joins them all before the next round (Go waits for one result per case).
#[test]
fn test_mrb_get_args() {
    // lots of this effort is centered around testing multithreaded behavior.
    for _ in 0..1000 {
        let handles: Vec<_> = GET_ARGS_CASES.iter().map(|tc| std::thread::spawn(move || get_args_case(tc))).collect();
        for h in handles {
            if let Err(e) = join(h) {
                panic!("{e}");
            }
        }
    }
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbGlobalVariable
#[test]
fn test_mrb_global_variable() {
    const TEST_VALUE: &str = "HELLO";
    let mrb = Mrb::new();
    if let Err(e) = mrb.load_string(&format!(r#"$a = "{TEST_VALUE}""#)) {
        panic!("err: {}", e.error());
    }
    let value = mrb.get_global_variable("$a");
    assert_eq!(value.string(), TEST_VALUE, "wrong value for $a");
    mrb.set_global_variable("$b", &mrb.string_value(TEST_VALUE).into());
    let value = mrb.load_string("$b").unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), TEST_VALUE, "wrong value for $b");
    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbInstanceVariable
#[test]
fn test_mrb_instance_variable() {
    const GOLDEN_RETRIEVER: &str = "golden retriever";
    const HUSKY: &str = "Husky";
    let mrb = Mrb::new();
    let loaded = mrb.load_string(
        r#"
		class Dog
			def initialize(breed)
				@breed = breed
			end
			def breed
				"cocker spaniel" # this line exists to ensure that it's not invoking the accessor method
			end
			def real_breed
				@breed
			end
		end
	"#,
    );
    if let Err(e) = loaded {
        panic!("err: {}", e.error());
    }
    let dog_class = mrb.class("Dog", None).expect("dog class not found");
    let inst = dog_class
        .new_instance(&[mrb.string_value(GOLDEN_RETRIEVER).into()])
        .unwrap_or_else(|e| panic!("err: {}", e.error()));
    let value = inst.get_instance_variable("@breed");
    assert_eq!(value.string(), GOLDEN_RETRIEVER, "wrong value for Dog.@breed");
    inst.set_instance_variable("@breed", &mrb.string_value(HUSKY));
    let value = inst.get_instance_variable("@breed");
    assert_eq!(value.string(), HUSKY, "wrong value for Dog.@breed");
    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbLoadString
#[test]
fn test_mrb_load_string() {
    let mrb = Mrb::new();

    // Go also checks `value == nil`; a successful Result always carries a value.
    let value = mrb.load_string(r#""HELLO""#);
    assert!(value.is_ok(), "err: {:?}", value.err().map(|e| e.error()));

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbLoadString_twice
#[test]
fn test_mrb_load_string_twice() {
    let mrb = Mrb::new();

    let value = mrb.load_string(r#""HELLO""#);
    assert!(value.is_ok(), "err: {:?}", value.err().map(|e| e.error()));

    let value = mrb.load_string(r#""WORLD""#).unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.string(), "WORLD", "bad");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbLoadStringException
#[test]
fn test_mrb_load_string_exception() {
    let mrb = Mrb::new();

    assert!(mrb.load_string(r#"raise "An exception""#).is_err(), "exception expected");

    let value = mrb.load_string(r#""test""#).unwrap_or_else(|_| panic!("exception should have been cleared"));

    assert_eq!(value.string(), "test", "bad test value returned");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbRaise
#[test]
fn test_mrb_raise() {
    let mrb = Mrb::new();

    let cb = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> { Err(Value::Mrb(m.get_args()[0].clone())) };

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_class_method("foo", cb, ArgSpec::req(1));
    let err = mrb.load_string(r#"Hello.foo(ArgumentError.new("ouch"))"#).expect_err("should have error");
    assert_eq!(err.error(), "ouch", "bad");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbYield
///
/// Upstream calls `t.Fatalf` inside the callback when Yield fails. The callback
/// records the failure instead (a panic must not cross back into the VM) and the
/// test fails on it right after `LoadString`.
#[test]
fn test_mrb_yield() {
    let mrb = Mrb::new();

    let failure: Rc<RefCell<Option<String>>> = Rc::default();
    let cb = {
        let failure = Rc::clone(&failure);
        move |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
            match m.yield_block(&m.get_args()[0].clone().into(), &[Value::Int(12), Value::Int(30)]) {
                Ok(result) => Ok(Some(result.into())),
                Err(e) => {
                    *failure.borrow_mut() = Some(format!("err: {}", e.error()));
                    Ok(None)
                }
            }
        }
    };

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_class_method("foo", cb, ArgSpec::block());
    let value = mrb.load_string("Hello.foo { |a, b| a + b }");
    if let Some(f) = failure.borrow().as_ref() {
        panic!("{f}");
    }
    let value = value.unwrap_or_else(|e| panic!("err: {}", e.error()));
    assert_eq!(value.fixnum(), 42, "bad: {value}");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbYieldException
#[test]
fn test_mrb_yield_exception() {
    let mrb = Mrb::new();

    let cb = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
        match m.yield_block(&m.get_args()[0].clone().into(), &[]) {
            Err(exc) => Err(Value::Mrb(exc.value)),
            Ok(result) => Ok(Some(result.into())),
        }
    };

    let class = mrb.define_class("Hello", Some(&mrb.object_class()));
    class.define_class_method("foo", cb, ArgSpec::block());
    assert!(mrb.load_string(r#"Hello.foo { raise "exception" }"#).is_err(), "should error");

    assert!(mrb.load_string("Hello.foo { 1 }").is_ok(), "exception should have been cleared");

    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbRun
#[test]
fn test_mrb_run() {
    let mrb = Mrb::new();

    let mut parser = Parser::new(&mrb);
    let context = CompileContext::new(&mrb);

    // Upstream ignores Parse's and the `$do_raise` LoadStrings' results.
    let _ = parser.parse(
        r#"
		if $do_raise
			raise "exception"
		else
			"rval"
		end"#,
        Some(&context),
    );

    let proc: Value = parser.generate_code().into();

    // Enable proc exception raising & verify
    let _ = mrb.load_string("$do_raise = true");
    assert!(mrb.run(&proc, None).is_err(), "expected exception");

    // Disable proc exception raising
    // If we still have an exception, it wasn't cleared from the previous invocation.
    let _ = mrb.load_string("$do_raise = false");
    let rval = mrb.run(&proc, None).unwrap_or_else(|e| panic!("unexpected exception, {}", e.error()));

    assert_eq!(rval.string(), "rval", "expected return value 'rval'");

    let _ = parser.parse("a = 10", Some(&context));
    let proc: Value = parser.generate_code().into();

    let (stack_keep, _) = mrb.run_with_context(&proc, None, 0).unwrap_or_else(|e| panic!("{}", e.error()));

    assert_eq!(stack_keep, 2, "stack value was {stack_keep} not 2; some variables may not have been captured");

    let _ = parser.parse("a", Some(&context));
    let proc: Value = parser.generate_code().into();

    let (_, ret) = mrb.run_with_context(&proc, None, stack_keep).unwrap_or_else(|e| panic!("{}", e.error()));

    assert_eq!(ret.string(), "10", "Captured variable was not expected value");

    context.close();
    parser.close();
    mrb.close();
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbDefineMethodConcurrent
///
/// Runtime adaptation (goroutines -> threads): 100 goroutines each define 100
/// singleton methods on their own VM, racing on go-mruby's global method table.
/// The Rust invariant: method definition on independent VMs from 100 concurrent
/// threads neither races nor deadlocks; every thread completes (the channel
/// rendezvous becomes joining every thread).
#[test]
fn test_mrb_define_method_concurrent() {
    let concurrency = 100;
    let num_funcs = 100;

    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            std::thread::spawn(move || {
                let cb = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
                    Ok(Some(Value::Mrb(m.get_args()[0].clone())))
                };
                let mrb = Mrb::new();
                for i in 0..num_funcs {
                    mrb.top_self().singleton_class().define_method(&format!("test{i}"), cb, ArgSpec::any());
                }
                mrb.close();
            })
        })
        .collect();

    for h in handles {
        join(h);
    }
}

/// upstream: vendorlib/go-mruby/mruby_test.go#TestMrbStackedException
#[test]
fn test_mrb_stacked_exception() {
    let test_class: Rc<RefCell<Option<Class>>> = Rc::default();

    fn create_exception(m: &Mrb, msg: &str) -> Value {
        let val = m
            .class("Exception", None)
            .expect("Exception class")
            .new_instance(&[Value::String(msg.to_owned())])
            .unwrap_or_else(|e| panic!("could not construct exception for return: {}", e.error()));
        Value::Mrb(val)
    }

    let test_func = {
        let test_class = Rc::clone(&test_class);
        move |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
            let args = m.get_args();

            let class = test_class.borrow().clone().expect("testClass is set before Ruby runs");
            let t = match class.new_instance(&[]) {
                Ok(t) => t,
                Err(e) => return Err(create_exception(m, &e.error())),
            };

            let argv: Vec<Value> = args.into_iter().map(Value::Mrb).collect();
            match t.call("dotest!", &argv) {
                Ok(v) => Ok(Some(v.into())),
                Err(e) => Err(create_exception(m, &e.error())),
            }
        }
    };

    let do_test_func = |m: &Mrb, _self: &MrbValue| -> Result<Option<Value>, Value> {
        let err = create_exception(m, "Fail us!");
        Err(err)
    };

    let mrb = Mrb::new();

    let class = mrb.define_class("TestClass", None);
    *test_class.borrow_mut() = Some(class.clone());
    class.define_method("dotest!", do_test_func, ArgSpec::req(0) | ArgSpec::opt(3));

    mrb.top_self().singleton_class().define_method("test", test_func, ArgSpec::req(0) | ArgSpec::opt(3));

    assert!(mrb.load_string("test").is_err(), "No exception when one was expected");

    mrb.close();
    let mrb = Mrb::new();

    let eval_func = |m: &Mrb, this: &MrbValue| -> Result<Option<Value>, Value> {
        let arg = m.get_args()[0].clone();
        match this.call_block("instance_eval", &[], &arg.into()) {
            Ok(result) => Ok(Some(result.into())),
            // Go returns `result, exception`; the result is ignored once an
            // exception is set.
            Err(e) => Err(create_exception(m, &e.error())),
        }
    };

    mrb.top_self().singleton_class().define_method("myeval", eval_func, ArgSpec::block());

    let result = mrb.load_string("myeval { raise 'foo' }");
    assert!(result.is_err(), "did not error");

    // Go: `result != nil` means "result was not cleared". A failed Result carries
    // no value, so this holds by construction.
    assert!(result.ok().is_none(), "result was not cleared");

    mrb.close();
}
