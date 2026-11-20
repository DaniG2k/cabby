//! Memory-safety regression tests of the safe API: the probes of the November
//! 2026 review (`docs/review/repro/mruby/main.rs`), each of which crashed or
//! read freed memory from safe Rust. Every probe must now work or fail
//! cleanly (an error or a panic). They are most useful under AddressSanitizer
//! (`cargo xtask asan`), where a use-after-free fails the run even when it
//! happens to read plausible data; the assertions below catch most of them
//! without it.
#![cfg(cabby_mruby)]

use std::rc::Rc;

use cabby_mruby_sys::{Context, Method, Parser, Val, Vm};

fn compile(vm: &Vm, code: &str) -> Val {
    let mut p = Parser::new(vm);
    let (errs, _) = p.parse(code, None);
    assert!(errs.is_empty(), "{errs:?}");
    p.generate_code()
}

/// A hash, class or object made by Ruby code and then left unrooted: alive
/// until the next collection, and no longer protected by the arena.
fn unrooted(vm: &Vm, code: &str) -> Val {
    let ai = vm.arena_save();
    let v = vm.load_string(code).expect("value");
    vm.arena_restore(ai);
    // Overwrite the stale VM stack slot that would keep it reachable.
    let _ = vm.load_string("nil");
    vm.arena_restore(ai);
    v
}

/// `generate_code` before any parse dereferenced the parser's missing tree.
#[test]
fn generate_code_before_parse_is_nil() {
    let vm = Vm::open();
    let mut p = Parser::new(&vm);
    assert!(p.generate_code().is_nil());
    let (errs, _) = p.parse("def foo(", None);
    assert!(!errs.is_empty());
    assert!(p.generate_code().is_nil(), "a failed parse has nothing to run");
    vm.close();
}

/// A hash that is alive but unrooted was collected by an allocation inside
/// `hash_set` (the key's `mrb_str_dup`), which then wrote to the freed table.
#[test]
fn hash_set_keeps_its_receiver_alive() {
    let vm = Vm::open();
    for round in 0..200 {
        let h = unrooted(&vm, "{}");
        for i in 0..5000 {
            let ai = vm.arena_save();
            let k = vm.string(&format!("key-{round}-{i}"));
            let val = vm.fixnum(1);
            if h.is_dead() {
                vm.arena_restore(ai);
                break;
            }
            h.hash_set(&k, &val).expect("set");
            assert!(!h.is_dead(), "round {round} iter {i}: the hash was collected inside hash_set");
            vm.arena_restore(ai);
        }
    }
    vm.close();
}

/// The same for the class of `define_method` (a write to a freed method table).
#[test]
fn define_method_keeps_its_class_alive() {
    let vm = Vm::open();
    for round in 0..100 {
        let c = unrooted(&vm, "Class.new");
        for i in 0..2000 {
            if c.is_dead() {
                break;
            }
            let f: Method = Rc::new(|_, _| Ok(None));
            vm.define_method(&c, &format!("m{}", i % 50), 0, false, f).expect("define");
            assert!(!c.is_dead(), "round {round} iter {i}: the class was collected inside define_method");
            let ai = vm.arena_save();
            let _ = vm.string("x");
            vm.arena_restore(ai);
        }
    }
    vm.close();
}

/// An unrooted argument of `obj_new` was collected while `initialize` stored it.
#[test]
fn obj_new_keeps_its_arguments_alive() {
    let vm = Vm::open();
    let cls = vm.load_string("class Keep; def initialize(a); @a = a; end; def a; @a; end; end; Keep").expect("class");
    cls.gc_protect();
    for round in 0..20_000 {
        let ai = vm.arena_save();
        for j in 0..(round % 7) {
            let _ = vm.string(&format!("f{j}"));
        }
        let text = format!("arg-{round}");
        let arg = vm.string(&text);
        vm.arena_restore(ai);
        if arg.is_dead() {
            continue;
        }
        let ai = vm.arena_save();
        let o = cls.obj_new(std::slice::from_ref(&arg)).expect("new");
        assert!(!arg.is_dead(), "round {round}: the argument was collected inside obj_new");
        assert_eq!(o.call("a", &[], None).expect("a").to_s().expect("to_s"), text);
        vm.arena_restore(ai);
    }
    vm.close();
}

/// An unrooted receiver of `singleton_class` was collected during the call.
#[test]
fn singleton_class_keeps_its_receiver_alive() {
    let vm = Vm::open();
    for round in 0..20_000 {
        let ai = vm.arena_save();
        let o = unrooted(&vm, "Object.new");
        for j in 0..(round % 7) {
            let _ = vm.string(&format!("f{j}"));
        }
        vm.arena_restore(ai);
        if o.is_dead() {
            continue;
        }
        let ai = vm.arena_save();
        let _ = o.singleton_class().expect("singleton class");
        assert!(!o.is_dead(), "round {round}: the object was collected inside singleton_class");
        vm.arena_restore(ai);
    }
    vm.close();
}

/// A Rust `to_s` restored the arena below what its C caller (`Array#join`)
/// relied on, and a collection freed the array being joined.
#[test]
fn methods_cannot_restore_the_arena_below_their_caller() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let cls = vm.define_class(&obj, "Foo", &obj).expect("class");
    let f: Method = Rc::new(|vm, _| {
        vm.arena_restore(0);
        vm.full_gc();
        for i in 0..2000 {
            let _ = vm.string(&format!("filler-{i}"));
        }
        Ok(Some(vm.string("x")))
    });
    vm.define_method(&cls, "to_s", 0, false, f).expect("define");
    let long = "A".repeat(165);
    let r = vm.load_string(&format!("a = Foo.new; b = Foo.new; ['{long}', a, 'B', b, 'C'].join('-')")).expect("join");
    assert_eq!(r.to_s().expect("to_s"), format!("{long}-x-B-x-C"));
    vm.close();
}

/// A Rust method called with a splat ran a proc on its own call frame, which
/// overwrote the splat array that `get_args` then read.
#[test]
fn run_inside_a_method_keeps_the_methods_frame() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let proc_ = compile(&vm, "x = 0x41414141; y = 2; x");
    proc_.gc_protect();
    let seen: Rc<std::cell::RefCell<Vec<i64>>> = Rc::default();
    let seen2 = seen.clone();
    let f: Method = Rc::new(move |vm, this| {
        let r = vm.run(&proc_, this).expect("run");
        assert_eq!(r.fixnum(), 0x41414141);
        seen2.borrow_mut().extend(vm.get_args().iter().map(Val::fixnum));
        Ok(None)
    });
    vm.define_method(&obj, "splatted", 1 << 12, false, f).expect("define");
    vm.load_string("args = [1, 2, 3]; splatted(*args)").expect("call");
    assert_eq!(*seen.borrow(), [1, 2, 3]);
    vm.close();
}

/// The same for `context_run` inside a method: a frame of its own, keep ignored.
#[test]
fn context_run_inside_a_method_keeps_the_methods_frame() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let proc_ = compile(&vm, "a = 7; a * 6");
    proc_.gc_protect();
    let f: Method = Rc::new(move |vm, this| {
        let (nlocals, r) = vm.context_run(&proc_, this, 50).expect("run");
        assert_eq!(nlocals, 2);
        assert_eq!(r.fixnum(), 42);
        let args = vm.get_args();
        Ok(Some(vm.fixnum(args.iter().map(Val::fixnum).sum::<i64>() + r.fixnum())))
    });
    vm.define_method(&obj, "runs", 1 << 12, false, f).expect("define");
    assert_eq!(vm.load_string("runs(1, 2)").expect("call").fixnum(), 45);
    vm.close();
}

/// Yielding to a compiled top-level proc read its missing environment.
#[test]
fn yield_to_a_proc_without_environment_fails() {
    let vm = Vm::open();
    let proc_ = compile(&vm, "1 + 1");
    let e = vm.yield_block(&proc_, &[]).expect_err("refused");
    assert!(e.message.contains("without an environment"), "{}", e.message);
    assert_eq!(vm.load_string("2").expect("usable").fixnum(), 2);
    vm.close();
}

/// The same proc as a block (Array#map yields to it from C).
#[test]
fn a_proc_without_environment_is_not_a_block() {
    let vm = Vm::open();
    let proc_ = compile(&vm, "42");
    let arr = vm.load_string("[1, 2, 3]").expect("array");
    assert!(arr.call("map", &[], Some(&proc_)).is_err());
    // Nor any other value Ruby code could call.
    assert!(arr.call("push", std::slice::from_ref(&proc_), None).is_err());
    assert!(vm.gv_set("$p", &proc_).is_err());
    assert!(proc_.call("call", &[], None).is_err());
    assert!(proc_.to_s().is_err());
    // It can still be run.
    assert_eq!(vm.run(&proc_, &vm.top_self()).expect("run").fixnum(), 42);
    vm.close();
}

/// A call that raised from C (NoMethodError) inside a Rust method left its
/// call frame behind; the method's own return then used it.
#[test]
fn a_raising_call_inside_a_method_pops_its_frames() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let f: Method = Rc::new(|_vm, this| {
        assert!(this.call("no_such_method", &[], None).is_err());
        Ok(None)
    });
    vm.define_method(&obj, "careful", 0, false, f).expect("define");
    assert_eq!(vm.load_string("careful; 42").expect("runs").fixnum(), 42);
    vm.close();
}

/// The same at the top level: leaked frames broke the next `run`.
#[test]
fn a_raising_top_level_call_pops_its_frames() {
    let vm = Vm::open();
    let top = vm.top_self();
    for _ in 0..3 {
        assert!(top.call("no_such_method", &[], None).is_err());
    }
    assert_eq!(vm.run(&compile(&vm, "1"), &top).expect("run").fixnum(), 1);
    // A top-level `return` is mruby's LocalJumpError, not a crash.
    let e = vm.run(&compile(&vm, "return 1"), &top).expect_err("raises");
    assert_eq!(e.message, "unexpected return");
    assert_eq!(vm.load_string("2").expect("usable").fixnum(), 2);
    vm.close();
}

/// A stack keep covering slots that the collector no longer rooted read a
/// collected (here: reused) object.
#[test]
fn a_stack_keep_only_covers_rooted_slots() {
    let vm = Vm::open();
    let top = vm.top_self();
    let a = compile(&vm, "a=1;b=2;c=3;d=4;e=5;f=6;g=7; h = 'x' * 5000; nil");
    let (keep, _) = vm.context_run(&a, &top, 0).expect("a");
    assert_eq!(keep, 9);
    let b = compile(&vm, "nil");
    vm.context_run(&b, &top, 0).expect("b");
    vm.arena_restore(0);
    vm.full_gc();
    for i in 0..3000 {
        let ai = vm.arena_save();
        let _ = vm.string(&format!("filler-{i}-{}", "y".repeat(40)));
        vm.arena_restore(ai);
    }
    let c = compile(&vm, "if false; a=1;b=2;c=3;d=4;e=5;f=6;g=7;h=8; end; h.inspect");
    let (_, v) = vm.context_run(&c, &top, keep).expect("c");
    assert_eq!(v.to_s().expect("to_s"), "nil", "the stale local must not be read back");
    vm.close();
}

/// Reading backtraces rebuilt them from popped call frames whose procs (and
/// ireps) had been collected.
#[test]
fn backtraces_of_old_frames_are_safe() {
    let vm = Vm::open();
    for _ in 0..20_000 {
        let ai = vm.arena_save();
        let e = vm.load_string("[1].each { |a| [2].each { |b| [3].each { |c| raise 'x' } } }").expect_err("raises");
        assert_eq!(e.message, "x");
        vm.arena_restore(ai);
    }
    // Backtraces of code with a file name still read as mruby's.
    let mut ctx = Context::new(&vm);
    ctx.set_filename("probe.rb");
    let mut p = Parser::new(&vm);
    let (errs, _) = p.parse("def boom\n  raise 'here'\nend\nboom", Some(&ctx));
    assert!(errs.is_empty(), "{errs:?}");
    let e = vm.run(&p.generate_code(), &vm.top_self()).expect_err("raises");
    assert_eq!(e.backtrace, ["probe.rb:2:in Object.boom", "probe.rb:4"]);
    vm.close();
}

/// `Fiber.yield` from a proc that a Rust method runs inside a fiber switched
/// contexts across the Rust frames; it is refused like any C boundary.
#[test]
fn fiber_yield_across_a_rust_method_is_refused() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let y = compile(&vm, "Fiber.yield 7");
    y.gc_protect();
    let refused: Rc<std::cell::RefCell<Option<String>>> = Rc::default();
    let refused2 = refused.clone();
    let f: Method = Rc::new(move |vm, this| {
        *refused2.borrow_mut() = vm.run(&y, this).err().map(|e| e.message);
        Ok(None)
    });
    vm.define_method(&obj, "ry", 0, false, f).expect("define");
    let r = vm.load_string("f = Fiber.new { ry; 1 }; x = f.resume; [x, 'after'].inspect").expect("runs");
    assert_eq!(refused.borrow().as_deref(), Some("can't cross C function boundary"));
    assert_eq!(r.to_s().expect("to_s"), r#"[1, "after"]"#);
    vm.close();
}

/// `Fiber#resume` called from Rust switched the VM's context while the call
/// popped frames of the old one; it is refused.
#[test]
fn fiber_resume_from_rust_is_refused() {
    let vm = Vm::open();
    let f = vm.load_string("Fiber.new { |x| y = Fiber.yield(x + 1); y * 2 }").expect("fiber");
    for _ in 0..3 {
        let e = f.call("resume", &[vm.fixnum(1)], None).expect_err("refused");
        assert_eq!(e.message, "can't cross C function boundary");
    }
    assert!(f.call("transfer", &[], None).is_err());
    assert_eq!(vm.load_string("1 + 1").expect("usable").fixnum(), 2);
    // From Ruby code fibers work as usual.
    let r = vm.load_string("g = Fiber.new { |x| y = Fiber.yield(x + 1); y * 2 }; [g.resume(1), g.resume(5)]");
    assert_eq!(r.expect("runs").to_s().expect("to_s"), "[2, 10]");
    let obj = vm.object_class();
    let m: Method = Rc::new(|vm, _| {
        let f = vm.load_string("Fiber.new { Fiber.yield 1; 2 }").map_err(|e| e.value)?;
        f.call("resume", &[], None).map_err(|e| e.value)?;
        Ok(None)
    });
    vm.define_method(&obj, "fib", 0, false, m).expect("define");
    assert!(vm.load_string("fib").is_err());
    vm.close();
}

/// Re-parsing with one parser (errors, unterminated heredocs) and running.
#[test]
fn reparsing_is_safe() {
    let vm = Vm::open();
    let mut p = Parser::new(&vm);
    for i in 0..2000 {
        let code = match i % 3 {
            0 => "def foo(".to_string(),
            1 => format!("x{i} = <<EOS\nabc"),
            _ => format!("a{i} = {i}; a{i} + 1"),
        };
        let _ = p.parse(&code, None);
        let v = p.generate_code();
        if !v.is_nil() {
            let r = vm.run(&v, &vm.top_self());
            if i % 3 == 2 {
                assert_eq!(r.expect("run").fixnum(), i + 1);
            }
        }
    }
    vm.close();
}

/// The metrics printer's calls (`custom_printer.rs`) with a formatter that
/// raises on every call: this crashed after about 1,400 to 4,200 prints. The
/// script is loaded with a file name, as `Engine::load_file` does, so its
/// ireps carry the debug information that backtraces read.
#[test]
fn a_formatter_that_always_raises_survives() {
    let vm = Vm::open();
    let mut ctx = Context::new(&vm);
    ctx.set_filename("formatter.rb");
    let mut p = Parser::new(&vm);
    let script = "module MetricsFormatter\n  def self.call(data)\n    data.each { |k, v| raise 'bad' }\n  end\nend";
    let (errs, _) = p.parse(script, Some(&ctx));
    assert!(errs.is_empty(), "{errs:?}");
    vm.run(&p.generate_code(), &vm.top_self()).expect("script");
    let module = vm.module_get("MetricsFormatter").expect("module");
    module.gc_protect();
    for i in 0..10_000_i64 {
        let ai = vm.arena_save();
        let rhash = vm.load_string("{}").expect("hash");
        for n in ["clients_num", "clients_uniq_num", "client_msg_total", "broadcast_msg_total", "rpc_call_total"] {
            rhash.hash_set(&vm.string(n), &vm.fixnum(i)).expect("set");
        }
        let e = module.call("call", &[rhash], None).expect_err("raises");
        assert_eq!(e.message, "bad");
        assert_eq!(e.backtrace.first().map(String::as_str), Some("formatter.rb:3:in MetricsFormatter.call"));
        vm.arena_restore(ai);
    }
    vm.close();
}
