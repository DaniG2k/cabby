use std::rc::Rc;
use cabby_mruby_sys::{Method, Parser, Vm};

fn main() {
    let which = std::env::args().nth(1).unwrap_or_default();
    match which.as_str() {
        "gen_no_parse" => gen_no_parse(),
        "hash_set" => hash_set(),
        "define_method" => define_method(),
        "arena_join" => arena_join(),
        "run_splat" => run_splat(),
        "obj_new" => obj_new(),
        "singleton" => singleton(),
        "yield_noenv" => yield_noenv(),
        "ci_leak_method" => ci_leak_method(),
        "ci_leak_top" => ci_leak_top(),
        "keep_stale" => keep_stale(),
        "bt_stress" => bt_stress(),
        "fiber_run" => fiber_run(),
        "printer" => printer(),
        "fiber_call" => fiber_call(),
        "block_noenv" => block_noenv(),
        "reparse" => reparse(),
        _ => eprintln!("unknown scenario"),
    }
}

// Parser::generate_code with no prior parse.
fn gen_no_parse() {
    let vm = Vm::open();
    let mut p = Parser::new(&vm);
    let v = p.generate_code();
    println!("survived: nil={}", v.is_nil());
}

// hash_set on a live-but-unrooted hash: GC inside mrb_hash_set (mrb_str_dup of the key).
fn hash_set() {
    let vm = Vm::open();
    let mut hits = 0;
    for round in 0..200 {
        let ai = vm.arena_save();
        let h = vm.load_string("{}").expect("hash");
        vm.arena_restore(ai);
        let _ = vm.load_string("nil"); // overwrite the stale VM stack slot
        vm.arena_restore(ai); // h is alive but no longer rooted
        for i in 0..5000 {
            let ai2 = vm.arena_save();
            let k = vm.string(&format!("key-{round}-{i}"));
            let val = vm.fixnum(1);
            if h.is_dead() {
                vm.arena_restore(ai2);
                break; // collected between calls: the safe layer would panic, fine
            }
            // h passes the liveness check here ...
            h.hash_set(&k, &val).expect("set");
            vm.arena_restore(ai2);
            // ... and was swept during the call:
            if h.is_dead() {
                hits += 1;
                println!("round {round} iter {i}: hash was swept INSIDE hash_set (write to freed khash)");
                break;
            }
        }
        if hits >= 3 { break; }
    }
    println!("hits={hits}");
}

fn define_method() {
    let vm = Vm::open();
    let mut hits = 0;
    for round in 0..200 {
        let ai = vm.arena_save();
        let c = vm.load_string("Class.new").expect("class");
        vm.arena_restore(ai);
        let _ = vm.load_string("nil");
        vm.arena_restore(ai);
        for i in 0..5000 {
            if c.is_dead() { break; }
            let f: Method = Rc::new(|_, _| Ok(None));
            vm.define_method(&c, &format!("m{}", i % 50), 0, false, f).expect("define");
            if c.is_dead() {
                hits += 1;
                println!("round {round} iter {i}: class was swept INSIDE define_method (write to freed method table)");
                break;
            }
            // churn to advance the GC threshold
            let ai2 = vm.arena_save();
            let _ = vm.string("x");
            vm.arena_restore(ai2);
        }
        if hits >= 3 { break; }
    }
    println!("hits={hits}");
}

fn obj_new() {
    let vm = Vm::open();
    let cls = vm.load_string("class Keep; def initialize(a); @a = a; end; def a; @a; end; end; Keep").expect("cls");
    let mut hits = 0;
    for round in 0..200000 {
        let ai = vm.arena_save();
        for j in 0..(round % 7) { let _ = vm.string(&format!("f{j}")); }
        let arg = vm.string(&format!("arg-{round}"));
        vm.arena_restore(ai); // arg alive, unrooted
        if arg.is_dead() { continue; }
        let ai = vm.arena_save();
        let o = cls.obj_new(&[arg.clone()]).expect("new");
        if arg.is_dead() {
            hits += 1;
            let got = o.call("a", &[], None).expect("a");
            println!("round {round}: arg swept INSIDE obj_new; @a now has type tag {} (1 = MRB_TT_FREE)", got.type_tag());
            if hits >= 3 { break; }
        }
        vm.arena_restore(ai);
    }
    println!("hits={hits}");
}

fn singleton() {
    let vm = Vm::open();
    let mut hits = 0;
    for round in 0..200000 {
        let ai = vm.arena_save();
        let o = vm.load_string("Object.new").expect("obj");
        vm.arena_restore(ai);
        let _ = vm.load_string("nil");
        vm.arena_restore(ai);
        for j in 0..(round % 7) { let _ = vm.string(&format!("f{j}")); }
        vm.arena_restore(ai);
        if o.is_dead() { continue; }
        let ai = vm.arena_save();
        let _sc = o.singleton_class().expect("sclass");
        if o.is_dead() {
            hits += 1;
            println!("round {round}: object swept INSIDE singleton_class");
            if hits >= 3 { break; }
        }
        vm.arena_restore(ai);
    }
    println!("hits={hits}");
}

// A Rust `to_s` that restores the arena below the level its C caller relies on.
fn arena_join() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let cls = vm.define_class(&obj, "Foo", &obj).expect("class");
    let f: Method = Rc::new(|vm, _| {
        vm.arena_restore(0);
        vm.full_gc();
        // allocate so freed slots get reused
        for i in 0..2000 { let _ = vm.string(&format!("filler-{i}")); }
        Ok(Some(vm.string("x")))
    });
    vm.define_method(&cls, "to_s", 0, false, f).expect("define");
    let r = vm.load_string("a = Foo.new; b = Foo.new; ['AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', a, 'B', b, 'C'].join('-')");
    match r {
        Ok(v) => println!("join returned type {} : {:?}", v.type_tag(), v.to_s()),
        Err(e) => println!("raised: {}", e.message),
    }
}

// Rust method called with a splat (ci->argc < 0) runs a script on its own frame, then get_args.
fn run_splat() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let mut p = Parser::new(&vm);
    let (errs, _) = p.parse("x = 0x41414141; y = 2; x", None);
    assert!(errs.is_empty());
    let proc_ = p.generate_code();
    let f: Method = Rc::new(move |vm, this| {
        let r = vm.run(&proc_, this);
        println!("run inside method ok={}", r.is_ok());
        let args = vm.get_args();
        println!("get_args -> {} values", args.len());
        Ok(None)
    });
    vm.define_method(&obj, "splatted", 1 << 12, false, f).expect("define");
    let r = vm.load_string("args = [1, 2, 3]; splatted(*args)");
    println!("survived: ok={}", r.is_ok());
}

fn compile(vm: &Vm, code: &str) -> cabby_mruby_sys::Val {
    let mut p = Parser::new(vm);
    let (errs, _) = p.parse(code, None);
    assert!(errs.is_empty(), "{errs:?}");
    p.generate_code()
}

// yield to a proc that has no environment (a compiled top-level proc).
fn yield_noenv() {
    let vm = Vm::open();
    let proc_ = compile(&vm, "1 + 1");
    let r = vm.yield_block(&proc_, &[]);
    println!("survived: ok={}", r.is_ok());
}

// A Rust method calls a method that raises from C (NoMethodError), handles the Err, returns normally.
fn ci_leak_method() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let f: Method = Rc::new(|_vm, this| {
        let r = this.call("no_such_method", &[], None);
        println!("inner call is_err={}", r.is_err());
        Ok(None)
    });
    vm.define_method(&obj, "careful", 0, false, f).expect("define");
    let r = vm.load_string("careful; 42");
    println!("survived: {:?}", r.map(|v| v.fixnum()).map_err(|e| e.message));
}

fn ci_leak_top() {
    let vm = Vm::open();
    let top = vm.top_self();
    for i in 0..3 {
        let r = top.call("no_such_method", &[], None);
        println!("call {i} is_err={}", r.is_err());
    }
    let proc_ = compile(&vm, "return 1");
    let r = vm.run(&proc_, &top);
    println!("survived: {:?}", r.map(|v| v.fixnum()).map_err(|e| e.message));
}

// context_run with a keep that is "initialized" but no longer rooted.
fn keep_stale() {
    let vm = Vm::open();
    let top = vm.top_self();
    let a = compile(&vm, "a=1;b=2;c=3;d=4;e=5;f=6;g=7; h = 'x' * 5000; nil");
    let (keep, _) = vm.context_run(&a, &top, 0).expect("a");
    println!("keep after A = {keep}");
    let b = compile(&vm, "nil");
    let (k2, _) = vm.context_run(&b, &top, 0).expect("b");
    println!("keep after B = {k2}");
    vm.arena_restore(0);
    vm.full_gc();
    for i in 0..3000 { let ai = vm.arena_save(); let _ = vm.string(&format!("filler-{i}-{}", "y".repeat(40))); vm.arena_restore(ai); }
    let c = compile(&vm, "if false; a=1;b=2;c=3;d=4;e=5;f=6;g=7;h=8; end; [h.class.to_s, h.size, h[0, 30]].inspect");
    let r = vm.context_run(&c, &top, keep);
    match r {
        Ok((_, v)) => println!("stale local h read back as: {:?}", v.to_s()),
        Err(e) => println!("raised: {}", e.message),
    }
}

fn bt_stress() {
    let vm = Vm::open();
    for i in 0..300000 {
        let ai = vm.arena_save();
        let r = vm.load_string("[1].each { |a| [2].each { |b| [3].each { |c| raise 'x' } } }");
        assert!(r.is_err());
        vm.arena_restore(ai);
        if i % 50000 == 0 { println!("iter {i}"); }
    }
    println!("survived");
}

fn fiber_run() {
    let vm = Vm::open();
    let obj = vm.object_class();
    let y = compile(&vm, "Fiber.yield 7");
    let f: Method = Rc::new(move |vm, this| {
        let r = vm.run(&y, this);
        println!("run in method: ok={}", r.is_ok());
        Ok(None)
    });
    vm.define_method(&obj, "ry", 0, false, f).expect("define");
    let r = vm.load_string("f = Fiber.new { ry; 1 }; x = f.resume; y = [x, 'after']; y.inspect");
    println!("survived: {:?}", r.map(|v| v.to_s()).map_err(|e| e.message));
}

// Mirrors cabby-server metrics/custom_printer.rs::print with a formatter that raises inside a block.
fn printer() {
    use std::io::Write;
    let body = std::env::args().nth(2).unwrap_or_else(|| "data.each { |k, v| raise 'bad' }".to_string());
    let vm = Vm::open();
    vm.load_string(&format!("module MetricsFormatter; def self.call(data); {body}; end; end")).expect("script");
    let module = vm.module_get("MetricsFormatter").expect("module");
    module.gc_protect();
    let names = ["clients_num", "clients_uniq_num", "client_msg_total", "failed_client_msg_total", "broadcast_msg_total",
        "failed_broadcast_msg_total", "broadcast_streams_num", "rpc_call_total", "rpc_error_total", "rpc_retries_total",
        "rpc_pending_num", "failed_auths_total", "goroutines_num", "disconnect_queue_size", "server_msg_total",
        "failed_server_msg_total", "data_sent_total", "data_rcvd_total", "mem_sys_bytes"];
    let out = std::io::stdout();
    for i in 0..2_000_000u64 {
        let ai = vm.arena_save();
        let rhash = vm.load_string("{}").expect("hash");
        for n in names {
            rhash.hash_set(&vm.string(n), &vm.fixnum(i as i64)).expect("set");
        }
        let r = module.call("call", &[rhash.clone()], None);
        if let Ok(line) = &r { let _ = line.to_s(); }
        vm.arena_restore(ai);
        if i % 200 == 0 { let mut o = out.lock(); let _ = writeln!(o, "print #{i} err={}", r.is_err()); let _ = o.flush(); }
    }
    println!("survived");
}

fn fiber_call() {
    let vm = Vm::open();
    let f = vm.load_string("Fiber.new { |x| y = Fiber.yield(x + 1); raise 'in fiber' if y == 0; y * 2 }").expect("fiber");
    println!("resume1 = {:?}", f.call("resume", &[vm.fixnum(1)], None).map(|v| v.fixnum()).map_err(|e| e.message));
    println!("resume2 = {:?}", f.call("resume", &[vm.fixnum(0)], None).map(|v| v.fixnum()).map_err(|e| e.message));
    println!("resume3 = {:?}", f.call("resume", &[vm.fixnum(5)], None).map(|v| v.fixnum()).map_err(|e| e.message));
    println!("after = {:?}", vm.load_string("1 + 1").map(|v| v.fixnum()).map_err(|e| e.message));
    let obj = vm.object_class();
    let g: Method = Rc::new(move |vm, _| {
        let f = vm.load_string("Fiber.new { Fiber.yield 1; 2 }").map_err(|e| e.value)?;
        let a = f.call("resume", &[], None).map_err(|e| e.value)?;
        let b = f.call("resume", &[], None).map_err(|e| e.value)?;
        Ok(Some(vm.fixnum(a.fixnum() * 10 + b.fixnum())))
    });
    vm.define_method(&obj, "fib", 0, false, g).expect("define");
    println!("in method = {:?}", vm.load_string("fib").map(|v| v.fixnum()).map_err(|e| e.message));
}

// an env-less compiled proc used as a block
fn block_noenv() {
    let vm = Vm::open();
    let proc_ = compile(&vm, "42");
    let arr = vm.load_string("[1, 2, 3]").expect("ary");
    let r = arr.call("map", &[], Some(&proc_));
    println!("survived: {:?}", r.map(|v| v.to_s()).map_err(|e| e.message));
}

fn reparse() {
    let vm = Vm::open();
    let mut p = Parser::new(&vm);
    for i in 0..2000 {
        let code = if i % 3 == 0 { "def foo(".to_string() } else if i % 3 == 1 { format!("x{i} = <<EOS\nabc") } else { format!("a{i} = {i}; a{i} + 1") };
        let _ = p.parse(&code, None);
        let v = p.generate_code();
        if !v.is_nil() { let _ = vm.run(&v, &vm.top_self()); }
    }
    println!("survived");
}
