# mruby in Cabby

Upstream AnyCable embeds mruby so operators can script extensions in Ruby. Today the only
consumer is the custom metrics printer (`--metrics_log_formatter=<file.rb>`,
`metrics/custom_printer.go`, scenario `features/mruby_printers.testfile`). It is built only
with the `mrb` Go build tag on Linux and macOS. This document records the embedding strategy
(SPEC §3), how the translated tests map Go mechanics to Rust invariants, and how those
tests run against the Go reference.

Status: implemented. All 58 translated cases (`go_mruby`, `mrb`) and the
`features/mruby_printers` scenario pass on Cabby; the CLI start line reports
` (with mruby 1.2.0 (2015-11-17))`, as upstream.

## What upstream links

| Item | Value | Evidence |
|---|---|---|
| mruby | **1.2.0** (tag `1.2.0`) | `vendorlib/go-mruby/Makefile` `MRUBY_COMMIT ?= 1.2.0`; `compat/upstream.lock` |
| Build config | `etc/build_config.rb`: gcc toolchain, `conf.gembox 'default'`, `conf.gem :github => 'iij/mruby-env'` | CI: `MRUBY_CONFIG=.../etc/build_config.rb make libmruby.a` |
| mruby-env | unpinned upstream (`master`); the reference build used `056ae324451ef16a50c7887e117f0ea30921b71b` | `mruby-build/mruby/build/mrbgems/mruby-env/.git` |
| Binding | go-mruby (`github.com/mitchellh/go-mruby`, vendored at `vendorlib/go-mruby`) + its C helper header `gomruby.h` | |
| Toolchain to build libmruby | Ruby < 3 (2.7) and bison; Cabby's baseline built it in `ruby:2.7` | `compat/baseline/README.md` §3 |
| `libmruby.a` sha256 | `6f5279914cf62329e8f322b30eddc1b565abdcda1024d5b48cc2e8c89d66d098` | `compat/upstream.lock` |

`mrbconf.h` is unmodified: no `MRB_USE_FLOAT`, no NaN/word boxing, so `mrb_float` is
`double` and `mrb_value` is the struct representation.

## Decision: our own `-sys` binding over vendored mruby 1.2.0

**Cabby links mruby 1.2.0, configured like upstream's `etc/build_config.rb`, through its own
small FFI crate (`cabby-mruby-sys`, with a safe layer that implements `cabby_server::mrb`).
The mruby sources and the build outputs generated from them are vendored and compiled with the
`cc` crate, so building Cabby needs a C compiler but no Ruby or bison.**

### Why not an existing crate

Searching crates.io for "mruby" (November 2026) returns about 45 crates. None of them both
targets mruby 1.2.0 and is maintained:

- `mrusty` 1.0.0 (2016-12, MIT) bundles a 2016-era mruby and is unmaintained. `mruby-sys`
  0.2.0 (2019), `mrb`/`mrb-sys` 0.1.2 (2020) and `mruby-eval` (2021) are also inactive.
- `mruby3-sys` (2023) and `minutus` (2023) target mruby 3.x. `beni`/`beni-sys` (2026)
  target a recent mruby C API. Using any of them means adopting a different mruby than upstream.
- `mrubyedge` and `sabiruby` are separate Rust VMs for mruby 3.x/4.x bytecode, not mruby.
- Artichoke is a full Ruby implementation, far larger than this feature needs.

Upstream also depends on go-mruby's C shim (`gomruby.h`) for several observable
behaviors (below). An external crate would not reproduce them. The surface we need is
about 60 mruby C functions, small enough to bind directly.

### Shape

```text
crates/cabby-mruby-sys/        the only crate with `unsafe` (it does not inherit the
                               workspace lint table; it denies undocumented unsafe blocks)
  mruby/                       mruby 1.2.0 sources compiled into upstream's libmruby.a
                               (core, mrblib, default gembox minus the CLI tools,
                               mruby-compiler), their .rb sources, MITL/LEGAL/AUTHORS
  mruby-env/                   iij/mruby-env at 056ae324 (src/env.c, mrblib/env.rb)
  generated/                   outputs of the reference build: mrbgems/mruby-compiler/core/
                               y.tab.c (bison), mrblib/mrblib.c and mrbgems/**/gem_init.c
                               (mrbc bytecode, little-endian)
  SHA256SUMS                   of every vendored file
  shim/cabby_mruby.c           the gomruby.h equivalents: every raising call behind its own
                               jump buffer (rooting its values, unwinding on a raise), type
                               checks before mruby dereferences a value, the trampoline for
                               Rust methods
  shim/gc_live.c               compiles mruby's gc.c plus a liveness test that needs its
                               private layout (built instead of src/gc.c)
  shim/vm_live.c               compiles mruby's vm.c plus frame unwinding and nested runs
                               that need its private helpers (built instead of src/vm.c)
  build.rs                     cc::Build over that file list, -std=gnu99 -O3 (upstream's
                               flags); only with feature `vendored` on Linux/macOS
  src/ffi.rs                   raw declarations of the shim (stubs when not compiled in)
  src/safe.rs                  safe handles: Vm, Val, Raised, Parser, Context
  tests/probes.rs              memory-safety regression tests (`cargo xtask asan`)
crates/cabby-server/src/mrb/   go-mruby's API and behavior on the safe handles;
                               `forbid(unsafe_code)` stays in force
compat/scripts/vendor-mruby.sh refreshes the vendored tree from the reference build
```

- `compat/scripts/vendor-mruby.sh` copies the sources and generated files from the
  reference build (`cargo xtask reference-mruby`, upstream's Makefile in `ruby:2.7` with
  `etc/build_config.rb`) after checking that its `libmruby.a` matches the sha256 in
  `compat/upstream.lock`, and writes `SHA256SUMS`. Normal builds never run it. The object
  list matches the reference `libmruby.a` (75 members).
- Cargo feature `mruby` on `cabby-server` (default) enables `cabby-mruby-sys/vendored`,
  replacing the `mrb` build tag; the C is built on Linux and macOS only, 64-bit
  little-endian. `mrb::supported()` reports it, and without it `mrb::version()` returns
  `unknown` and `--metrics_log_formatter` fails with `unsupported`, as upstream.
- License: mruby is MIT ("Copyright (c) 2015 mruby developers", `compat/licenses/mruby-MITL`).
  mruby-env is MIT (© 2012 Internet Initiative Japan Inc., `compat/licenses/mruby-env-LICENSE`).
  go-mruby is MIT (© 2017 Mitchell Hashimoto, `compat/licenses/go-mruby-LICENSE`): Cabby
  copies none of its code, but the API is modeled on it and the translated tests carry its
  notice. The final parity claim must disclose the remaining C dependency (statically
  linked mruby).

### Memory-safety model

go-mruby hands raw pointers to mruby and trusts the caller. Cabby's safe API must not
let safe Rust cause undefined behavior, so `cabby-mruby-sys` keeps these invariants:

- **No longjmp across Rust.** Rust calls only shim functions. Each one that can raise
  sets its own jump buffer (`CABBY_CALL`), so a raise ends in C and leaves the exception
  in `mrb->exc`; Rust then collects and clears it (`Raised`). Rust methods run from the
  C trampoline through a dispatch callback that returns before anything raises. Shim
  code that runs outside a jump buffer and could only raise on out-of-memory runs with
  none set, so mruby aborts instead of jumping into Rust frames.
- **A raise leaves the call stack as it found it.** `mrb_funcall` only pops its call
  frames on a raise when no jump buffer is set; under the shim's it left them behind,
  and later calls read their stale procs and stack slots (a crash after a few thousand
  raising calls). A protected call restores the context and pops every frame pushed
  since it started (`cabby_unwind_ci`, built from mruby's own `cipop` in
  `shim/vm_live.c`).
- **No freed memory.** A closed VM nulls its state and every later operation panics.
  A value is passed to C only after `cabby_value_live` confirms it is an immediate or a
  live object of its recorded type in one of the VM's live heap pages (values are
  arena-protected exactly as in go-mruby, so they can be collected; TestMrbFullGC relies
  on that). A slot reused by a new object of the same type passes the check: that is
  memory-safe, merely a different object.
- **Values stay alive during a call.** A live value need not be rooted (the caller may
  have restored the arena), and the first allocation inside the call could collect it.
  Every protected call pushes its values on the arena before anything allocates and
  removes exactly those entries afterwards, keeping whatever the call itself left
  there (so the arena contents seen by go-mruby code are unchanged).
- **The arena below a Rust method is its caller's.** C code that calls a Rust method
  (`Array#join` calling `to_s`) may keep unrooted objects in the arena; inside a Rust
  method, `arena_restore` stops at the index the method was entered with.
- **Procs without an environment can only be run.** `Parser::generate_code` returns a
  proc with no environment, and mruby reads `proc->env` without checking when a proc is
  yielded to, called or used as a block. Such a proc is refused (`TypeError`) wherever
  Ruby code could see it (receiver, argument, block, yield target, stored value); `run`
  and `context_run` accept it.
- **Backtraces read live frames only.** mruby rebuilds an exception's backtrace from the
  call frames still in memory when it is read; by then they have been popped and their
  procs (and ireps) may be collected. The shim's version of `mrb_exc_backtrace` uses a
  frame only when its proc is a live Proc, and is otherwise identical.
- **Run never clobbers a frame.** At the top level `run`/`context_run` use the root
  frame, as go-mruby, with the stack keep limited to the root frame's registers (only
  those slots are GC roots; a larger keep read collected objects). Inside a Rust method
  they run on a call frame of their own (`cabby_run_nested`) instead of overwriting the
  method's (and its arguments).
- **No context switch across C.** `Fiber#resume`/`#transfer` called directly from C
  (`Val::call`, or a block defined in C) switched contexts while the C caller popped
  frames of the old one. The shim wraps both and raises mruby's own
  `FiberError: can't cross C function boundary` in that case; from Ruby code fibers work
  as usual. `Fiber.yield` from a proc run inside a Rust method is refused the same way.
- **No unwinding into C.** Rust methods run under `catch_unwind`. A panic becomes a
  `RuntimeError` inside the VM and is re-raised in Rust as soon as the VM call returns.
- **Checked types.** The shim checks types where mruby casts without checking (procs for
  `Run`/`Yield`/blocks, arrays, hashes, classes), refuses `generate_code` without a
  parse tree, limits `mrb_gc_arena_restore` to initialized slots, and only accepts
  exception objects as errors of Rust methods.

Where go-mruby would read freed memory, crash or abort, Cabby returns an error or panics
(deviation D-032). What no binding can fix are memory-safety bugs inside mruby 1.2.0
itself (reachable from Ruby code alone, e.g. a script reading an exception's backtrace
long after the raise), which is why scripts must be trusted operator input.

#### Checking memory safety

`crates/cabby-mruby-sys/tests/probes.rs` holds one regression test per crash the
November 2026 review found from safe Rust, plus the metrics
printer's calls with a formatter that raises on every call. Their assertions catch most
of the bugs on their own; under AddressSanitizer any read of freed memory fails the run:

```sh
cargo xtask asan   # x86_64 Linux: cabby-mruby-sys tests, C and Rust instrumented
```

The command compiles mruby and the shim with `-fsanitize=address` (the system C
compiler) and the Rust code with `-Zsanitizer=address` into `target/asan`. The pinned
stable toolchain accepts the flag with `RUSTC_BOOTSTRAP=1`, and its x86_64 Linux
standard library ships the sanitizer runtime, so no nightly toolchain is needed. Leak
detection is off (mruby 1.2 does not free everything at exit). CI runs it in the
`mruby-asan` job.

### Binding semantics the implementation must keep

These are go-mruby/`gomruby.h` behaviors that the translated tests observe:

| Behavior | Upstream mechanism | Rust requirement |
|---|---|---|
| `Float()` returns `1.2000000476837158` for `1.2` | `_go_mrb_float` returns a C `float` | `MrbValue::float` narrows through `f32`. Decode uses it too |
| Raised exceptions are returned and **cleared** (VM stays usable) | `GOMRUBY_EXC_PROTECT` (MRB_TRY around the call), `checkException` sets `mrb->exc = NULL` | Every raising call goes through a C shim that catches the longjmp (a longjmp across Rust frames is UB) |
| `Exception.File`/`Line` come from the first backtrace line split at `:`; `Unknown`/0 without backtrace | `newExceptionValue` | Same parsing |
| `ValueType` numbering; nil is `0xffffffff` | `value_type.go` over mruby 1.2.0 `enum mrb_vtype` | `ValueType` discriminants (`value.rs`) |
| `RunWithContext` returns the proc's `nlocals` as the new stack keep | `_go_mrb_context_run` | Same (TestMrbRun expects 2 after `a = 10`) |
| Rust methods called from Ruby | `goMRBFuncCall` looks the `Func` up in a global `state → class → symbol` table | Per-VM table in `mrb_state->ud`. The C trampoline calls an `extern "C"` Rust fn that runs the closure under `catch_unwind` and turns a panic into a Ruby exception |
| `GetArgs` | `mrb_get_args(mrb, "*&")` into a global accumulator under a lock | Per-call buffer; no global lock |
| `Parser` captures errors | `capture_errors = 1` | Same |

### Version policy

mruby 1.2.0 is the version that keeps upstream behavior. mruby 2.x and 3.x renumber and
rename value types (e.g. `MRB_TT_FIXNUM` → `MRB_TT_INTEGER`), change integer/float semantics,
backtrace and `mrb_context_run` stack handling, and drop or rename C APIs go-mruby uses.
mruby 1.2.0 (2015) also has known memory-safety bugs that later releases fixed. They matter
less here, because the scripts are operator-provided files, not client input. Still, SPEC §6
says not to keep a security defect only to match Go. So moving to a maintained mruby 3.x is the
first candidate deviation once the suite passes on 1.2.0. Record it in `compat/deviations.md`
with pinning tests for each observable change (value types, float precision, error texts).

### Threading

An mruby VM is single-threaded, so `Mrb`, `MrbValue`, `Class` and `Engine` are `!Send`.
Independent VMs on different threads are independent (TestMrbGetArgs and
TestMrbDefineMethodConcurrent check this). Upstream shares one `DefaultEngine` per process
behind a mutex. Cabby keeps one default engine per thread that asks for it. The metrics
printer (`metrics/custom_printer.rs`) owns a thread that holds its engine and receives
snapshots over a channel, and restores the GC arena after each snapshot (upstream's arena
grows with every print). See deviation D-033.

## Translated tests

| Upstream | Rust | Cases |
|---|---|---|
| `vendorlib/go-mruby/*_test.go` (10 files with cases, plus helper `func_test.go`) | `tests/unit/go_mruby/` (binary `go_mruby`) | 57 |
| `mrb/mrb_test.go` | `tests/unit/mrb/` (binary `mrb`) | 1 |

Facades are `cabby_testkit::reference::{mruby, mrb}`.

### Runtime-specific adaptations

These assertions depend on Go mechanics. Each test keeps the upstream body and checks the
Rust invariant written in its doc comment:

| Upstream case | Go mechanic | Rust invariant |
|---|---|---|
| `decode_test.go#TestDecode`, `#TestDecodeInterface` | `reflect`-driven `Decode(out interface{}, v)` | serde: `decode::<T: Deserialize>` (`int`→`i64`, `*int`→`Box<i64>`, struct field `Foo`→serde field `foo`); `interface{}`→`Decoded` via `decode_value` |
| `examples_test.go#Example_Decode` | `fmt` `%v` output `map[foo:bar]` | decoded value equals the map `{"foo" => "bar"}` |
| `examples_test.go#Example_*` (all three) | `// Output:` comparison | the same output string is built and compared |
| `value_test.go#TestMrbValueValue` | `*MrbValue` pointer identity | `mrb_value()` of a value denotes the same value (`same`) |
| `value_test.go#TestMrbValueValue_impl` | compile-time `var _ Value = ...` | `MrbValue` converts into a `Value` that denotes the same value |
| `parser_test.go#TestParserError_error` | compile-time `var _ error = new(ParserError)` | the zero `ParserError` is a `std::error::Error` rendering `Ruby parse error!\n\n` |
| `value_test.go#TestValueClass` | `reflect.DeepEqual` of `*Class` | `Class: PartialEq` (same class) |
| `gc_test.go#TestIsDead` | `IsDead` after `Close` reads the freed `mrb_state` | a value outliving its VM reports dead without touching freed memory |
| `value_test.go#TestExceptionString_afterClose` | cgo lifetime of the exception message | exception text is owned and readable after `close` |
| `mruby_test.go#TestMrbGetArgs`, `#TestMrbDefineMethodConcurrent` | goroutines racing go-mruby's global tables | independent VMs on OS threads, joined; no shared state, no deadlock |
| `mruby_test.go#TestMrbYield` | `t.Fatalf` inside a cgo callback | the callback records the failure (a panic must not cross into the VM) and the test fails after `LoadString` |

Smaller API adaptations: Go `(value, error)` pairs are `Result`s, so "result was not cleared"
in TestMrbStackedException holds by construction. A nil `*MrbValue` from `Array.Get` or
`Hash.Delete` is `None`. `Class`/`Module` return `Option` where Go aborts the process.
`CallBlock` takes the block as its own parameter. `Class.New` is `new_instance`.

### Verification against Go

The cases run against Go through `compat/reference-adapters/mruby-bridge`. It is a separate
Go module because it links `libmruby.a` with cgo (`go build -tags mrb`). `cargo xtask
reference-build` builds it to `target/reference-adapters/mruby-bridge` when `libmruby.a`
exists. The Rust client is `cabby_testkit::mruby_bridge`.

- Go objects stay in the bridge and Rust holds integer handles. A Go pointer always gets
  the same handle, so pointer identity is observable.
- Each Rust test thread is a bridge session served by its own goroutine. Concurrent tests
  therefore run concurrently in Go, as upstream's goroutine tests do.
- Methods defined from a Rust test (`DefineMethod` with a closure) are Go `Func`s that send
  a callback message to the defining thread. They keep serving that session's nested
  requests (`GetArgs`, `Yield`, ...) until the Rust closure's `Return` arrives.
- Ops only marshal: each calls one exported go-mruby or `mrb` function. Decode targets are
  named Go types, because Go decodes by static type.

- A callback reply that cannot be used (malformed `Return`, unknown handle, session ended)
  is a harness fault. The op that ran the Ruby code fails with `callback fault: …`; it never
  looks like a nil result.
- Go returns a nil `*MrbValue` with every exception. The facade asserts this, because a Rust
  `Err` carries no value (TestMrbStackedException's "result was not cleared").

All 58 cases pass against Go. None is `not_observable`. TestIsDead's Go pass depends on
upstream's read of freed memory (see above). With the Rust tests running in parallel, other
VMs in the shared bridge can reuse that memory first. Upstream's tests run sequentially.
Before isolation, one of about 95 back-to-back runs of the `go_mruby` binary failed. So the
case runs in a bridge process of its own (`reference::mruby::isolated`), where no other VM
allocates between `Close` and `IsDead`.
