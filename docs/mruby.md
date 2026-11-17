# mruby in Cabby

Upstream AnyCable embeds mruby so operators can script extensions in Ruby. Today the only
consumer is the custom metrics printer (`--metrics_log_formatter=<file.rb>`,
`metrics/custom_printer.go`, scenario `features/mruby_printers.testfile`). It is built only
with the `mrb` Go build tag on Linux and macOS. This document records the embedding strategy
(SPEC §3), how the translated tests map Go mechanics to Rust invariants, and how those
tests run against the Go reference.

Status: only the interface exists (`crates/cabby-server/src/mrb/`). Every body is `pending!`.

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
crates/cabby-mruby-sys/        unsafe FFI; allows `unsafe_code` (the workspace forbids it)
  mruby/                       mruby 1.2.0 source tree + mruby-env at the pinned commit
  generated/                   outputs of one upstream-style build, checked in with sha256s:
                               parse.c (bison), mrblib.c, per-gem gem_init.c (mrbc bytecode),
                               build/host/mrbgems/gem_init.c
  shim/cabby_mruby.c           the gomruby.h equivalents (exception protection, getters)
  build.rs                     cc::Build over the file list of the reference build
crates/cabby-server/src/mrb/   safe API (Mrb, MrbValue, Class, ...); `forbid(unsafe_code)`
                               stays in force; the safe layer lives in the sys crate's
                               `safe` module and cabby-server wraps it
```

- A `generate` xtask regenerates `generated/` in `ruby:2.7` with bison, using upstream's
  `etc/build_config.rb`, and checks the file list and hashes against
  `compat/upstream.lock`. Normal builds never run it.
- Cargo feature `mruby` on `cabby-server` replaces the `mrb` build tag. It is on by default on
  Linux and macOS. `mrb::supported()` reports it, and without it `mrb::version()` returns
  `unknown`, as upstream does.
- License: mruby is MIT ("Copyright (c) 2015 mruby developers", `MITL`). mruby-env is MIT
  (`mrbgem.rake`). go-mruby is MIT (© 2017 Mitchell Hashimoto,
  `compat/licenses/go-mruby-LICENSE`): Cabby copies none of its code, but the API is
  modeled on it and the translated tests carry its notice. Add mruby's and mruby-env's
  notices under `compat/licenses/` when the sources are vendored. The final parity claim
  must disclose the remaining C dependency (statically linked libmruby).

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
printer owns its engine on the thread that formats snapshots.

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
