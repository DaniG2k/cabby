# Translating upstream tests

How to port one upstream Go test file (or Ruby scenario) to Rust. Read `SPEC.md` §5–§6
first. This guide covers the mechanics. Examples to copy from:
`tests/unit/protocol/protocol_test.rs`, `tests/unit/ws/handler_test.rs`,
`tests/conformance/origin.rs`.

## Hard rules

- **Tests first, no behavior.** Production crates (`cabby-core`, `cabby-server`) get only
  types, signatures and `pending!("<pkg>::<fn>")` bodies. Never return a plausible value
  from a stub. Never implement an algorithm while translating.
- **Port every assertion,** including weak or odd ones. When an upstream assertion is vacuous or
  wrong, keep it as written with a comment, and add a separate `/// regression-for:` test that
  asserts the intended behavior.
- **No `#[ignore]`.** Upstream skips: translate the body; for a skip with a reason, add a deterministic
  active equivalent and note it in the manifest (`notes`).
- **No sleeps for synchronization.** Use channels, barriers or explicit readiness. Every wait has a
  deadline. Real-socket tests use bounded timeouts.
- **Preserve opaque strings exactly** (identifiers, JSON-in-strings). Compare JSON semantically
  only where key order is irrelevant.
- **Upstream files are read-only.** Never edit `compat/reference/`.

## Layout

| Upstream | Rust |
|---|---|
| Go package `foo` | test binary `foo`: `tests/unit/foo/main.rs` + `[[test]]` entry in `tests/Cargo.toml` |
| `foo/bar_test.go` | module `tests/unit/foo/bar_test.rs` (`mod bar_test;` in `main.rs`) |
| `TestThing` (no subtests) | `#[test] fn test_thing()` |
| `TestThing` with `t.Run("Some case", …)` | `mod test_thing { #[test] fn some_case() }` |
| table-driven `t.Run(tc.name, …)` | one `#[test]` per row, or one parameterized helper called from one `#[test]` per row |
| nested package dirs `a/b` | binary name `a_b` |

`main.rs` carries the MIT notice header (copy it from `tests/unit/protocol/main.rs`).

## Traceability annotations (required)

Every `#[test]` has at least one doc annotation immediately above it:

```rust
/// upstream: protocol/protocol_test.go#TestParseConnectResponse/Success
/// regression-for: protocol/protocol_test.go#TestNewCommandMessage/Base
/// external-for: ws/handler_test.go#TestCheckOrigin
```

The path after `#` is the **Go runtime name** as printed by `go test -json`: spaces become `_`
(e.g. `TestNewCommandMessage/With_connection,_channel_state_and_data`). Look the exact IDs up in
`compat/test-map.json` (`"id": "26a1a703:<file>#<path>"`; the annotation omits the `26a1a703:`
prefix). Table-driven rows have their own runtime IDs there too. `cargo xtask inventory` fails on
unknown IDs and on tests without annotations.

Adaptations and `not_observable` reasons must be recorded (SPEC §4). Write them as
`/// note: …` lines on the test; `cargo xtask inventory` copies them into the case's
`notes` in `compat/test-map.json` (prefixed `[src]`), e.g.

```rust
/// upstream: utils/queue_test.go#TestByteQueueClear
/// note: not_observable on Go: head/tail are unexported with no accessor; assertions before head() run against Go.
```

An upstream test that exercises several behaviors may map to several Rust tests (repeat the same
`/// upstream:` line). Several upstream rows may share one parameterized Rust helper, but each row
needs its own `#[test]` so it is reported individually.

## Making one test body run against Go and Rust

Tests never call `cabby_core`/`cabby_server` behavior directly. They call a **facade** in
`crates/cabby-testkit/src/reference/<pkg>.rs`, which dispatches on `CABBY_BACKEND`:

- `Backend::Rust`: calls the Rust stub (pending until implemented).
- `Backend::Go`: calls the Go reference through the JSON-lines bridge
  (`compat/reference-adapters/bridge`). Add a `case "<pkg>.<Func>":` to the bridge that only
  unmarshals arguments, calls the **exported** Go function and returns its result. Put a
  package's bridge ops in their own file `bridge/<pkg>.go` that registers in `init()` (see
  `ops` in `main.go`) so parallel work does not conflict.

Facades only convert shapes (Go JSON ↔ Cabby types); they contain no behavior.

When Go functionality is **not callable** (unexported state, internal goroutines): write the
test against the Rust interface anyway, keep the Go facade arm as
`unimplemented!("not_observable: <reason>")`. The runner classifies such Go runs as
`not_observable` (not as failures), and `cargo xtask test --backend go --record` stores that
reference status. Do not reimplement the behavior in the bridge.

When behavior is **externally observable** (HTTP status, WebSocket frames, CLI output), also
consider a `tests/conformance/` scenario using `cabby_testkit::sut::Sut`, linked with
`/// external-for:`.

## Ruby scenarios (AnyT, features)

AnyT scenarios live in `tests/conformance/anyt_*.rs`: bodies in `anyt_core.rs` / `anyt_ext.rs`,
one file per Makefile `test-conformance*` target instantiating them through `anyt_tests!`
(generated with its `/// upstream:`/`/// note:` lines by `compat/scripts/gen_anyt_variants.py`)
(`anyt_support.rs` holds the target's flags, env and broadcast adapter). The Ruby RPC
application they need is `cabby_testkit::anyt_app`; Redis/NATS come from
the shared services in `cabby_testkit::services` (per-test ACL users and names). `features/*.testfile` and `.benchfile`
ports are in `tests/features/`, with wsdirector's step semantics in `cabby_testkit::wsdirector`.
Replace sleeps with observations (an ordering barrier subscription, a witness client, a log
line); keep inherent windows (ping intervals, TTLs, AnyT's 0.5 s "no message" check) with a
comment.

## Workflow

```sh
cargo xtask reference-checkout   # once: pinned upstream at compat/reference
cargo xtask reference-build      # Go binary + bridge into target/
# translate, then:
cargo xtask inventory            # refresh mapping (fails on bad annotations)
cargo xtask test --backend go    # every translated case must pass against Go
cargo xtask test --backend go --record
cargo xtask test --backend rust  # must be green: new cases show as pending
cargo fmt --all && cargo clippy --workspace --all-targets
```

A translated case that fails against Go is a translation bug until proven otherwise. If Go
really fails, record `reference_status: upstream_failure` with evidence in `notes`. The Go run
enforces the recorded statuses: a case recorded `pass` that stops passing, or a
`not_observable`/`upstream_failure` case whose outcome changes, fails the run until the
change is reviewed and recorded with `--record`.
