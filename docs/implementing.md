# Implementing Cabby (M2 onwards)

Implementation turns `pending!` stubs into working code, one subsystem at a time, until the
translated tests pass against Cabby (SPEC.md §8, M2–M6). Read SPEC.md §7 (improvements
policy) first.

## Definition of done for a subsystem

- Every translated test for the subsystem's upstream packages passes with
  `cargo xtask test --backend rust`.
- `cargo xtask test --backend go` still passes. Implementation work never changes what the tests
  assert against Go.
- `cargo xtask test --backend rust --record` promotes the newly passing cases; after that they
  are mandatory regressions.
- fmt and clippy `-D warnings` are clean, and there is no `unsafe` (`unsafe_code = "forbid"`).

## Rules

- **Tests are the specification.** Do not edit a translated test to make it pass. If a test looks
  wrong, it must still pass against Go. Report it with evidence instead of changing it.
  Facades in `cabby-testkit` may get Rust-arm adjustments when the Rust API legitimately changes
  shape, but the Go arm and the assertions stay the same.
- **Improve freely inside.** Use better data structures and algorithms, less copying, fewer locks,
  less allocation. The Go code is a reference for behavior, not for structure.
- **Observable changes need a deviation first.** Wire format, defaults, limits, ordering, error
  texts, log lines that tests or users depend on, CLI output, timing contracts: record an entry
  in `compat/deviations.md` with a pinning test before you implement it.
- **Keep the public API stable.** The tests and facades call it. Rename or reshape an API only
  together with its facade's Rust arm.
- **Byte-exact where it matters.** JSON produced for clients and RPC must match Go's
  `encoding/json` output wherever a test compares bytes: key order (Go sorts map keys, struct
  fields in declaration order), `omitempty`, HTML escaping (`<`, `>` and `&` become `<`,
  `>` and `&`), number formatting, and `json.Number` preservation (`UseNumber`, so
  serde_json `arbitrary_precision`).
- **Dependencies.** Prefer maintained, widely used crates. Add them as workspace dependencies
  with exact current versions, at least two weeks old. Note each one in the commit message.
- **Concurrency.** Use Tokio for I/O. Never hold a lock across an `.await` or a network wait. Give
  each session a single owner of its state (an actor or one task) wherever upstream relies on
  serialized transitions.
- **Errors.** Use typed errors (`thiserror`) in libraries. Keep exact error texts where tests or
  clients observe them.

## Workflow

```sh
export CARGO_BUILD_JOBS=2   # several agents share the machine
eval "$(/home/daniele/Documents/apps/cabby/compat/scripts/services.sh start)"   # idempotent, shared
cargo xtask test --backend rust <filter>   # iterate on one package's tests
cargo xtask test --backend rust            # whole suite: no FAIL / regression / unmapped
cargo xtask test --backend go              # unchanged: still passes
```

When a case starts passing, the runner reports `UNREVIEWED-PASS` until you record it. The
orchestrator records it on main after merging, so you don't commit `compat/test-map.json`.
`--record` only updates cases whose tests all ran in that invocation, so recording a filtered
run never promotes a case from part of its tests.

The runner also fails the run when a test binary dies (a signal, an abort, or an exit
without libtest's summary: `CRASH`), when a binary reports fewer results than the tests it
started (`INCOMPLETE`), when a mapped test did not run (`MISSING`), and when the number of
tests run differs from the manifest (`COUNT`). Against Go, every case must keep its recorded
reference status (`REGRESSION-ON-GO`, `CHANGED-ON-GO` until recorded).
