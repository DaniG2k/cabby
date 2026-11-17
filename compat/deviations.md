# Deviations from the Go reference

Cabby may improve on the Go implementation internally (algorithms, data structures,
concurrency). This file records only the changes that can be **observed from outside**
(wire format, defaults, limits, ordering, error codes, CLI output, timing contracts), plus
fixes to test tooling. Each entry links a test that pins the new behavior.

Internal changes that keep observable behavior identical don't need an entry. The
compatibility suite and the Go-vs-Rust benchmarks cover them.

| ID | Area | Go behavior | Cabby behavior | Why | Test | Status |
|---|---|---|---|---|---|---|
| D-001 | test tooling | `features/runner.rb` waits for exit before reading a child's output pipe; deadlocks when output exceeds the pipe buffer (seen with 8 KiB pipes) | `cabby_testkit::process::Supervised` drains stdout/stderr continuously | correctness of the harness; no server behavior involved | `crates/cabby-testkit/src/process.rs` | adopted |
| D-002 | test translation | `protocol_test.go` `TestNewCommandMessage/Base` asserts `NotNil("/cable-test", …)`, which is vacuous | assertion kept as written; separate regression asserts URL equality | SPEC §6 | `protocol::protocol_test::test_new_command_message::base_url_equality` | adopted |

## Proposed (not adopted yet)

Record a proposed observable change here first, with its rationale and a test, before you implement it.
