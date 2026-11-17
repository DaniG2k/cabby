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
| D-006 | test tooling (AnyT ports) | AnyT waits `sleep 1` after an unsubscribe or `stop_stream_from` (no acknowledgement in Action Cable) | the port sends a subscription to `Cabby::BarrierChannel` on the same session and waits for its confirmation; this relies on the server applying a session's commands in order (true for Go: `Session.ReadMessage` handles commands sequentially), which is a stricter contract than the sleep | deterministic and faster than a sleep; the stricter contract is pinned for Cabby | `conformance::anyt_default::many_clients_one_unsubscribes`, `conformance::anyt_default::single_stream_none_after_unsubscribe`, `conformance::anyt_default::multiple_streams_none_after_unsubscribe`, `conformance::anyt_default::stop_streams_unsubscribes_from_stream` (and their other `anyt_<target>` copies) | adopted |
| D-007 | test tooling (AnyT ports) | AnyT's remote-disconnect scenario sleeps 1 s after `welcome` (guarding an Action Cable-only race) | the port sends the remote disconnect right after `welcome`, relying on the server registering the session (with its identifiers) before sending `welcome` (true for Go: `Node.Authenticated` runs before the transmissions are sent) | the guard is not needed for AnyCable servers; the stricter contract is pinned for Cabby | `conformance::anyt_default::remote_disconnect_by_id` (and its other `anyt_<target>` copies) | adopted |

## Proposed (not adopted yet)

Record a proposed observable change here first, with its rationale and a test, before you implement it.

| ID | Area | Go behavior | Proposed Cabby behavior | Why | Pinning test | Go result |
|---|---|---|---|---|---|---|
| D-003 | Redis pub/sub liveness | If Redis drops the node's stream subscription (subscription gone, connection alive) or the subscriber connection goes half-open, the node never resubscribes and silently stops delivering broadcasts until restarted | Detect lost subscriptions (unsolicited `unsubscribe` pushes, periodic liveness/reconciliation of the subscription set) and dead subscriber connections (heartbeat with timeout); reconnect and resubscribe; delivery resumes within 15s (dropped subscription) / 30s (half-open) | anycable/anycable#275 (open, not reproduced upstream); reproduced deterministically with `cabby_testkit::fault_proxy` | `conformance::issues::delivery_recovers_after_redis_drops_the_stream_subscription`, `conformance::issues::delivery_recovers_after_the_subscriber_connection_goes_half_open` | fails (upstream defect reproduced) |
| D-004 | Redis pub/sub subscription lifecycle | Pinned version releases channels after the last subscriber leaves (fixed in 1.6.8 for this pattern) | Same, as a hard invariant: no Redis subscription for a stream without local subscribers | anycable/anycable#263 (open; reporter still sees growth) | `conformance::issues::redis_pubsub_channels_are_released_when_streams_have_no_subscribers` | passes (regression guard, no behavior change) |
| D-005 | Memory stability | Slow RSS growth under traffic over days | No unbounded growth: after repeated connect/subscribe/broadcast/disconnect churn, RSS, threads, sockets and file descriptors return to within a fixed margin of baseline | anycable/anycable#265 (open) | Planned soak workload in `crates/cabby-bench` (M7), run against both servers | to measure |
