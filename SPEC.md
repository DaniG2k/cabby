# Cabby: tests-first AnyCable Rust port specification

Status: M0–M6 complete: all 923 translated upstream cases pass against Cabby (AnyT conformance, SSE, Pusher, durable streams, Redis/NATS, mruby, Benchi harness; AnyCable #275 fix adopted). M7 in progress: Go vs Rust benchmarks, independent review, release.
Prepared: 2026-11-17. Progress: `cargo xtask report`; baseline evidence: `compat/baseline/README.md`.
Project name: **Cabby**. Directory and planned binary: `cabby` (an independent AnyCable-compatible implementation).

## 1. Objective and sequence

Implement the open-source AnyCable server in Rust while preserving the observable behavior of a pinned Go implementation.

The sequence is mandatory:

1. Freeze and inventory the upstream source, tests, fixtures, and test dependencies.
2. Port the complete test corpus to Rust, with traceability to every upstream case.
3. Validate the translated tests against Go wherever possible.
4. Implement the Rust production code one subsystem at a time, making the existing translated tests pass.
5. Establish full compatibility before optimizing and publishing performance claims.

During steps 1–3, production Rust code is limited to type declarations, interfaces, generated protocol bindings, and explicitly unimplemented stubs needed to compile tests. Test clients, process supervisors, mock services, and Go reference adapters are permitted. A test compiling is not evidence that its behavior is implemented.

Success means a useful compatible server with independently verified correctness. Rust being faster or smaller is a hypothesis to measure.

## 2. Reference implementation and evidence

Repository: https://github.com/anycable/anycable
Baseline commit: `26a1a703a54b870e42f5660c3d0785db2c602764`.
Use the commit, not the moving `main` branch or archived `anycable-go` repository.

Source inspection for this specification covered the recursive file tree, Makefile, CI workflow, Gemfile, Go module, license, benchmark documentation, and representative protocol, subscription, origin, RPC, and disconnect-queue tests. Tests have not been executed. File counts below are an inventory seed, not case counts or coverage claims.

| Corpus | Files found | Treatment |
|---|---:|---|
| Main server Go tests | 86 | Translate all cases and subcases |
| Nested Benchi Go tests | 12 | Translate harness behavior and scenario assertions |
| Vendored go-mruby tests | 11 | Account for and translate the applicable Ruby-binding semantics; isolate Go-specific assertions |
| Ruby feature scenarios | 16 | Rewrite their scenario logic and assertions in Rust |
| Local AnyT extensions | 6 | Rewrite in Rust and retain originals as an independent check |
| Feature benchmark scripts | 2 | Port workloads/checks; replace runtime-specific measurements explicitly |
| External AnyT / scenario-runner dependencies | Not yet enumerated | Resolve, pin, inventory invoked cases and fixtures in milestone 0 |

There are 109 Go test files in total. `test-files.json` records their paths plus the other locally discovered test/benchmark files. Every entry currently has status `unreviewed`.

Upstream CI covers Redis, NATS, HTTP and gRPC RPC, TLS, embedded execution, brokers, and feature scenarios. The main `go test ./...` invocation does not establish coverage of nested modules; run Benchi and vendored module suites separately where buildable.

Preserve upstream MIT copyright and permission notices for translated tests and code. Record the licenses of separately sourced test dependencies too.

## 3. Compatibility scope

The final scope includes all behavior represented by the pinned open-source implementation and test corpus:

- Action Cable WebSocket protocol and subscription lifecycle.
- gRPC and HTTP RPC, metadata, connection/channel state, failures, deadlines, cancellation, retries, concurrency limits, and disconnect processing.
- Broadcast HTTP endpoints and Redis/NATS adapters, including legacy formats still tested upstream.
- Hub routing, broker history/recovery, presence, and cross-node pub/sub.
- Authentication, JWT identification, signed streams, request origins, headers, and cookies.
- SSE, durable streams, Pusher support, and other exposed transports/features discovered during inventory.
- CLI, environment and TOML configuration, defaults, precedence, health endpoints, TLS, metrics, lifecycle, and shutdown.
- Embedded-server use cases, embedded NATS, and mruby-backed extension behavior.
- Benchmark harness correctness, utilities, and regression cases.

Rails application channel code and the existing JavaScript client remain external interoperability partners. Rewriting the Rails integration or frontend is outside this server port.

Go source-level embedding APIs cannot be preserved literally in Rust. Provide Rust APIs with equivalent capabilities and test their behavior. Go goroutine counts, heap counters, pointer identity, and runtime diagnostics require documented Rust equivalents rather than fabricated compatibility.

Embedded NATS and mruby are substantial integration work: using an external NATS process can unblock early testing, but does not satisfy the embedded-NATS milestone. Determine an explicit packaging/embedding strategy during test design. Final claims must disclose any remaining Go or C runtime dependency. This project does not require rewriting third-party NATS or mruby internals.

No test disappears because its feature is inconvenient. A truly Go-specific assertion must have a recorded explanation and replacement invariant where meaningful. Unimplemented features remain visible as incomplete; they are not silently removed from the denominator.

## 4. Test inventory and traceability

Create `compat/test-map.json`, expanding the file inventory into individual cases. Use Go AST analysis plus runtime test enumeration to discover top-level tests, table rows, nested `t.Run` cases, examples, benchmarks, fuzz targets if present, build tags, and skips. Inspect Ruby scenarios and external AnyT discovery output as well. Regex-only counting is insufficient.

Each entry must contain:

- Stable ID: upstream commit + file + test/subtest path.
- Upstream language, source lines, fixture hashes, build tags, services, and platform requirements.
- Original assertions and intended behavior in a short description.
- Rust case name/path and any split or merged mappings.
- Translation status: `unreviewed | translated | reviewed`.
- Reference status: `unverified | pass | upstream_failure | upstream_skip | not_observable`.
- Rust status: `pending | fail | pass`.
- Adaptation notes, known discrepancies, and evidence/run artifact paths.

One upstream case may map to several Rust cases. Multiple upstream cases may share a Rust parameterized implementation, but each must remain individually reported. Source helpers and mock files must also be mapped as dependencies.

Report translation completeness, Go-reference verification, and Rust implementation coverage separately. Include skipped and adapted cases explicitly. Do not equate line coverage or test-file counts with compatibility.

## 5. Make the tests executable before the port

Use two execution paths.

### External behavior

Build a Rust test driver that accepts `SUT_BIN` and a backend kind (`go` or `rust`). It launches the selected server, assigns isolated ports/configuration, runs WebSocket/HTTP clients and fake RPC applications, collects observable events, and tears everything down.

Run the same translated scenarios against the pinned Go binary first. Preserve expected outcomes from source assertions and protocol contracts; captured Go output is supplementary evidence, not automatically correct golden data.

Retain original AnyT and feature suites as independent verification during the migration. Reusing Ruby tooling is a temporary verification dependency, not a substitute for translating the tests themselves.

### Internal units

Port unit assertions into Rust against small planned interfaces. Where Go functionality is callable, a test-only Go adapter can expose operations through a versioned JSON-lines protocol. For private package state, use a minimal test-only bridge in a disposable reference checkout; production algorithms must remain untouched. Record and review every adapter patch.

Do not invent a second implementation in the adapter to make the tests pass. If an internal assertion cannot reasonably be observed through Go, translate and review it, mark reference status `not_observable`, and retain its explicit pending Rust implementation.

The adapters support tests only; the production Rust server must not call back into the Go reference server.

### Pending tests and CI

Every translated test must compile and contain its real arrange/action/assert logic. Stubs return a distinct typed pending result or a recognizable unimplemented marker; they never return plausible success values.

During migration, a custom runner reconciles results with the manifest:

- Expected pending cases are reported separately and never counted as passing.
- Unexpected failures, crashes, timeouts, compilation errors, and new skips fail CI.
- Once a case passes against Rust, it joins the required passing set and may not regress.
- Accidental passes of cases marked pending require status review.
- Broad ignore attributes and blanket expected-failure handling are prohibited.

The migration job may be green while documenting pending cases; the full release gate remains incomplete until pending behavior is resolved. An ordinary failing assertion cannot be classified as pending merely because the implementation is unfinished.

## 6. Translation rules

Port behavior and assertions, not Go syntax or memory layout.

- Preserve string identifiers exactly where they are opaque protocol values. Do not normalize JSON embedded inside identifier strings.
- Compare JSON objects semantically only where object key order is irrelevant. Preserve array order, numeric precision, omitted/null distinctions, and raw payload bytes where the protocol or test requires them.
- Generate Rust protobuf types from the pinned `etc/rpc.proto`; do not transcribe generated Go bindings. HTTP RPC serialization must be tested separately against actual Go JSON behavior.
- Replace sleeps and scheduler yields with explicit barriers, notifications, and controlled clocks in unit tests. Real socket tests use bounded deadlines and observable readiness.
- Give every process, socket, service startup, and shutdown wait a deadline. No busy-waits or unbounded drains.
- Translate mock expectations including call arguments, counts, ordering, and forbidden calls. A mock is not a replacement for a real Redis/NATS integration case.
- Separate process environments to test configuration precedence safely; avoid global environment mutation in parallel tests.
- Keep source assertions even when weak. Add stronger regression tests as separate, linked cases; document corrections rather than silently changing expected behavior.
- Classify upstream skips and failures with evidence. Do not perpetuate a known security defect solely to match Go.
- Preserve behavior under cancellation and races. Rust memory safety does not establish application-level ordering or liveness.

Examples from inspected source:

| Upstream source | Required translated behavior |
|---|---|
| `protocol/protocol_test.go` | Request/response mapping, state propagation, SUCCESS/FAILURE/ERROR distinctions |
| `node/subscription_state_test.go` | Separate identifiers for parameterized channels; remove only intended streams |
| `ws/handler_test.go` | Case-insensitive origins, wildcard domain boundaries, empty entries, absent Origin |
| `rpc/http_test.go` | Request paths, metadata, bearer auth, deadline/error mapping, connection reuse |
| `node/disconnect_queue_test.go` | Worker limits, shared rate limiting, backlog discard metrics, concurrent shutdown/enqueue |

The disconnect queue suite includes a skipped timeout case: preserve its provenance and add a deterministic active equivalent. The protocol suite contains a suspicious URL non-nil assertion where equality appears intended: retain a mapping and add an explicit URL equality regression.

## 7. Proposed Rust structure

Start with a small Cargo workspace rather than a crate for every Go package:

```text
cabby/
  SPEC.md
  compat/
    upstream.lock
    test-files.json
    test-map.json
    fixtures/
    reference-adapters/
    deviations.md
  crates/
    cabby-core/       # types, protocol, state, routing, broker interfaces
    cabby-server/     # transports, RPC, adapters, config, executable
    cabby-testkit/    # external clients, fake RPC, fixtures, lifecycle
  tests/
    conformance/
    features/
  tools/
    xtask/              # inventory, test selection, reference builds, reports
  benchmarks/
```

Candidate implementation stack: Tokio for asynchronous execution, an HTTP/WebSocket stack, tonic/prost for gRPC/protobuf, serde for data, and maintained Redis/NATS clients. Select and lock exact crate versions during milestone 0 after compatibility spikes; these are proposed choices, not validated dependencies.

Use explicit interfaces for RPC, clock, connection transport, broker, pub/sub, and instrumentation. Initially keep module names close to upstream to simplify mapping.

**Improvements over Go are encouraged.** The Rust implementation need not mirror Go's algorithms or data structures. Internal improvements (data structures, copying, scheduling, routing, concurrency) are made freely and are judged by the compatibility suite and the Go-vs-Rust benchmarks. Changes that are observable externally (wire format, defaults, limits, ordering, error codes, CLI output, timing contracts) are recorded in `compat/deviations.md` with a pinning test before they are adopted, because the upstream tests do not constrain every observable behavior.

Connection state should have a clear owner, with serialized state transitions per session where required. Avoid locks held across network waits. Preserve upstream queue limits/defaults and cancellation rules; record any proposed change separately.

## 8. Milestones and exit gates

### M0 — Reproducible upstream baseline and complete inventory

- Checkout the exact commit and record checksums, toolchains, build tags, dependency versions, container image digests, and platform.
- Run original unit, feature, conformance, nested-module, and optional-feature suites; record all baseline failures/skips.
- Pin AnyT and scenario-runner versions; upstream Gemfile ranges are not a reproducible lock.
- Inventory every test case, helper, fixture, and benchmark.
- Decide how to expose internal Go behavior to Rust tests.
- Capture an initial workload baseline with Go.

Exit: every discovered case is mapped; baseline results and remaining environment blockers are explicit.

### M1 — Port the complete test corpus to Rust

Translate in this order to build reusable test infrastructure:

1. Data/protocol, encoders, configuration, utilities, and identity.
2. Session/subscription state, hub, queues, node lifecycle, and RPC.
3. Broadcast adapters, pub/sub, brokers, streams, and presence.
4. HTTP/WebSocket, SSE, durable streams, Pusher, and management API.
5. CLI, telemetry/metrics/logging, feature scenarios, and conformance.
6. Embedded integrations, mruby semantics, Benchi, and benchmark assertions.

This is test-porting order only. Do not start production subsystem implementations between these batches.

Exit: every case has reviewed Rust assertions or an explicit runtime-specific adaptation; the test workspace compiles; externally observable scenarios pass against Go except documented baseline issues; internal unobservable cases are identified; production stubs remain pending. Missing test translations block M2.

### M2 — Foundational implementation

Implement data types, protocol conversion, JSON encoding, configuration, signing/identity primitives, and utilities. Preserve case-level mappings.

Exit: relevant unit suites pass and are promoted to mandatory regression tests.

### M3 — State, routing, and RPC

Implement subscription state, hub routing, session/node lifecycle, bounded work scheduling, disconnect handling, and HTTP/gRPC RPC clients.

Exit: state and RPC suites pass, including rejection, timeout, connection reuse, concurrency, and cleanup. Exercise deterministic race schedules, not only happy paths.

### M4 — First interoperable server

Implement WebSocket/HTTP endpoints, handshake/origin handling, heartbeat lifecycle, and HTTP broadcast ingestion.

Exit: a real Action Cable client can authenticate through the existing RPC application, subscribe, receive a broadcast, perform an action, unsubscribe, and disconnect. Applicable original AnyT and translated Rust scenarios both pass.

This is an intermediate deliverable, not a claim of full AnyCable compatibility.

### M5 — Distributed and extended functionality

Implement Redis/NATS integration, history/recovery, presence, signed/public streams, Pusher, SSE, durable streams, and management APIs in dependency order.

Exit: associated unit/integration/feature suites pass, including multi-node and reconnect cases with real external services.

### M6 — Operational and optional-feature parity

Complete CLI/configuration parity, metrics, TLS, logging/telemetry behavior, shutdown, embedded-server API, embedded NATS strategy, mruby integration, and harness ports.

Exit: all supported feature/platform matrices pass. Runtime-specific adaptations are documented; any unresolved behavior prevents a full-parity claim.

### M7 — Performance and release readiness

Run full differential, original conformance, interoperability, stress, and performance suites. Profile before optimizing; retain all correctness gates.

Exit: no unexplained mismatch, pending feature, or new skip; repeatable performance report; packaging and rollback instructions; dependency and license notices.

## 9. CI and verification matrix

Required jobs:

- Inventory integrity: detect new upstream cases, stale mappings, missing assertions, and unexplained exclusions.
- Format, lint, compile, and deterministic unit tests.
- Rust test driver against the Go reference.
- Rust test driver against Rust, enforcing the monotonically growing passing set.
- Original AnyT against each server independently.
- Real Redis/NATS integration, HTTP/gRPC RPC, TLS, broker and presence variants.
- Linux and macOS coverage initially; inventory all upstream platform-specific behavior before claiming platform parity.
- Optional feature matrix including mruby and embedded NATS.
- Scheduled race/liveness stress, disconnect storms, malformed input, slow consumers, and multi-node recovery.

Use unique Redis keys, NATS subjects, storage paths, and process environments per test. Provide services explicitly; no silent skip because localhost Redis is absent. Use a controlled fixture clock for expiration tests and fake RPC barriers for precise concurrent behavior.

Pin the Go reference for the migration. Review later upstream changes in separate updates with a diff of tests and behavior; never silently move the comparison target.

## 10. Performance evaluation

Benchmark both release builds using the same hardware, protocol, payloads, authentication, RPC latency, subscriptions, TLS/compression settings, queue limits, and broker topology. Keep load generators outside the measured server process.

Workloads:

- Idle connections at increasing counts to measure incremental memory per connection.
- One-to-one and one-to-many fan-out, including large rooms.
- Connection churn and simultaneous resubscription.
- Slow readers mixed with healthy clients.
- Slow/unavailable RPC with queue saturation.
- Cluster broadcasts, reconnect/recovery, and shutdown under load.

Measure RSS and peak memory, CPU, delivered messages/second, p50/p95/p99 delivery latency, connect/subscribe latency, recovery time, errors, drops, duplicates, ordering violations, and residual resources after disconnect.

Use warm-up, fixed seeds, repeated runs, and a load level the generator can sustain. Reconcile message IDs and counts rather than treating accepted publications as delivered messages. Do not use Go race builds as the performance comparison.

Benchi currently embeds the Go server and includes Go runtime measurements. Port its workload and correctness checks, then run an external equivalent against both servers for fair comparisons. Its in-process results alone are not production performance evidence.

Set measurable improvement targets after M0 establishes real baseline variance. Compatibility and useful performance are separate acceptance criteria; a correct port can still fail to justify replacing Go.

## 11. First implementation tasks

- [x] Create the Cargo workspace and lock reference/toolchain/test dependency versions (`compat/upstream.lock`, `mise.toml`, `Cargo.lock`).
- [x] Expand `test-files.json` to a complete case-level manifest (`compat/test-map.json`, `cargo xtask inventory`).
- [x] Execute and archive the original Go/Ruby baseline (`compat/baseline/`; the two `.benchfile` workloads remain).
- [x] Build Rust process/service supervision (`cabby-testkit::process`, `sut`) and fake HTTP/gRPC RPC services (`cabby-testkit::fake_rpc`).
- [x] Port protocol and origin cases as the first test-harness validation.
- [x] Verify those tests against Go or documented reference bridges (`compat/reference-adapters/bridge`).
- [ ] Complete the remaining test translations before production Rust implementation.
- [ ] Start M2 and promote passing cases subsystem by subsystem.

The next coding deliverable is the test workspace, manifest, and baseline evidence—not an initial server implementation.

### Owner requests to complete at the end

- [ ] Independent review of the whole project with Fable 5.2 (not anchored on the implementer's summary).
- [ ] Fill the README's Go vs Rust comparison table with measured results from the external harness (`crates/cabby-bench`), including workloads where Rust is not faster.
- [ ] Hand over manual testing instructions, one step at a time (server start, Rails/AnyT client, broadcast, metrics).
- [ ] Once the rewrite is complete, remove the local Go toolchain and Go reference artifacts from the development machine (mise Go install, Go caches, `cabby-reference`/`ruby:2.7` Docker images and volumes, `compat/reference` build outputs). Go-vs-Rust verification then runs only in CI.

## Sources

All repository links below refer to the pinned baseline:

- [Source tree](https://github.com/anycable/anycable/tree/26a1a703a54b870e42f5660c3d0785db2c602764)
- [Makefile: test and conformance commands](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/Makefile)
- [CI test matrix](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/.github/workflows/test.yml)
- [Protocol unit tests](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/protocol/protocol_test.go)
- [HTTP RPC tests](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/rpc/http_test.go)
- [Disconnect queue tests](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/node/disconnect_queue_test.go)
- [Benchi benchmark design](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/etc/benchi/README.md)
- [MIT license](https://github.com/anycable/anycable/blob/26a1a703a54b870e42f5660c3d0785db2c602764/MIT-LICENSE)
- [Custom server guidance](https://docs.anycable.io/misc/how_to_anycable_server)

