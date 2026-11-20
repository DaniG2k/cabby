# Independent review: Fable 5.2, November 2026

An independent review of the whole project at `7fc880e`..`1fba6cb`, done by a reviewer not
involved in building Cabby, as the owner requested (SPEC §11). The reviewer compared Cabby with
the Go reference by reading both and by running both binaries side by side.

Status of each finding is tracked in the table at the end.

## Verdict (reviewer)

> I would not yet call Cabby a drop-in replacement, but it is closer than I expected. On the
> wire it is remarkably faithful: side-by-side probes of the Action Cable protocol, the HTTP RPC
> contract, history, presence, session restore, JWT, signed streams, Pusher, config and CLI
> found only trivial or already-documented differences. The problems are where the
> architecture differs from Go and no upstream test looks: failure and overload behaviour
> (DNS changes, stalled Redis/NATS, slow RPC, idle HTTP connections), operator logging, and
> thread use.

## Findings

### High

| ID | Finding | Where | Evidence |
|---|---|---|---|
| H1 | gRPC RPC client resolves DNS once and never again; grpc-go re-resolves when a subchannel fails. Rolling Rails pods behind a name break every RPC until Cabby restarts. | `rpc/grpc.rs:157,198-250,304` | verified |
| H2 | Redis and NATS publish queues are unbounded; Go publishes synchronously and pushes back. A 25 s Redis stall grew RSS 14 MB → 1.04 GB and delivered messages 26 s stale. NATS subscribe/unsubscribe share the queue. | `redis/publisher.rs:19-33`, `nats/client.rs:206` | verified |
| H3 | NATS broker: a JetStream init failure is not fatal (Go exits 1) and every later broadcast is kept in a growing backlog with no log line. | `broker/nats.rs:797-813`, `cli/adapters.rs:217-219`; Go `broker/nats.go:140-150`, `cli/cli.go:393` | verified |
| H4 | The bounded command pool causes head-of-line blocking Go doesn't have: non-RPC commands and welcomes wait behind RPC-bound commands, and `rpc_wait_timeout` no longer bounds the wait. With a hung RPC and 600 pending subscribes, a new no-auth client waited 17.5 s (Go: 0 s). | `node/tasks.rs:33,96`, `cli.rs:82-85`, `ws/handler.rs:204`, `node/session.rs:1403,1426` | verified |
| H5 | Idle keep-alive HTTP connections are closed after 5 s (hyper's header-read timeout also applies between requests; Go's `ReadHeaderTimeout` doesn't). Load balancers and pooled broadcast clients get sporadic 502s and failed POSTs. | `server/http_server.rs:587-589` | verified |
| H6 | Redis Cluster: batched `PUBLISH` pipelines fail with CROSSSLOT and the batch is dropped. Go sends one `PUBLISH` per call. | `redis/publisher.rs:58-86` | verified against a stub cluster |
| H7 | Node, hub, memory broker, streams, identity and NATS layers log nothing (Go has 72 log sites in `node/` alone): no `failed to handle incoming message`, `closing active connections`, `no RPC configured`, NATS lines, per-message debug trace. Redis lines fold `error=` into the message and drop `provider=`/`consumer=`. | `node/app.rs`, `node/session.rs`, `hub.rs`, `broker.rs`, `broker/nats.rs` | verified |

### Medium

| ID | Finding | Where | Evidence |
|---|---|---|---|
| M1 | Pusher sessions use one OS thread each (1,000 idle Pusher connections: 1,043 threads vs Go 16), contradicting the README. | `pusher.rs:937` → `node/session.rs:1018-1030` | verified |
| M2 | RPC calls are never cancelled when a session closes (Go passes `s.closeCtx`); a slot and a pool thread are held to completion and `rpc_canceled_total` never moves. | `cli/adapters.rs:372-405`, `common.rs:157` | verified |
| M3 | The API server buffers the request body before the method and auth checks (Go authenticates first). | `server/mounts.rs:27-36`; Go `api/server.go:173-182` | verified |
| M4 | The "safe" mruby API is not memory-safe: eight crashes from safe Rust under ASAN (use-after-free on unrooted values, leaked call frames, NULL write in `generate_code` before a parse, env-less procs as blocks). One is reachable from a metrics formatter that raises on every call. Operator scripts only. | `cabby-mruby-sys/src/safe.rs`, `shim/cabby_mruby.c` | verified |
| M5 | The test runner ignores a test binary's exit status and skips the MISSING check when no test of a case ran; CI runs only `xtask` and `cabby-core` unit tests (not the `cabby-server` ones, including the deviation pins) and never enforces Go statuses. | `tools/xtask/src/runner.rs:222,272`, `.github/workflows/ci.yml:27` | code |
| M6 | Non-ASCII header values are dropped: an SSE `Last-Event-ID` for a non-ASCII stream is ignored; a non-ASCII `--broadcast_key` gets 401 (Go 201). | `sse.rs:423-425,475`, `broadcast.rs:214` | verified |
| M7 | Benchmarks: arithmetic honest, evidence thin: n=3 in one session, fixed order, no warm-up; CPU gaps inside run-to-run spread; fan-out p99 near the generator noise floor; only `--norpc --noauth --broker=none` with tiny payloads, described as "identical flags"; tuning just before the run undisclosed; thread row partly unsourced. Memory result robust; latency/CPU should read "no worse". | README, `compat/scripts/bench-*` | data |
| M8 | Coverage gaps: Durable Streams integration tests use a testkit HTTP server instead of Cabby's; HTTP broadcaster tests call a handler production never mounts; the memory broker expiry loop is untested (manually checked: matches Go); about ten Go-bridge node cases run adapter logic. | `cabby-testkit/src/reference/ds.rs:1386`, `broadcast.rs:212`, `compat/reference-adapters/bridge/node.go:909-945` | code |
| M9 | Unverified: a single Redis URL pointing at a cluster is treated as standalone (rueidis probes for cluster); the embedded `nats-server` child isn't watched after start and its output accumulates in memory forever. | `redis/mod.rs:199-211`, `enats.rs:360-376` | code |

### Low

- A client message nested deeper than 128 levels stops the session's reader for good (Go processes it).
- Stalled SSE clients are disconnected (D-031), but their queued data stays until the socket closes.
- HTTP framing: SSE/DS non-streaming responses are chunked (Go sets `Content-Length`); `/health` has no `Content-Type`; 413 text; a DS 304 carries `Content-Type`; unknown `/api/*` paths return an empty 404. hyper limits differ from net/http; SSE/DS give no response after a client half-close; `%+f` decoding in query strings.
- Redis: `INF connected` every 10 s; `--redis_keepalive_interval` and URL `write_timeout` accepted but ignored; invalid UTF-8 pub/sub payloads dropped (Go delivers U+FFFD).
- WebSocket: the close echo includes the client's reason; protocol-error close reasons differ in text.
- Docs: `compat/deviations.md` lists implemented deviations under "Proposed (not adopted yet)"; the README says every difference is recorded, but H1–H7 and M1–M3 weren't.

## Checked and found sound (reviewer)

- The full suite passed (1162 tests); no `#[ignore]` or silent skips; the 9 `not_observable` cases are honest; about 430 cases compared line by line were faithful translations.
- CLI and config: `--help`, `--print-config` and 17 env/preset combinations byte-identical (apart from random map order).
- Wire protocol: a 110-line differential (history, presence, whisper, session restore, broadcasts) and 35 raw WebSocket frame cases matched Go except documented deviations.
- RPC contract: 25 HTTP RPC requests byte-identical; replies for reject/error/500/garbage, state, stop streams and disconnect identical; unreachable gRPC behaves the same.
- Brokers: TTL expiry matches Go; mixed Go+Cabby clusters on NATS JetStream and Redis interoperate; D-003/D-004 recovery works without duplicates.
- Security: JWT and message-verifier logic match Go with constant-time comparisons; Pusher REST/channel auth and CORS identical; no network-reachable panic or smuggling issue found; no secrets in logs; mruby types are `!Send`/`!Sync`.
- Concurrency: no lost wakeup or lock-order cycle found in the session writer, hub gates and timer wheel; slow-client handling matches Go; Cabby delivers the `server_restart` disconnect on SIGTERM every time (Go lost it in 2 of 5 runs).

Not covered: TLS, Sentinel, `max_conn`, statsd and log metric printers, macOS; H6 on a real Redis Cluster.

## Status

Each fix has regression tests that fail before it and pass after it, run against both backends where Go can expose the behavior. The probes were re-run against the fixed build.

| ID | Status | Result |
|---|---|---|
| H1 | fixed | re-resolves after a connection failure, like grpc-go: new backend reached at 30.0 s (Go 30.1 s; before: never) |
| H2 | fixed (D-040, D-041) | bounded queues with back-pressure: 25 s Redis stall peaks at 25 MB (before 1.07 GB, Go 31 MB); NATS control operations no longer wait behind publications |
| H3 | fixed (D-042) | exits 1 with Go's error after 49 s, like Go; nothing kept after a failure |
| H4 | fixed (D-036) | threads blocked in RPC give up their command-pool seat: fresh client served in 0.00 s with 600 pending RPCs (before 17.5 s, Go 0.00 s) |
| H5 | fixed | Go's `ReadHeaderTimeout` semantics; idle keep-alive connections stay open |
| H6 | fixed | cluster batches split per slot: 30/30 published (before 3/30); also checked on a real single-node cluster |
| H7 | fixed (D-050..D-053) | 304 of 308 Go log call sites matched, 4 not applicable (`docs/logging.md`); logprobe default level 28 lines on both (before 19), debug 59 vs Go 58 (random reconnect backoff); conformance tests compare both backends' log output |
| M1 | fixed | Pusher uses the push-model reader: 1,000 idle Pusher connections add 5 threads (before 1,005, Go 7) |
| M2 | fixed | session close cancels in-flight RPC and RPC-slot waits, with Go's metrics |
| M3 | fixed | API checks method and auth before reading the body |
| M4 | fixed (D-032 rewritten) | all review probes pass under ASAN; `cargo xtask asan` and a CI job; a raising formatter survives 10,000 prints |
| M5 | fixed | runner reports CRASH / INCOMPLETE / MISSING / COUNT; `--record` only from complete runs; Go statuses enforced; CI runs all unit tests |
| M6 | fixed | header values handled as bytes, like Go |
| M7 | method fixed | warm-up, shuffled interleaved rounds, 5 runs; README wording corrected; results to be re-measured on the fixed build |
| M8 | fixed | DS and HTTP broadcaster tests go through Cabby's real HTTP layer; memory broker expiry tested; Go bridge node cases call real Go |
| M9 | fixed (D-044) | single-URL cluster probing like rueidis; embedded NATS supervised, output bounded |
| Low | fixed, except D-045/D-046 (hyper parser limits, unread bodies) | nested JSON up to Go's 10,000 levels; HTTP framing, WebSocket close codes and Redis log/keepalive items match Go |
