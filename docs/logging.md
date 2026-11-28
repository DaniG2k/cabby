# Logging parity

Cabby writes the log lines Go writes: same level, message, attributes, attribute order
and context attributes (`nodeid`, `context=…`, `sid=…`, `component=hub`, `provider=…`,
…). This page describes how logging is wired and lists every log call site of the Go
reference (anycable/anycable@26a1a703) with its Cabby counterpart. It closes review
finding H7 (`docs/review/2026-11-fable-review.md`).

## How it works

- **One process logger.** `cabby_core::logger` holds the sink, a `Record` type and an
  atomic minimum level. The server's logger (`cabby-server` `metrics/printer.rs`, tint
  text or JSON) installs itself as the sink, prepends the runner's attributes
  (`nodeid`, as Go's `slog.With("nodeid", id)`) and keeps the level gate in sync with
  `--log_level`/`--debug`.
- **Context loggers.** `cabby_core::logger::Logger` is Go's `*slog.Logger` built with
  `With(...)`: `Logger::default().with("context", "node")`, then `.with("sid", uid)` for a
  session. Cloning is an `Arc` clone. `Logger::root()` is a logger without the runner's
  attributes, for the few Go components that log through `slog.With(...)` directly (the
  HTTP RPC circuit breaker).
- **Lazy attributes.** `log_debug!`, `log_info!`, `log_warn!` and `log_error!` check the
  level before evaluating the logger expression or building any attribute. A disabled
  debug record costs one atomic load and a comparison (pinned by
  `cabby-core` `logger::tests::disabled_records_build_no_attributes`).
- **Log values.** `LogValuer` implementations in `cabby_core::common` render messages,
  replies, results and remote commands as Go's `LogValue` methods do. Values without a
  `LogValue` method in Go (`HistoryRequest`, `PresenceEvent`, Pusher payloads) are rendered
  as Go's `%+v` (`cabby_core::logger::plus_v`).
- **Go's error texts.** Where an error text is Go's own (not a library's), Cabby
  reproduces it: `encoding/json` syntax errors (`cabby_core::gojson::syntax`, used for
  client frames, pub/sub payloads and JWT segments), top-level JSON type errors of
  pub/sub payloads, golang-jwt's errors, `connect: connection refused` dial errors.
  Library texts that differ are deviation D-050.

## Hot paths

Per-message records (`incoming message`, `controller …`, the hub's `schedule
broadcast`/`no sessions`, `incoming broadcast message`, pub/sub `received …`, `publish
message`) are all behind the level check. At the default level the broadcast fan-out
adds three atomic loads per broadcast (node, gate) and no allocation; the per-frame
client path adds one.

## Tests

- `conformance::logging` (both backends): the review's logprobe scenario at the default
  level and with `--debug`, a broadcast/remote-command/whisper/presence scenario with the
  memory broker at `--debug` (shutdown with an active session included), failing RPC
  calls (`failed to handle incoming message`, `controller disconnect failed`), and the
  D-052 pin. Lines are normalised (timestamps, `nodeid`, session ids, ports, epoch) and
  compared as a multiset with the lines taken from Go.
- `conformance::logging_server` (both backends): RPC failures (HTTP and gRPC), JWT
  rejections, WebSocket upgrade failures, API/SSE/Durable Streams/streams records, metrics
  writers.
- `conformance::logging_transport` (both backends): Redis and NATS pub/sub and
  broadcasters, NATS reconnects per component, NATS broker consumers.

## Deviations

| ID | What differs |
|---|---|
| D-035 | the start line names Cabby |
| D-050 | library error texts inside `error=` attributes |
| D-051 | `handle batch-broadcast message` lists the messages, not element addresses |
| D-052 | Redis publishing failures are logged (Go ignores them) |
| D-053 | embedded NATS server records come from the child process |
| D-056 | `failed to handle incoming message` with `error="too many deeply nested messages in progress"` for a deep message dropped at the cap |

Lines Cabby logs that Go does not, all recorded: the D-003 Redis resubscription warnings,
D-044's `embedded NATS server exited unexpectedly`, D-052's publishing failures,
D-056's dropped deep messages, D-117's `shutdown step panicked` (error, with `error=` the
panic message; a Go process ends at the panic).

## Coverage

Go call sites: every `Debug`/`Info`/`Warn`/`Error` call in the packages Cabby ports
(test files, mocks, `cmd/`, `etc/`, `gobench/` and vendored code excluded): **308**.
Matched: **298** (same message, level and attributes; a deviation ID in the status marks
a recorded difference in an attribute value). Not applicable: **10** (6 of them removed with telemetry, D-058).

| Package | Go call sites | Matched | Not applicable |
|---|---|---|---|
| api | 8 | 8 | 0 |
| broadcast | 29 | 29 | 0 |
| broker | 19 | 19 | 0 |
| cli | 25 | 24 | 1 |
| config | 1 | 1 | 0 |
| ds | 22 | 21 | 1 |
| enats | 9 | 9 | 0 |
| hub | 10 | 10 | 0 |
| identity | 3 | 3 | 0 |
| metrics | 13 | 12 | 1 |
| node | 71 | 70 | 1 |
| pubsub | 31 | 31 | 0 |
| pusher | 13 | 13 | 0 |
| redis | 16 | 16 | 0 |
| rpc | 12 | 12 | 0 |
| server | 2 | 2 | 0 |
| sse | 9 | 9 | 0 |
| streams | 4 | 4 | 0 |
| telemetry | 6 | 0 | 6 |
| ws | 5 | 5 | 0 |

Line numbers refer to the pinned Go commit and to Cabby at the time of writing; Cabby
call sites are found by the message text (`grep -rn '"<message>"' crates`).

| Go call site | Level | Message | Cabby call site | Status | Note |
|---|---|---|---|---|---|
| `api/server.go:114` | INF | `Handle API requests at %s%s (%s)` | `crates/cabby-server/src/cli/runner.rs:887` | matched |  |
| `api/server.go:168` | DBG | `invalid request method` | `crates/cabby-server/src/api.rs:317`, `crates/cabby-server/src/api.rs:352`, `crates/cabby-server/src/pusher.rs:1209` | matched |  |
| `api/server.go:186` | DBG | `request body is too large` | `crates/cabby-server/src/api.rs:334`, `crates/cabby-server/src/pusher.rs:1274`, `crates/cabby-server/src/broadcast.rs:202`, `crates/cabby-server/src/broadcast.rs:230` | matched |  |
| `api/server.go:191` | ERR | `failed to read request body` | `crates/cabby-server/src/pusher.rs:1278`, `crates/cabby-server/src/server/mounts.rs:61`, `crates/cabby-server/src/broadcast.rs:236` | matched |  |
| `api/server.go:199` | ERR | `failed to handle broadcast` | `crates/cabby-server/src/api.rs:341` | matched |  |
| `api/server.go:209` | DBG | `invalid request method` | `crates/cabby-server/src/api.rs:317`, `crates/cabby-server/src/api.rs:352`, `crates/cabby-server/src/pusher.rs:1209` | matched |  |
| `api/server.go:222` | DBG | `invalid path for get users` | `crates/cabby-server/src/api.rs:361`, `crates/cabby-server/src/pusher.rs:1395` | matched |  |
| `api/server.go:238` | DBG | `failed to get presence info` | `crates/cabby-server/src/api.rs:369`, `crates/cabby-server/src/api.rs:375`, `crates/cabby-server/src/pusher.rs:1417` | matched |  |
| `broadcast/http.go:130` | INF | `auto-generated authorization secret from the application secret` | `crates/cabby-server/src/rpc/mod.rs:415`, `crates/cabby-server/src/broadcast.rs:175` | matched |  |
| `broadcast/http.go:180` | INF | `Accept broadcast requests at %s%s (%s)` | `crates/cabby-server/src/broadcast.rs:308` | matched |  |
| `broadcast/http.go:216` | DBG | `invalid request method` | `crates/cabby-server/src/api.rs:317`, `crates/cabby-server/src/api.rs:352`, `crates/cabby-server/src/pusher.rs:1209` | matched |  |
| `broadcast/http.go:237` | DBG | `request body is too large` | `crates/cabby-server/src/api.rs:334`, `crates/cabby-server/src/pusher.rs:1274`, `crates/cabby-server/src/broadcast.rs:202`, `crates/cabby-server/src/broadcast.rs:230` | matched |  |
| `broadcast/http.go:242` | ERR | `failed to read request body` | `crates/cabby-server/src/pusher.rs:1278`, `crates/cabby-server/src/server/mounts.rs:61`, `crates/cabby-server/src/broadcast.rs:236` | matched |  |
| `broadcast/legacy_nats.go:63` | WRN | `connection failed` | `crates/cabby-core/src/nats/client.rs:147` | matched (D-050) | error text ('disconnected') |
| `broadcast/legacy_nats.go:67` | INF | `connection restored` | `crates/cabby-core/src/nats/client.rs:151` | matched |  |
| `broadcast/legacy_nats.go:82` | DBG | `received pubsub message` | `crates/cabby-server/src/broadcast.rs:489`, `crates/cabby-server/src/broadcast.rs:613` | matched |  |
| `broadcast/legacy_nats.go:91` | INF | `subscribing for broadcasts` | `crates/cabby-server/src/broadcast.rs:621` | matched |  |
| `broadcast/legacy_redis.go:77` | INF | `Starting Redis pub/sub (sentinels): %v` | `cabby-server/src/broadcast.rs` via `redis/mod.rs` | matched |  |
| `broadcast/legacy_redis.go:79` | INF | `Starting Redis pub/sub (cluster): %v` | `cabby-server/src/broadcast.rs` via `redis/mod.rs` | matched |  |
| `broadcast/legacy_redis.go:81` | DBG | `Starting Redis pub/sub: %s` | `cabby-server/src/broadcast.rs` via `redis/mod.rs` | matched | debug for a single instance, as Go |
| `broadcast/legacy_redis.go:98` | DBG | `shutting down Redis pub/sub` | `crates/cabby-server/src/pubsub.rs:212`, `crates/cabby-server/src/broadcast.rs:548` | matched |  |
| `broadcast/legacy_redis.go:129` | ERR | `failed to connect to Redis` | `crates/cabby-server/src/redis/subscription.rs:244`, `crates/cabby-server/src/redis/streamer.rs:222`, `crates/cabby-server/src/redis/publisher.rs:153` | matched |  |
| `broadcast/legacy_redis.go:137` | DBG | `initialized pub/sub client` | `crates/cabby-server/src/redis/subscription.rs:247` | matched |  |
| `broadcast/legacy_redis.go:143` | INF | `reconnected to Redis channel` | `crates/cabby-server/src/broadcast.rs:503` | matched |  |
| `broadcast/legacy_redis.go:145` | INF | `subscribed to Redis channel` | `crates/cabby-server/src/broadcast.rs:502` | matched |  |
| `broadcast/legacy_redis.go:150` | DBG | `s.log.With("channel", m.Channel).Debug(m.Kind)` | `cabby-server/src/redis/subscription.rs` (subscribe/unsubscribe pushes) | matched |  |
| `broadcast/legacy_redis.go:153` | DBG | `received pubsub message` | `crates/cabby-server/src/broadcast.rs:489`, `crates/cabby-server/src/broadcast.rs:613` | matched |  |
| `broadcast/legacy_redis.go:161` | ERR | `failed to subscribe to Redis channel` | `crates/cabby-server/src/broadcast.rs:505` | matched |  |
| `broadcast/legacy_redis.go:170` | ERR | `Redis subscription error` | `crates/cabby-server/src/broadcast.rs:507` | matched |  |
| `broadcast/legacy_redis.go:173` | WRN | `Redis connection failed` | `crates/cabby-server/src/broadcast.rs:508` | matched |  |
| `broadcast/legacy_redis.go:178` | DBG | `close pub/sub channel` | `crates/cabby-server/src/redis/subscription.rs:253` | matched |  |
| `broadcast/legacy_redis.go:201` | INF | `next Redis reconnect attempt in %s` | `crates/cabby-server/src/redis/subscription.rs:279`, `crates/cabby-server/src/redis/streamer.rs:378` | matched |  |
| `broadcast/legacy_redis.go:204` | INF | `reconnecting to Redis...` | `crates/cabby-server/src/redis/subscription.rs:284`, `crates/cabby-server/src/redis/streamer.rs:387` | matched |  |
| `broadcast/redis.go:95` | INF | `Starting Redis broadcaster at %v (sentinels)` | `crates/cabby-server/src/broadcast.rs:424`, `crates/cabby-server/src/broadcast.rs:426`, `crates/cabby-server/src/broadcast.rs:428` | matched |  |
| `broadcast/redis.go:97` | INF | `Starting Redis broadcaster at %v (cluster)` | `crates/cabby-server/src/broadcast.rs:424`, `crates/cabby-server/src/broadcast.rs:426`, `crates/cabby-server/src/broadcast.rs:428` | matched |  |
| `broadcast/redis.go:99` | INF | `Starting Redis broadcaster at %s` | `crates/cabby-server/src/broadcast.rs:424`, `crates/cabby-server/src/broadcast.rs:426`, `crates/cabby-server/src/broadcast.rs:428` | matched |  |
| `broadcast/redis.go:111` | DBG | `received broadcast` | `crates/cabby-server/src/broadcast.rs:393` | matched |  |
| `broker/nats.go:115` | WRN | `connection failed` | `crates/cabby-core/src/nats/client.rs:147` | matched (D-050) | error text ('disconnected') |
| `broker/nats.go:119` | INF | `connection restored` | `crates/cabby-core/src/nats/client.rs:151` | matched |  |
| `broker/nats.go:201` | WRN | `JetStream initialization failed` | `crates/cabby-core/src/broker/nats.rs:583` | matched |  |
| `broker/nats.go:203` | INF | `next JetStream initialization attempt in %s` | `crates/cabby-core/src/broker/nats.rs:584` | matched |  |
| `broker/nats.go:206` | INF | `re-initializing JetStream...` | `crates/cabby-core/src/broker/nats.rs:586` | matched |  |
| `broker/nats.go:247` | WRN | `failed to set up epoch watcher` | `crates/cabby-core/src/broker/nats.rs:602` | matched |  |
| `broker/nats.go:250` | INF | `NATS broker is ready` | `crates/cabby-core/src/broker/nats.rs:605` | matched |  |
| `broker/nats.go:323` | DBG | `JetStream is not ready yet to publish messages, add to backlog` | `crates/cabby-core/src/broker/nats.rs:1026` | matched |  |
| `broker/nats.go:537` | ERR | `failed to create JetStream stream` | `crates/cabby-core/src/broker/nats.rs:745` | matched |  |
| `broker/nats.go:550` | ERR | `failed to create JetStream stream consumer` | `crates/cabby-core/src/broker/nats.rs:780` | matched |  |
| `broker/nats.go:554` | DBG | `created JetStream consumer` | `crates/cabby-core/src/broker/nats.rs:782` | matched |  |
| `broker/nats.go:567` | ERR | `failed to fetch initial messages from JetStream` | `crates/cabby-core/src/broker/nats.rs:787` | matched |  |
| `broker/nats.go:586` | DBG | `deleting JetStream consumer` | `crates/cabby-core/src/broker/nats.rs:815` | matched |  |
| `broker/nats.go:603` | ERR | `failed to get JetStream message metadata` | `crates/cabby-core/src/broker/nats.rs:826` | matched |  |
| `broker/nats.go:612` | ERR | `failed to store message in local broker` | `crates/cabby-core/src/broker/nats.rs:833` | matched |  |
| `broker/nats.go:707` | WRN | `epoch updated` | `crates/cabby-core/src/broker/nats.rs:621` | matched |  |
| `broker/nats.go:734` | WRN | `failed to retrieve bucket, retrying in 500ms...` | `crates/cabby-core/src/broker/nats.rs:941` | matched |  |
| `broker/nats.go:767` | WRN | `bucket TTL has been changed, recreating the bucket` | `crates/cabby-core/src/broker/nats.rs:959` | matched |  |
| `broker/nats.go:972` | WRN | `operation failed, retrying in %s...` | `crates/cabby-core/src/broker/nats.rs:152` | matched |  |
| `cli/cli.go:170` | INF | `Starting %s %s%s (pid: %d, open file limit: %s, gomaxprocs: %d)` | `crates/cabby-server/src/cli/runner.rs:304` | matched (D-035) | the start line names Cabby |
| `cli/cli.go:173` | INF | `Using configuration from file: %s` | `crates/cabby-server/src/cli/runner.rs:313` | matched |  |
| `cli/cli.go:176` | WRN | `Unknown keys in configuration file (ignored): %s` | `crates/cabby-server/src/cli/runner.rs:318` | matched |  |
| `cli/cli.go:181` | WRN | `Server is running in the public mode` | `crates/cabby-server/src/cli/runner.rs:339` | matched |  |
| `cli/cli.go:185` | WRN | `Redis server TLS certificate is not verified; set --redis_tls_verify=true to enable ver...` | `crates/cabby-server/src/cli/runner.rs:961` | matched |  |
| `cli/cli.go:206` | INF | `Handle WebSocket connections at %s%s` | `crates/cabby-server/src/cli/runner.rs:360` | matched |  |
| `cli/cli.go:227` | INF | `Handle Pusher WebSocket connections at %s%s` | `crates/cabby-server/src/cli/runner.rs:823` | matched |  |
| `cli/cli.go:234` | INF | `Handle Pusher API requests at %s%s` | `crates/cabby-server/src/pusher.rs:1559`, `crates/cabby-server/src/cli/runner.rs:831` | matched |  |
| `cli/cli.go:253` | INF | `Handle health requests at %s%s` | `crates/cabby-server/src/cli/runner.rs:368` | matched |  |
| `cli/cli.go:256` | INF | `Handle SSE requests at %s%s` | `crates/cabby-server/src/cli/runner.rs:371` | matched |  |
| `cli/cli.go:273` | INF | `Handle Durable Streams requests at %s%s` | `crates/cabby-server/src/cli/runner.rs:383` | matched |  |
| `cli/cli.go:331` | INF | `r.log.With("context", "telemetry").Info(tracker.Announce())` | — | n/a | removed with telemetry (D-058) |
| `cli/cli.go:350` | INF | `r.log.Info(appBroker.Announce())` | `cabby-server/src/cli/runner.rs` (broker announce) | matched |  |
| `cli/cli.go:376` | INF | `Embedded NATS server started: %s%s` | `crates/cabby-server/src/cli/runner.rs:513` | matched |  |
| `cli/cli.go:441` | DBG | `🔧 🔧 🔧 Debug mode is on 🔧 🔧 🔧` | `crates/cabby-server/src/cli/runner.rs:580` | matched |  |
| `cli/cli.go:470` | INF | `JWT authentication is enabled (param: %s, enforced: %v)` | `crates/cabby-server/src/cli/runner.rs:602` | matched |  |
| `cli/cli.go:475` | INF | `connection authentication is disabled` | `crates/cabby-server/src/cli/runner.rs:606` | matched |  |
| `cli/cli.go:488` | INF | `Using channels router: %s` | `crates/cabby-server/src/cli/runner.rs:642` | matched |  |
| `cli/cli.go:553` | INF | `API server is running without authentication` | `crates/cabby-server/src/cli/runner.rs:900` | matched |  |
| `cli/cli.go:681` | ERR | `mruby failed to initialize: %v` | `crates/cabby-server/src/cli/runner.rs:1072` | matched |  |
| `cli/cli.go:736` | DBG | `Go pools initialized (%s)` | `crates/cabby-server/src/cli/runner.rs:586` | matched |  |
| `cli/cli.go:743` | WRN | `Immediate termination requested. Stopped` | `crates/cabby-server/src/cli/signals.rs:97` | matched |  |
| `cli/cli.go:748` | INF | `Shutting down... (hit Ctrl-C to stop immediately or wait for up to %ds for graceful shu...` | `crates/cabby-server/src/cli/signals.rs:79` | matched |  |
| `cli/embed.go:74` | INF | `Starting embedded %s %s%s (open file limit: %s)` | `crates/cabby-server/src/cli/runner.rs:561` | matched |  |
| `cli/session_options.go:52` | WRN | `invalid ping interval value, must be a positive integer` | `crates/cabby-server/src/cli/session_options.rs:106` | matched |  |
| `config/presets.go:30` | INF | `load presets` | `crates/cabby-server/src/cli/runner.rs:194` | matched |  |
| `ds/ds.go:242` | DBG | `skip authentication` | `crates/cabby-server/src/ds.rs:1283` | matched |  |
| `ds/handler.go:60` | DBG | `invalid stream request` | `crates/cabby-server/src/ds.rs:1319` | matched |  |
| `ds/handler.go:67` | DBG | `non-read operations are not supported` | `crates/cabby-server/src/ds.rs:1327` | matched |  |
| `ds/handler.go:82` | DBG | `unauthenticated` | `crates/cabby-server/src/ds.rs:1344` | matched |  |
| `ds/handler.go:99` | DBG | `unauthorized` | `crates/cabby-core/src/common.rs:680`, `crates/cabby-server/src/ds.rs:1363`, `crates/cabby-server/src/ds.rs:1364`, `crates/cabby-server/src/ds.rs:1366` | matched |  |
| `ds/handler.go:106` | DBG | `session established` | `crates/cabby-server/src/ds.rs:1370`, `crates/cabby-server/src/sse.rs:973` | matched |  |
| `ds/handler.go:108` | DBG | `session completed` | `crates/cabby-server/src/ds.rs:1181`, `crates/cabby-server/src/sse.rs:1009` | matched |  |
| `ds/handler.go:115` | ERR | `failed to get stream metadata` | `crates/cabby-server/src/ds.rs:1376` | matched |  |
| `ds/handler.go:159` | ERR | `failed to fetch history` | `crates/cabby-server/src/ds.rs:1427`, `crates/cabby-server/src/ds.rs:1467`, `crates/cabby-server/src/ds.rs:1605` | matched |  |
| `ds/handler.go:183` | DBG | `not modified` | `crates/cabby-server/src/ds.rs:1510` | matched |  |
| `ds/handler.go:189` | DBG | `write history` | `crates/cabby-server/src/ds.rs:1516`, `crates/cabby-server/src/ds.rs:1619` | matched |  |
| `ds/handler.go:211` | ERR | `failed to subscribe` | `crates/cabby-core/src/nats/client.rs:367`, `crates/cabby-server/src/ds.rs:1459`, `crates/cabby-server/src/ds.rs:1597` | matched |  |
| `ds/handler.go:228` | DBG | `server shutdown` | `crates/cabby-server/src/ds.rs:1653`, `crates/cabby-server/src/ds.rs:1676`, `crates/cabby-server/src/sse.rs:992` | matched |  |
| `ds/handler.go:235` | DBG | `polling request terminated` | `crates/cabby-server/src/ds.rs:1661` | matched |  |
| `ds/handler.go:238` | DBG | `polling request fulfilled` | `crates/cabby-server/src/ds.rs:1665` | matched |  |
| `ds/handler.go:247` | ERR | `streaming not supported` | — | n/a | Go checks for 'http.Flusher'; Cabby's response writer always streams |
| `ds/handler.go:268` | ERR | `failed to fetch history` | `crates/cabby-server/src/ds.rs:1427`, `crates/cabby-server/src/ds.rs:1467`, `crates/cabby-server/src/ds.rs:1605` | matched |  |
| `ds/handler.go:284` | DBG | `write history` | `crates/cabby-server/src/ds.rs:1516`, `crates/cabby-server/src/ds.rs:1619` | matched |  |
| `ds/handler.go:297` | ERR | `failed to subscribe` | `crates/cabby-core/src/nats/client.rs:367`, `crates/cabby-server/src/ds.rs:1459`, `crates/cabby-server/src/ds.rs:1597` | matched |  |
| `ds/handler.go:312` | DBG | `server shutdown` | `crates/cabby-server/src/ds.rs:1653`, `crates/cabby-server/src/ds.rs:1676`, `crates/cabby-server/src/sse.rs:992` | matched |  |
| `ds/handler.go:316` | DBG | `client disconnected` | `crates/cabby-server/src/ds.rs:1680` | matched |  |
| `ds/handler.go:322` | DBG | `connection closed` | `crates/cabby-server/src/ds.rs:1685`, `crates/cabby-server/src/redis/subscription.rs:368`, `crates/cabby-server/src/redis/subscription.rs:455` | matched |  |
| `enats/enats.go:54` | DBG | `e.Debug(fmt.Sprintf(format, v...))` | `cabby-server/src/enats.rs` (`server_log_record`) | matched (D-053) | nats-server child output forwarded at the mapped level in debug mode |
| `enats/enats.go:59` | WRN | `e.Warn(fmt.Sprintf(format, v...))` | `cabby-server/src/enats.rs` (`server_log_record`) | matched (D-053) | as above |
| `enats/enats.go:64` | ERR | `e.Error(fmt.Sprintf(format, v...))` | `cabby-server/src/enats.rs` (`server_log_record`) | matched (D-053) | as above |
| `enats/enats.go:69` | INF | `e.Info(fmt.Sprintf(format, v...))` | `cabby-server/src/enats.rs` (`server_log_record`) | matched (D-053) | as above |
| `enats/enats.go:79` | INF | `e.Info(fmt.Sprintf(format, v...))` | `cabby-server/src/enats.rs` (`server_log_record`) | matched (D-053) | as above (Noticef) |
| `enats/enats.go:84` | DBG | `e.Debug(fmt.Sprintf(format, v...))` | `cabby-server/src/enats.rs` (`server_log_record`) | matched (D-053) | as above (Tracef) |
| `enats/enats.go:350` | DBG | `NATS server not accepting connections` | `crates/cabby-server/src/enats.rs:774` | matched |  |
| `enats/enats.go:362` | DBG | `JetStream cluster is ready` | `crates/cabby-server/src/enats.rs:782` | matched (D-053) | 'leader=' cannot be read from a child process |
| `enats/enats.go:368` | DBG | `JetStream cluster is not ready yet, waiting for 1 second...` | `crates/cabby-server/src/enats.rs:787` | matched |  |
| `hub/gate.go:52` | DBG | `schedule broadcast` | `crates/cabby-core/src/hub.rs:359` | matched |  |
| `hub/gate.go:56` | DBG | `no sessions` | `crates/cabby-core/src/hub.rs:361` | matched |  |
| `hub/hub.go:201` | DBG | `registered` | `crates/cabby-core/src/hub.rs:643` | matched |  |
| `hub/hub.go:212` | WRN | `session hasn't been registered` | `crates/cabby-core/src/hub.rs:651` | matched |  |
| `hub/hub.go:231` | DBG | `unregistered` | `crates/cabby-core/src/hub.rs:664` | matched |  |
| `hub/hub.go:273` | DBG | `unsubscribed` | `crates/cabby-core/src/hub.rs:691`, `crates/cabby-core/src/hub.rs:728`, `crates/cabby-server/src/node/app.rs:940` | matched |  |
| `hub/hub.go:283` | ERR | `attempted to subscribe a session without registration` | `crates/cabby-core/src/hub.rs:709` | matched |  |
| `hub/hub.go:290` | DBG | `subscribed` | `crates/cabby-core/src/hub.rs:713`, `crates/cabby-server/src/node/app.rs:844` | matched |  |
| `hub/hub.go:305` | DBG | `unsubscribed` | `crates/cabby-core/src/hub.rs:691`, `crates/cabby-core/src/hub.rs:728`, `crates/cabby-server/src/node/app.rs:940` | matched |  |
| `hub/hub.go:318` | DBG | `cannot disconnect session` | `crates/cabby-core/src/hub.rs:563` | matched |  |
| `identity/jwt.go:115` | DBG | `no token is found` | `crates/cabby-core/src/identity.rs:302` | matched |  |
| `identity/jwt.go:134` | DBG | `token has expired` | `crates/cabby-core/src/identity.rs:315` | matched |  |
| `identity/jwt.go:139` | DBG | `invalid token` | `crates/cabby-core/src/identity.rs:319` | matched |  |
| `metrics/custom_printer.go:40` | INF | `Log metrics every %ds using a custom Ruby formatter from %s` | `crates/cabby-server/src/metrics/custom_printer.rs:97` | matched |  |
| `metrics/custom_printer.go:68` | ERR | `mruby call failed` | `crates/cabby-server/src/metrics/custom_printer.rs:147` | matched |  |
| `metrics/custom_printer.go:72` | INF | `p.log.Info(result.String())` | `cabby-server/src/metrics/custom_printer.rs` (formatter output) | matched |  |
| `metrics/metrics.go:130` | INF | `Serve metrics at %s%s` | `crates/cabby-server/src/metrics/mod.rs:268`, `crates/cabby-server/src/cli/runner.rs:924` | matched |  |
| `metrics/metrics.go:142` | DBG | `no metrics writers, disabling metrics rotation` | `crates/cabby-server/src/metrics/mod.rs:282` | matched |  |
| `metrics/metrics.go:175` | DBG | `rotate metrics` | `crates/cabby-server/src/metrics/mod.rs:312` | matched |  |
| `metrics/metrics.go:180` | ERR | `metrics writer failed to write` | `crates/cabby-server/src/metrics/mod.rs:324` | matched |  |
| `metrics/printer.go:40` | INF | `log metrics` | `crates/cabby-server/src/metrics/printer.rs:234` | matched |  |
| `metrics/printer.go:42` | INF | `log metrics` | `crates/cabby-server/src/metrics/printer.rs:234` | matched |  |
| `metrics/printer.go:74` | INF | `` | `cabby-server/src/metrics/printer.rs` (snapshot record) | matched |  |
| `metrics/statsd_writer.go:27` | ERR | `lg.log.Error(fmt.Sprintf(msg, args...))` | `cabby-server/src/metrics/statsd.rs` (`service=statsd`) | matched | error texts: D-050 |
| `metrics/statsd_writer.go:29` | WRN | `lg.log.Warn(fmt.Sprintf(msg, args...))` | `cabby-server/src/metrics/statsd.rs` (`<n> packets lost (overflow)`, `service=statsd`) | matched | go-statsd's only warning: packets dropped because the send queue was full, reported once a minute |
| `metrics/statsd_writer.go:86` | INF | `sw.log.Info(` | `cabby-server/src/metrics/statsd.rs` (`Send statsd metrics to ...`) | matched |  |
| `node/controller.go:35` | INF | `no RPC configured` | `crates/cabby-server/src/node/app.rs:1502` | matched |  |
| `node/controller.go:43` | DBG | `reject connection` | `crates/cabby-server/src/node/app.rs:1509` | matched |  |
| `node/controller.go:53` | DBG | `reject subscription` | `crates/cabby-server/src/node/app.rs:1521` | matched |  |
| `node/disconnect_queue.go:80` | DBG | `calls rate` | `crates/cabby-server/src/node/disconnect_queue.rs:118` | matched |  |
| `node/disconnect_queue.go:165` | INF | `disconnected sessions` | `crates/cabby-server/src/node/disconnect_queue.rs:211` | matched |  |
| `node/disconnect_queue.go:173` | INF | `invoking remaining disconnects` | `crates/cabby-server/src/node/disconnect_queue.rs:194`, `crates/cabby-server/src/node/disconnect_queue.rs:198` | matched |  |
| `node/disconnect_queue.go:175` | INF | `invoking remaining disconnects` | `crates/cabby-server/src/node/disconnect_queue.rs:194`, `crates/cabby-server/src/node/disconnect_queue.rs:198` | matched |  |
| `node/node.go:178` | DBG | `incoming message` | `crates/cabby-server/src/node/app.rs:1428` | matched |  |
| `node/node.go:219` | WRN | `failed to parse pubsub message` | `crates/cabby-server/src/node/app.rs:605`, `crates/cabby-server/src/node/app.rs:641`, `crates/cabby-server/src/pubsub.rs:136` | matched |  |
| `node/node.go:227` | DBG | `handle broadcast message` | `crates/cabby-server/src/node/app.rs:616` | matched |  |
| `node/node.go:230` | DBG | `handle batch-broadcast message` | `crates/cabby-server/src/node/app.rs:620` | matched (D-051) | payload lists the messages instead of element addresses |
| `node/node.go:237` | DBG | `handle remote command` | `crates/cabby-server/src/node/app.rs:629` | matched |  |
| `node/node.go:250` | WRN | `failed to parse pubsub message` | `crates/cabby-server/src/node/app.rs:605`, `crates/cabby-server/src/node/app.rs:641`, `crates/cabby-server/src/pubsub.rs:136` | matched |  |
| `node/node.go:289` | INF | `closing active connections` | `crates/cabby-server/src/node/app.rs:462` | matched |  |
| `node/node.go:300` | WRN | `failed to shutdown disconnector gracefully` | `crates/cabby-server/src/node/app.rs:470` | matched |  |
| `node/node.go:308` | WRN | `failed to shutdown controller gracefully` | `crates/cabby-server/src/node/app.rs:474` | matched |  |
| `node/node.go:371` | DBG | `skip authenticate result: closed` | `crates/cabby-server/src/node/app.rs:684` | matched |  |
| `node/node.go:379` | DBG | `controller authenticate` | `crates/cabby-server/src/node/app.rs:692` | matched |  |
| `node/node.go:403` | ERR | `failed to persist session in cache` | `crates/cabby-server/src/node/app.rs:658` | matched |  |
| `node/node.go:438` | ERR | `failed to fetch session cache` | `crates/cabby-server/src/node/app.rs:751` | matched |  |
| `node/node.go:443` | DBG | `session not found in cache` | `crates/cabby-server/src/node/app.rs:747` | matched |  |
| `node/node.go:448` | DBG | `skip restore result: closed` | `crates/cabby-server/src/node/app.rs:757` | matched |  |
| `node/node.go:455` | ERR | `failed to restore session from cache` | `crates/cabby-server/src/node/app.rs:762` | matched |  |
| `node/node.go:459` | DBG | `session restored` | `crates/cabby-server/src/node/app.rs:766` | matched |  |
| `node/node.go:489` | ERR | `failed to persist session in cache` | `crates/cabby-server/src/node/app.rs:658` | matched |  |
| `node/node.go:508` | DBG | `skip subscribe result: closed` | `crates/cabby-server/src/node/app.rs:824` | matched |  |
| `node/node.go:517` | DBG | `controller subscribe` | `crates/cabby-server/src/node/app.rs:832` | matched |  |
| `node/node.go:529` | DBG | `subscribed` | `crates/cabby-core/src/hub.rs:713`, `crates/cabby-server/src/node/app.rs:844` | matched |  |
| `node/node.go:531` | DBG | `subscription rejected` | `crates/cabby-server/src/node/app.rs:847`, `crates/cabby-server/src/sse.rs:944` | matched |  |
| `node/node.go:544` | ERR | `failed to persist session in cache` | `crates/cabby-server/src/node/app.rs:658` | matched |  |
| `node/node.go:550` | WRN | `couldn't retrieve history` | `crates/cabby-server/src/node/app.rs:866` | matched |  |
| `node/node.go:556` | WRN | `couldn't process presence join` | `crates/cabby-server/src/node/app.rs:873` | matched |  |
| `node/node.go:576` | DBG | `skip unsubscribe result: closed` | `crates/cabby-server/src/node/app.rs:913` | matched |  |
| `node/node.go:585` | DBG | `controller unsubscribe` | `crates/cabby-server/src/node/app.rs:921` | matched |  |
| `node/node.go:602` | DBG | `unsubscribed` | `crates/cabby-core/src/hub.rs:691`, `crates/cabby-core/src/hub.rs:728`, `crates/cabby-server/src/node/app.rs:940` | matched |  |
| `node/node.go:618` | ERR | `failed to persist session in cache` | `crates/cabby-server/src/node/app.rs:658` | matched |  |
| `node/node.go:624` | DBG | `remove presence` | `crates/cabby-server/src/node/app.rs:957` | matched |  |
| `node/node.go:626` | ERR | `failed to remove presence` | `crates/cabby-server/src/node/app.rs:959` | matched |  |
| `node/node.go:651` | DBG | `skip perform result: closed` | `crates/cabby-server/src/node/app.rs:983` | matched |  |
| `node/node.go:659` | DBG | `controller perform` | `crates/cabby-server/src/node/app.rs:991` | matched |  |
| `node/node.go:671` | ERR | `failed to persist session in cache` | `crates/cabby-server/src/node/app.rs:658` | matched |  |
| `node/node.go:699` | DBG | `skip history result: closed` | `crates/cabby-server/src/node/app.rs:1025` | matched |  |
| `node/node.go:771` | DBG | `whisper stream not found` | `crates/cabby-server/src/node/app.rs:1077` | matched |  |
| `node/node.go:787` | ERR | `failed to whisper` | `crates/cabby-server/src/node/app.rs:1090` | matched |  |
| `node/node.go:789` | DBG | `whispered` | `crates/cabby-server/src/node/app.rs:1091` | matched |  |
| `node/node.go:832` | DBG | `skip presence result: closed` | `crates/cabby-server/src/node/app.rs:1114` | matched |  |
| `node/node.go:886` | DBG | `incoming broadcast message` | `crates/cabby-server/src/node/app.rs:1143` | matched |  |
| `node/node.go:897` | WRN | `failed to parse remote disconnect command` | `crates/cabby-server/src/node/app.rs:1156` | matched |  |
| `node/node.go:901` | DBG | `incoming remote command` | `crates/cabby-server/src/node/app.rs:1152` | matched |  |
| `node/node.go:954` | DBG | `unsubscribed from broker streams` | `crates/cabby-server/src/node/app.rs:1182` | matched |  |
| `node/node.go:968` | DBG | `disconnect` | `crates/cabby-core/src/common/json.rs:689`, `crates/cabby-core/src/common.rs:474`, `crates/cabby-core/src/common.rs:666` | matched |  |
| `node/node.go:979` | ERR | `controller disconnect failed` | `crates/cabby-server/src/node/app.rs:1221` | matched |  |
| `node/node.go:982` | DBG | `controller disconnect succeeded` | `crates/cabby-server/src/node/app.rs:1224` | matched |  |
| `node/node.go:990` | DBG | `incoming pubsub command` | `crates/cabby-server/src/node/app.rs:1231` | matched |  |
| `node/node.go:1094` | WRN | `failed to process presence` | `crates/cabby-server/src/node/app.rs:1332` | matched |  |
| `node/node.go:1142` | DBG | `presence event` | `crates/cabby-server/src/node/app.rs:1388` | matched |  |
| `node/node.go:1213` | WRN | `terminated while disconnecting active sessions` | `crates/cabby-server/src/node/app.rs:532` | matched |  |
| `node/node.go:1215` | INF | `all active connections closed` | `crates/cabby-server/src/node/app.rs:530` | matched |  |
| `node/session.go:365` | WRN | `disconnecting idle session` | `crates/cabby-server/src/node/session.rs:988` | matched |  |
| `node/session.go:459` | DBG | `WebSocket closed` | `crates/cabby-server/src/node/session.rs:1246` | matched (D-050) | error text |
| `node/session.go:462` | DBG | `WebSocket close error` | `crates/cabby-server/src/node/session.rs:1249` | matched (D-050) | error text |
| `node/session.go:471` | DBG | `WebSocket read failed` | `crates/cabby-server/src/node/session.rs:1231`, `crates/cabby-server/src/node/session.rs:1756` | matched |  |
| `node/session.go:503` | DBG | `write timed out` | `crates/cabby-server/src/node/session.rs:833` | matched (D-050) | error text |
| `node/session.go:534` | WRN | `failed to handle incoming message` | `crates/cabby-server/src/node/session.rs:1298`, `crates/cabby-server/src/node/session.rs:1355` | matched |  |
| `node/session.go:547` | WRN | `failed to encode message` | `crates/cabby-server/src/node/session.rs:1365` | matched |  |
| `node/session.go:559` | WRN | `failed to encode transmission` | `crates/cabby-server/src/node/session.rs:1376` | matched |  |
| `node/session.go:728` | DBG | `slow client detected, disconnecting` | `crates/cabby-server/src/node/session.rs:1450` | matched |  |
| `node/session.go:762` | ERR | `unknown frame type` | — | n/a | frame types are a closed Rust enum: no unknown frame type exists |
| `node/session.go:780` | ERR | `failed to encode ping message` | `crates/cabby-server/src/node/session.rs:1013` | matched |  |
| `node/session.go:903` | WRN | `disconnecting session due to no pongs` | `crates/cabby-server/src/node/session.rs:1069` | matched |  |
| `node/session.go:910` | DBG | `ping received` | `crates/cabby-server/src/node/session.rs:1076` | matched |  |
| `pubsub/nats.go:68` | WRN | `connection failed` | `crates/cabby-core/src/nats/client.rs:147` | matched (D-050) | error text ('disconnected') |
| `pubsub/nats.go:72` | INF | `connection restored` | `crates/cabby-core/src/nats/client.rs:151` | matched |  |
| `pubsub/nats.go:86` | INF | `Starting NATS pub/sub: %s` | `crates/cabby-server/src/pubsub.rs:308` | matched |  |
| `pubsub/nats.go:122` | ERR | `failed to subscribe` | `crates/cabby-core/src/nats/client.rs:367`, `crates/cabby-server/src/ds.rs:1459`, `crates/cabby-server/src/ds.rs:1597` | matched |  |
| `pubsub/nats.go:148` | DBG | `publish message` | `crates/cabby-server/src/pubsub.rs:143` | matched |  |
| `pubsub/nats.go:151` | ERR | `failed to publish message` | `crates/cabby-server/src/pubsub.rs:294` | matched |  |
| `pubsub/nats.go:159` | WRN | `failed to parse pubsub message` | `crates/cabby-server/src/node/app.rs:605`, `crates/cabby-server/src/node/app.rs:641`, `crates/cabby-server/src/pubsub.rs:136` | matched |  |
| `pubsub/nats.go:165` | DBG | `received broadcast message` | `crates/cabby-server/src/pubsub.rs:123` | matched |  |
| `pubsub/nats.go:168` | DBG | `received remote command` | `crates/cabby-server/src/pubsub.rs:127` | matched |  |
| `pubsub/nats.go:171` | WRN | `received unknown message` | `crates/cabby-server/src/pubsub.rs:132` | matched |  |
| `pubsub/redis.go:101` | INF | `Starting Redis pub/sub (sentinels): %v` | `cabby-server/src/pubsub.rs` via `redis/mod.rs` (starting announcement) | matched |  |
| `pubsub/redis.go:103` | INF | `Starting Redis pub/sub (cluster): %v` | `cabby-server/src/pubsub.rs` via `redis/mod.rs` | matched |  |
| `pubsub/redis.go:105` | INF | `Starting Redis pub/sub: %s` | `cabby-server/src/pubsub.rs` via `redis/mod.rs` | matched |  |
| `pubsub/redis.go:126` | DBG | `shutting down Redis pub/sub` | `crates/cabby-server/src/pubsub.rs:212`, `crates/cabby-server/src/broadcast.rs:548` | matched |  |
| `pubsub/redis.go:151` | DBG | `subscribing` | `crates/cabby-server/src/pubsub.rs:227` | matched |  |
| `pubsub/redis.go:171` | DBG | `unsubscribing` | `crates/cabby-server/src/pubsub.rs:233` | matched |  |
| `pubsub/redis.go:196` | DBG | `publish message` | `crates/cabby-server/src/pubsub.rs:143` | matched |  |
| `pubsub/redis.go:231` | ERR | `failed to connect to Redis` | `crates/cabby-server/src/redis/subscription.rs:244`, `crates/cabby-server/src/redis/streamer.rs:222`, `crates/cabby-server/src/redis/publisher.rs:153` | matched |  |
| `pubsub/redis.go:239` | DBG | `initialized pub/sub client` | `crates/cabby-server/src/redis/subscription.rs:247` | matched |  |
| `pubsub/redis.go:245` | INF | `reconnected` | `crates/cabby-server/src/pubsub.rs:157` | matched |  |
| `pubsub/redis.go:247` | INF | `connected` | `crates/cabby-server/src/redis/subscription.rs:415`, `crates/cabby-server/src/rpc/grpc.rs:583`, `crates/cabby-server/src/pubsub.rs:156` | matched |  |
| `pubsub/redis.go:252` | DBG | `s.log.With("channel", m.Channel).Debug(m.Kind)` | `cabby-server/src/redis/subscription.rs` (subscribe/unsubscribe pushes) | matched | D-003 periodic re-confirmations are not logged |
| `pubsub/redis.go:259` | WRN | `failed to parse pubsub message` | `crates/cabby-server/src/node/app.rs:605`, `crates/cabby-server/src/node/app.rs:641`, `crates/cabby-server/src/pubsub.rs:136` | matched |  |
| `pubsub/redis.go:265` | DBG | `received broadcast message` | `crates/cabby-server/src/pubsub.rs:123` | matched |  |
| `pubsub/redis.go:268` | DBG | `received remote command` | `crates/cabby-server/src/pubsub.rs:127` | matched |  |
| `pubsub/redis.go:284` | ERR | `Redis pub/sub disconnected` | `crates/cabby-server/src/pubsub.rs:160` | matched |  |
| `pubsub/redis.go:291` | DBG | `close pub/sub channel` | `crates/cabby-server/src/redis/subscription.rs:253` | matched |  |
| `pubsub/redis.go:319` | INF | `next Redis reconnect attempt in %s` | `crates/cabby-server/src/redis/subscription.rs:279`, `crates/cabby-server/src/redis/streamer.rs:378` | matched |  |
| `pubsub/redis.go:322` | INF | `reconnecting to Redis...` | `crates/cabby-server/src/redis/subscription.rs:284`, `crates/cabby-server/src/redis/streamer.rs:387` | matched |  |
| `pubsub/redis.go:341` | ERR | `failed to resubscribe` | `crates/cabby-server/src/pubsub.rs:159` | matched |  |
| `pubsub/redis.go:353` | ERR | `failed to resubscribe` | `crates/cabby-server/src/pubsub.rs:159` | matched |  |
| `pusher/executor.go:33` | DBG | `pusher authorization failed` | `crates/cabby-server/src/pusher.rs:913` | matched |  |
| `pusher/http.go:74` | INF | `Handle Pusher API requests at %s%s` | `crates/cabby-server/src/pusher.rs:1559`, `crates/cabby-server/src/cli/runner.rs:831` | matched |  |
| `pusher/http.go:165` | DBG | `invalid request method` | `crates/cabby-server/src/api.rs:317`, `crates/cabby-server/src/api.rs:352`, `crates/cabby-server/src/pusher.rs:1209` | matched |  |
| `pusher/http.go:180` | DBG | `invalid auth_timestamp` | `crates/cabby-server/src/pusher.rs:1227` | matched |  |
| `pusher/http.go:186` | DBG | `auth_timestamp out of range` | `crates/cabby-server/src/pusher.rs:1239` | matched |  |
| `pusher/http.go:225` | DBG | `request body is too large` | `crates/cabby-server/src/api.rs:334`, `crates/cabby-server/src/pusher.rs:1274`, `crates/cabby-server/src/broadcast.rs:202`, `crates/cabby-server/src/broadcast.rs:230` | matched |  |
| `pusher/http.go:230` | ERR | `failed to read request body` | `crates/cabby-server/src/pusher.rs:1278`, `crates/cabby-server/src/server/mounts.rs:61`, `crates/cabby-server/src/broadcast.rs:236` | matched |  |
| `pusher/http.go:237` | DBG | `body_md5 mismatch` | `crates/cabby-server/src/pusher.rs:1289` | matched |  |
| `pusher/http.go:257` | ERR | `failed to parse Pusher event` | `crates/cabby-server/src/pusher.rs:1311`, `crates/cabby-server/src/pusher.rs:1352` | matched |  |
| `pusher/http.go:270` | DBG | `event received` | `crates/cabby-server/src/pusher.rs:1369` | matched |  |
| `pusher/http.go:299` | DBG | `invalid path for get users` | `crates/cabby-server/src/api.rs:361`, `crates/cabby-server/src/pusher.rs:1395` | matched |  |
| `pusher/http.go:307` | DBG | `get users is only supported for presence channels` | `crates/cabby-server/src/pusher.rs:1402` | matched |  |
| `pusher/http.go:320` | DBG | `failed to get presence info` | `crates/cabby-server/src/api.rs:369`, `crates/cabby-server/src/api.rs:375`, `crates/cabby-server/src/pusher.rs:1417` | matched |  |
| `redis/streamer.go:112` | DBG | `shutting down Redis streamer` | `crates/cabby-server/src/redis/streamer.rs:147` | matched |  |
| `redis/streamer.go:126` | ERR | `failed to remove Redis stream consumer` | `crates/cabby-server/src/redis/streamer.rs:161`, `crates/cabby-server/src/redis/streamer.rs:162` | matched |  |
| `redis/streamer.go:172` | ERR | `failed to connect to Redis` | `crates/cabby-server/src/redis/subscription.rs:244`, `crates/cabby-server/src/redis/streamer.rs:222`, `crates/cabby-server/src/redis/publisher.rs:153` | matched |  |
| `redis/streamer.go:178` | INF | `reconnected to Redis` | `crates/cabby-server/src/redis/streamer.rs:231` | matched |  |
| `redis/streamer.go:191` | DBG | `Redis consumer group already exists` | `crates/cabby-server/src/redis/streamer.rs:247` | matched |  |
| `redis/streamer.go:193` | ERR | `failed to create consumer group` | `crates/cabby-server/src/redis/streamer.rs:249` | matched |  |
| `redis/streamer.go:208` | DBG | `stop consuming stream` | `crates/cabby-server/src/redis/streamer.rs:267` | matched |  |
| `redis/streamer.go:216` | ERR | `failed to claim from Redis stream` | `crates/cabby-server/src/redis/streamer.rs:275` | matched |  |
| `redis/streamer.go:224` | DBG | `reclaimed messages` | `crates/cabby-server/src/redis/streamer.rs:285` | matched |  |
| `redis/streamer.go:233` | ERR | `failed to read from Redis stream` | `crates/cabby-server/src/redis/streamer.rs:294` | matched |  |
| `redis/streamer.go:298` | DBG | `received message` | `crates/cabby-server/src/redis/streamer.rs:344` | matched |  |
| `redis/streamer.go:309` | ERR | `failed to ack message` | `crates/cabby-server/src/redis/streamer.rs:353`, `crates/cabby-server/src/redis/streamer.rs:356` | matched |  |
| `redis/streamer.go:312` | ERR | `Redis stream handler failed` | `crates/cabby-server/src/redis/streamer.rs:359` | matched |  |
| `redis/streamer.go:320` | ERR | `failed to reconnect to Redis: attempts exceeded` | `crates/cabby-server/src/redis/streamer.rs:369`, `crates/cabby-server/src/pubsub.rs:158` | matched |  |
| `redis/streamer.go:328` | INF | `next Redis reconnect attempt in %s` | `crates/cabby-server/src/redis/subscription.rs:279`, `crates/cabby-server/src/redis/streamer.rs:378` | matched |  |
| `redis/streamer.go:331` | INF | `reconnecting to Redis...` | `crates/cabby-server/src/redis/subscription.rs:284`, `crates/cabby-server/src/redis/streamer.rs:387` | matched |  |
| `rpc/http.go:118` | WRN | `circuit breaker is open` | `crates/cabby-server/src/rpc/http.rs:94`, `crates/cabby-server/src/rpc/http.rs:113` | matched |  |
| `rpc/http.go:120` | INF | `circuit breaker is closed` | `crates/cabby-server/src/rpc/http.rs:95` | matched |  |
| `rpc/rpc.go:113` | DBG | `connected` | `crates/cabby-server/src/redis/subscription.rs:415`, `crates/cabby-server/src/rpc/grpc.rs:583`, `crates/cabby-server/src/pubsub.rs:156` | matched |  |
| `rpc/rpc.go:118` | DBG | `disconnected` | `crates/cabby-core/src/nats/client.rs:147`, `crates/cabby-server/src/rpc/grpc.rs:231` | matched |  |
| `rpc/rpc.go:146` | WRN | `connection is lost, trying to reconnect immediately` | `crates/cabby-server/src/rpc/grpc.rs:733` | matched |  |
| `rpc/rpc.go:157` | INF | `connection is restored` | `crates/cabby-server/src/rpc/grpc.rs:738` | matched |  |
| `rpc/rpc.go:194` | WRN | `RPC concurrency must be positive, reverted to the default value` | `crates/cabby-server/src/rpc/mod.rs:369` | matched |  |
| `rpc/rpc.go:257` | INF | `auto-generated authorization secret from the application secret` | `crates/cabby-server/src/rpc/mod.rs:415`, `crates/cabby-server/src/broadcast.rs:175` | matched |  |
| `rpc/rpc.go:289` | INF | `RPC controller initialized: %s (concurrency: %s, impl: %s, enable_tls: %t, proto_versio...` | `crates/cabby-server/src/rpc/mod.rs:433` | matched |  |
| `rpc/rpc.go:323` | INF | `waiting for active RPC calls to finish` | `crates/cabby-server/src/rpc/mod.rs:488` | matched |  |
| `rpc/rpc.go:334` | INF | `all active RPC calls finished` | `crates/cabby-server/src/rpc/mod.rs:497` | matched |  |
| `rpc/rpc.go:634` | DBG | `RPC failed` | `crates/cabby-server/src/rpc/mod.rs:603` | matched |  |
| `server/server.go:131` | DBG | `HTTP server has been already started` | `crates/cabby-server/src/server/http_server.rs:459` | matched |  |
| `server/server.go:135` | DBG | `starting HTTP server` | `crates/cabby-server/src/server/http_server.rs:462` | matched |  |
| `sse/handler.go:78` | ERR | `failed to build subscribe command` | `crates/cabby-server/src/sse.rs:844`, `crates/cabby-server/src/sse.rs:908` | matched |  |
| `sse/handler.go:87` | ERR | `failed to establish sesssion` | `crates/cabby-server/src/sse.rs:923` | matched |  |
| `sse/handler.go:93` | ERR | `authentication failed` | `crates/cabby-server/src/sse.rs:918` | matched |  |
| `sse/handler.go:108` | ERR | `failed to subscribe` | `crates/cabby-core/src/nats/client.rs:367`, `crates/cabby-server/src/ds.rs:1459`, `crates/cabby-server/src/ds.rs:1597` | matched |  |
| `sse/handler.go:115` | DBG | `subscription rejected` | `crates/cabby-server/src/node/app.rs:847`, `crates/cabby-server/src/sse.rs:944` | matched |  |
| `sse/handler.go:125` | DBG | `session established` | `crates/cabby-server/src/ds.rs:1370`, `crates/cabby-server/src/sse.rs:973` | matched |  |
| `sse/handler.go:141` | DBG | `server shutdown` | `crates/cabby-server/src/ds.rs:1653`, `crates/cabby-server/src/ds.rs:1676`, `crates/cabby-server/src/sse.rs:992` | matched |  |
| `sse/handler.go:148` | DBG | `request terminated` | `crates/cabby-server/src/sse.rs:1003` | matched |  |
| `sse/handler.go:152` | DBG | `session completed` | `crates/cabby-server/src/ds.rs:1181`, `crates/cabby-server/src/sse.rs:1009` | matched |  |
| `streams/controller.go:64` | DBG | `subscription failed` | `crates/cabby-core/src/streams.rs:247` | matched |  |
| `streams/controller.go:74` | DBG | `subscribed verified` | `crates/cabby-core/src/streams.rs:261` | matched |  |
| `streams/controller.go:76` | DBG | `` | `cabby-core/src/streams.rs` (empty-message record) | matched |  |
| `streams/controller.go:135` | DBG | `unsigned` | `crates/cabby-core/src/streams.rs:212` | matched |  |
| `telemetry/telemetry.go:131` | DBG | `send telemetry event` | — | n/a | removed with telemetry (D-058) |
| `telemetry/telemetry.go:145` | DBG | `failed to marshal telemetry payload` | — | n/a | removed with telemetry (D-058) |
| `telemetry/telemetry.go:164` | DBG | `timed out to send telemetry data` | — | n/a | removed with telemetry (D-058) |
| `telemetry/telemetry.go:168` | DBG | `failed to perform telemetry request` | — | n/a | removed with telemetry (D-058) |
| `telemetry/telemetry.go:175` | DBG | `telemetry authenticated failed` | — | n/a | removed with telemetry (D-058) |
| `telemetry/telemetry.go:180` | DBG | `telemetry request failed` | — | n/a | removed with telemetry (D-058) |
| `ws/handler.go:31` | DBG | `WebSocket connection upgrade failed` | `crates/cabby-server/src/ws/handler.rs:125`, `crates/cabby-server/src/ws/handler.rs:192` | matched |  |
| `ws/handler.go:52` | DBG | `no subprotocol negotiated` | `crates/cabby-server/src/ws/handler.rs:208` | matched |  |
| `ws/handler.go:57` | DBG | `WebSocket session established` | `crates/cabby-server/src/ws/handler.rs:214` | matched |  |
| `ws/handler.go:59` | DBG | `WebSocket session completed` | `crates/cabby-server/src/ws/handler.rs:217` | matched |  |
| `ws/handler.go:63` | ERR | `WebSocket session failed` | `crates/cabby-server/src/ws/handler.rs:220` | matched |  |
