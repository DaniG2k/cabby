# Changelog

All notable changes to Cabby are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Cabby uses
[Semantic Versioning](https://semver.org/). While the version is 0.x, a minor
release may contain breaking changes; they are listed under "Changed" and "Removed".

Differences from the AnyCable Go server are listed by ID in
[`compat/deviations.md`](compat/deviations.md).

## [0.4.0] - 2026-11-26

Cabby 0.4.0 delivers broadcasts with less CPU and lower latency, and uses far less memory when
messages are large. It also fixes how Cabby handles gRPC backend restarts, slow readers, presence
with deeply nested info, `--pong_timeout` and starting with busy PostgreSQL tables. It stays
compatible with AnyCable server 1.6.21. Two changes to the Rust embedding API are breaking (see
"Changed").

### Changed

- Lower CPU and allocation overhead on the broadcast fan-out hot path (thin LTO and
  `codegen-units = 1` in release builds, borrowed encoder IDs, cached hot metric counter
  handles and session config, single-hop non-blocking writes, single batch write timer, and
  fewer wakeups).
- **Breaking (Rust embedding API):** `cabby_core::encoders::SentFrame.payload` is now
  `bytes::Bytes` instead of `Vec<u8>`, so broadcast frames are shared across WebSocket
  connections with a reference-count bump instead of copying or re-boxing.
- WebSocket data frames and server pings are written straight to the socket, already framed,
  with one vectored write per batch: payloads are no longer copied into a per-connection write
  buffer, and a batch over 1 KiB is no longer split into several writes. With large messages this
  saves memory, CPU and latency: a 5,000-client room receiving 32 KiB messages used 62 MB, down
  from about 318 MB in 0.3.0.
  `--write_buffer_size` is still accepted, but only control frames (pongs, close frames) go
  through that buffer.
- The write timeout (`--ws_write_timeout`) of a WebSocket batch starts when the batch is
  written, as in AnyCable, instead of when its last frame is queued: frames that wait behind
  another batch, or for a busy CPU, keep their whole timeout. A client that does not take a
  batch within the timeout is still disconnected. The earliest limit of a batch's frames bounds
  it: a native ping (`--enable_native_pings`) keeps its own deadline, half the ping interval
  from when it is sent, and never gives the data written with it more than the write timeout.
- A WebSocket connection ends when writing a pong fails or takes more than a second, and
  messages behind that ping are not executed, as in AnyCable when nothing else is being
  written. Before, the failure was ignored. (While frames are being written, Cabby answers
  pings after that batch.)
- A WebSocket client that is behind with reading is still read, one message per batch of frames
  written to it (D-072). Before, its commands waited until it had caught up, and with
  `--pong_timeout` it was disconnected once it was behind for longer than that, since its pongs
  waited too. As in AnyCable, a close frame or the end of the stream from such a client now ends
  the connection after at most the batch being written, without sending the frames still queued. A
  pong to a ping read this way must also be written within a second.
- **Breaking (Rust embedding API):** `cabby_server::node::Connection::try_write_frame` takes the
  write timeout (a `Duration`, from the start of the write) instead of a deadline.

### Fixed

- The depth of presence info is bounded: info nested deeper than 126 levels is kept and sent as
  its JSON text (D-073).
- A session whose client sent a message that cannot be decoded ends when its connection does.
  Before, the connection was no longer read, and its end was only noticed by a later write
  (D-074).
- HTTP RPC requests cancelled because their session closed no longer count as failures for the
  circuit breaker (D-075).
- gRPC: after every connection attempt to the RPC backends failed, Cabby reconnects in the
  background with backoff (1 s at first, up to 2 minutes), as AnyCable does. Before, RPC calls
  kept failing with `grpc connection is not ready` after the backend was back on the same
  address, until Cabby was restarted. While no backend is connected, a background attempt that gets
  no answer within 20 s counts as failed too, and the host name is resolved again.
- The session id is a JSON string in the welcome message and in the identifiers of public and
  no-auth connections, whatever characters it contains (D-076).
- Sessions with the same id (a proxy may repeat an `X-Request-ID`) are registered, subscribed and
  unregistered independently (D-077).
- JWT: tokens over 128 KiB, or with a header segment over 8 KiB, are rejected, and the claims are
  decoded after the signature has been verified (D-079).
- Errors about an invalid Redis or NATS URL leave out its user and password (D-080).
- The warning about unverified Redis TLS certificates also covers `valkeys://` and schemes in
  upper case, and a new one says when a URL asks for TLS but connections do not use it, such as
  a `rediss://` master with Sentinel addresses without TLS (D-081).
- Durable Streams serve a client only when authentication succeeded, as the SSE endpoint does
  (D-078).
- Postgres adapters: a node starting while another session wrote to Cabby's tables, or ran
  `VACUUM` on them, waited without a limit, and the broadcasts of running nodes failed behind it.
  A start no longer takes exclusive locks on tables and indexes that exist, and one that needs a
  lock another session holds stops after a few seconds with an error naming the statement (see
  [`docs/postgres.md`](docs/postgres.md)).
- Postgres broadcaster: a connection that went silent while the claim of an empty outbox was being
  committed could stop the node from claiming broadcasts; the commit now has the same timeout as
  the other claim statements.
- Redis pub/sub and the legacy `redis` broadcast adapter couldn't connect with an implicit Sentinel
  URL (`redis://sentinel:26379?master_set=mymaster`) (D-082).
- The timer queue is bounded by the timers that can still fire: a session's timer that is
  stopped or re-armed leaves the queue at once instead of at its old deadline, also when a
  connection ends before its session is set up. With `--pong_timeout`, a pong that does not change
  which deadline comes first no longer touches the queue.

## [0.3.0] - 2026-11-24

Cabby 0.3.0 adds multi-node history and session recovery without NATS: a Redis broker for
deployments that already run Redis, and PostgreSQL pub/sub, broadcasting and a broker for apps
that already run Postgres. It stays compatible with AnyCable server 1.6.21; two changes need
attention when upgrading (see "Changed").

### Added

- `cabby-healthcheck` reads the host from `CABBY_HOST` or `ANYCABLE_HOST`. For an empty
  host, `localhost`, `0.0.0.0` or `::` it tries `127.0.0.1`, then `::1`; for an IP address,
  that address; for a host name, every address it resolves to, IPv4 first. The first healthy
  address passes the check.
- SQLite broker files use incremental auto-vacuum, so space freed by pruning goes back to the
  file system. A file written by an earlier Cabby is converted once when it is opened (a
  `VACUUM`, logged with the page counts before and after); shutdown frees pages only within
  the shutdown timeout. The conversion needs free disk space of about the file's size, for a
  temporary copy in SQLite's temp directory and for the WAL next to the file; if it fails,
  Cabby logs a warning and uses the file unconverted.
- The Redis broker (`--broker=redis`): stream history and sessions shared by several nodes
  through Redis ([guide](https://docs.cabbyhq.com/redis), D-069).
- PostgreSQL pub/sub and broadcasting (`--pubsub=postgres`, `--broadcast_adapter=postgres`):
  several nodes share broadcasts through the app's Postgres, and the app can broadcast by
  inserting into an outbox table inside its own transactions
  ([guide](https://docs.cabbyhq.com/postgres), D-070).
- The PostgreSQL broker (`--broker=postgres`): stream history, offsets and session recovery
  shared by every node ([guide](https://docs.cabbyhq.com/postgres), D-071).

### Changed

- **Breaking:** Pusher needs a secret. When Pusher is enabled (`--pusher_app_id` and
  `--pusher_app_key`) without `--pusher_secret` or `--secret`, Cabby stops at startup with
  `Pusher requires a secret (--pusher_secret or --secret)`.
- **Breaking for very large responses:** `rpc.max_recv_size` (`--rpc_max_call_recv_size`) now
  also limits HTTP RPC response bodies, to 16 MiB by default. A larger response fails the call
  without a retry and does not count against the circuit breaker.
- Prometheus label values escape `\`, `"` and newlines, so the exposition output stays valid
  for any tag value.
- The dedicated Prometheus listener (`--metrics_host` different from `--host`) uses the
  server's TLS settings, the main server's header timeout and limits, a connection limit of
  `--max-conn` (1024 when it is 0) and a 60 s idle timeout for keep-alive connections.
- The SQLite write queue is also bounded by 64 MiB of payload; history writes it drops make the
  next start use a new epoch, as for the op count bound.
- URL credentials (`user:password@`) are left out of HTTP RPC error messages and debug output.
- The embedded NATS config file is created with mode 0600 under a random name, and the default
  JetStream store directory with mode 0700; an existing default store directory must belong to
  the user running Cabby.

## [0.2.0] - 2026-11-24

Cabby 0.2.0 is compatible with AnyCable server 1.6.21: an AnyCable deployment that swaps
`anycable-go` for `cabby` and keeps its `anycable.toml` and `ANYCABLE_*` variables behaves as
before.

### Added

- The SQLite broker (`--broker=sqlite --sqlite_path=<file>`): stream history, offsets and
  sessions survive a graceful restart of a single node, without Redis or NATS. The Docker image
  has a data directory for it, `/var/lib/cabby`, owned by its non-root user.
- `cabby.toml` and `/etc/cabby/cabby.toml`, found before `anycable.toml` and
  `/etc/anycable/anycable.toml`.
- `CABBY_*` environment variables, read before `ANYCABLE_*`, with one warning per setting
  when both are set to different values. `CABBY_NATS_SERVER_BIN` names the `nats-server`
  executable for embedded NATS (`CABBY_NATS_SERVER` still works).
- A startup warning when a Redis URL is set but no broadcast adapter reads from Redis (the
  Docker image takes broadcasts over HTTP only unless `CABBY_BROADCAST_ADAPTER` or
  `ANYCABLE_BROADCAST_ADAPTER` says otherwise).
- A built-in health check in the Docker image (`cabby-healthcheck`: `GET /health` on the main
  port, or a TCP connection when TLS is configured).

### Changed

- `--help` lists the `CABBY_*` variable names instead of the `ANYCABLE_*` ones, which still
  work.
- mruby is a cargo feature, off by default and not in release builds. `--metrics_log_formatter`
  needs a build with `--features mruby`, and otherwise stops with an error that says so.

### Removed

- The usage telemetry code. The `cabby` binary never sent telemetry; now the code is gone, and
  `ANYCABLE_DISABLE_TELEMETRY` and the other telemetry variables are accepted and ignored.
- `RunnerOption::Telemetry` from the embedding API.

## [0.1.0] - 2026-11-23

The first release: binaries for Linux (x86_64 and arm64, glibc 2.28 or newer) and macOS (Apple
silicon), and the multi-arch image `ghcr.io/danig2k/cabby:0.1.0`.
