# Changelog

All notable changes to Cabby are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Cabby uses
[Semantic Versioning](https://semver.org/). While the version is 0.x, a minor
release may contain breaking changes; they are listed under "Changed" and "Removed".

Differences from the AnyCable Go server are listed by ID in
[`compat/deviations.md`](compat/deviations.md).

## [Unreleased]

### Added

- `--redis_sentinel_tls` (`CABBY_REDIS_SENTINEL_TLS`, `sentinel_tls` in `[redis]`; Cabby only):
  TLS for the connections to the Redis Sentinel nodes, with the `--redis_tls_*` verification, CA
  and client certificate of the master. Off by default (D-115).

### Changed

- **Breaking for Sentinel nodes that accept TLS only:** with `--redis_sentinels`, the scheme of
  the first Sentinel URL (`rediss://`) turns TLS on for the master only, and the connections to
  the Sentinel nodes are plain, as in AnyCable. Before, Cabby used TLS for both, so a
  configuration with plain Sentinel nodes and a TLS master failed to connect. If you use
  `rediss://` in `--redis_sentinels` and your Sentinel nodes accept TLS only, set
  `--redis_sentinel_tls`. While the master uses TLS and the Sentinel nodes do not, Cabby logs
  `Redis Sentinel connections do not use TLS; rediss:// in --redis_sentinels applies to the
  master only (set --redis_sentinel_tls=true to use TLS with the Sentinel nodes)` at startup, at
  info level. The implicit form (`--redis_url=rediss://sentinel:26379?master_set=mymaster`) follows the same rule
  (D-115).

### Fixed

- Redis Sentinel: a master name written without a scheme (`--redis_url=mymaster` with
  `--redis_sentinels`) names that master. Before, Cabby asked the Sentinel nodes for a master with
  an empty name and failed to connect. An empty name now stops startup with `no Redis Sentinel
  master name: give it as the host of --redis_url (redis://<name>)` (D-116).

### Fixed

- Redis Streams broadcasting (`--broadcast_adapter=redisx`): a read takes at most 1000 entries.
  Before, after the servers were stopped while broadcasts were added, the first server to read
  again took the whole backlog in one reply, and the other servers claimed its entries that had
  waited longer than the read block time and broadcast them too, so some broadcasts reached
  clients twice. A backlog is now shared by the servers that read it, 1000 entries at a time,
  so entries are no longer claimed while their server works through a large reply (D-109).
- Redis Streams broadcasting: claiming the entries a stopped server left pending continues
  where the last pass ended. Before, every pass started at the beginning of the pending list, so
  100 entries there whose handling kept failing (an entry without a `payload` field, for
  example) were claimed again every time, and the entries after them were never broadcast
  (D-110).
- Redis Streams broadcasting: when every attempt connects and a command then fails (a value of
  another type at the stream key, a command the Redis user may not run), the delay before the
  next attempt grows to 8 to 16 s. Before, the reader reconnected every 250 ms and wrote four log
  records each time, without end. The first retry after a read that worked is still 250 ms, so
  a consumer group deleted by a flush is created again as quickly as before; while Redis can't
  be reached the delays are unchanged (D-111).

### Fixed

- Postgres broadcaster (`--broadcast_adapter=postgres`) with `--broker=postgres`: an outbox row
  holding a message Postgres can never store (a NUL character in `data` or in the stream name, or
  a stream name over 2704 bytes, the limit of an index entry) no longer holds back the rows after
  it. Before, the broker's error was retried at the head of the outbox for as long as the node
  ran, and no later row was broadcast. Such a row is now deleted with an error naming its ID
  (`dropping a broadcast Postgres can't store`); other broker errors are still retried.
- Postgres broadcaster: in a `SQL_ASCII` database, a row whose payload is not UTF-8 is deleted and
  logged as a row that is not JSON. Before, the server failed the claim of every batch on it, so
  nothing more was broadcast from the outbox.

## [0.4.2] - 2026-11-27

Cabby 0.4.2 fixes request handling in the SSE and Durable Streams endpoints, keeps metrics
writers off the server's runtime threads, and makes `--print-config` output load back. It stays
compatible with AnyCable server 1.6.21.

### Fixed

- SSE (`--sse`): a request is answered from its request line and headers without waiting for
  its body: a preflight, a method that is not served, a failed sign-in (`401`), a POST body
  announced over 64 KiB (`400`), and a GET, whose body is never read. Before, the endpoint read
  the whole body first, so a request whose headers announced a body the client did not send
  got no response. A POST is now signed in before its body is read, so one whose commands
  cannot be parsed and whose sign-in fails gets `401` instead of `400`. Durable Streams
  (`--ds`) already answered without reading a body; a test now pins it (D-096).
- SSE: the body of a POST must arrive within 5 s of the sign-in; a later or stalled body is
  answered `400` (`failed to build subscribe command error="i/o timeout"`) and its session
  is disconnected. Before, reading it had no deadline (D-097).
- SSE: a message type with a line terminator is no longer written as the `event:` field, and a
  stream name or epoch with one no longer as the `id:` field; the event keeps its data. Before,
  the rest of such a value was read as further fields of the event (D-098).
- Durable Streams SSE: a line terminator in the history data (pretty-printed JSON, for
  example) starts another `data:` field, so the client gets the whole batch. Before, the client
  got the batch up to the first line terminator (D-099).
- Durable Streams: a stream path with `://` in it (`/ds/https://example.com/feed`) names that
  stream, as in AnyCable. Before, everything up to the host was dropped, so the request read the
  stream `feed`, and with a signed stream name it was answered `401`. Absolute-form request
  targets (`http://host/ds/feed`) are read as before.
- Durable Streams: a stream path that is not valid UTF-8 once decoded (`/ds/%ff`) is answered
  `400`. Before, every such path read the stream named U+FFFD, so `/ds/%ff`, `/ds/%fe` and
  `/ds/%EF%BF%BD` read the same stream (D-104).
- StatsD metrics (`--statsd_host`): the host lookup, connect and sends no longer run on a
  thread of the server's runtime. While the host did not resolve, every metrics interval held a
  runtime thread for the resolver's timeout (5 s for a `.local` name), so on a one-CPU host the
  whole server paused, and shutdown waited for the lookup. As in go-statsd, a sender thread of
  the writer owns the socket and connects again 5 s after a failed connect or send; the metrics
  task only queues packets. Up to 10 packets wait in the queue; when it is full, new packets are
  dropped and reported once a minute (`<n> packets lost (overflow)`, as go-statsd).
- mruby metrics formatter (`--metrics_log_formatter`): loading the script no longer holds a
  thread of the server's runtime. A script whose top-level code ran for a long time paused the
  server meanwhile on a one-CPU host, and one that never finished stopped it. Each metrics
  writer now starts on a thread of its own, as only the metrics goroutine waits in Go; shutting
  down during the load returns at once. A script that fails to load still stops the server at
  startup with the same error.
- Prometheus endpoint (`--metrics_http`) served on the main port or another port of the
  server: when its path is also another route's (health, WebSocket, SSE, Durable Streams, API),
  that route answers there, as in Go. Before, the metrics endpoint replaced it, because it was
  mounted after the other routes; it is now mounted first.
- `--print-config`: the `[rpc]` section loads back to the settings it was printed from. A CA set
  with `--rpc_tls_root_ca` (a path or PEM contents) is written as `tls_root_ca_path = "…"`
  instead of a commented-out placeholder, so loading the printed file keeps the CA and TLS on
  RPC connections. `--rpc_tls_verify=false`, a `--rpc_command_timeout` of 0 (no limit) and an
  empty `proxy_headers` are written as set too, instead of the commented-out defaults (D-092).
- `--print-config`: string values that contain `"`, `\`, a newline or another control character
  are escaped in every section, `[rpc]` included, so the printed file loads back to the same
  settings. Before, such a value made the file fail to load, or load as another value. The same
  goes for list items and for metrics tag keys that are not bare TOML keys (`tags."a.b"`, which
  loaded as a nested table before). Characters TOML has no short escape for are written `\u00XX`
  (Go writes `\a`, `\v` and `\x7f`, which TOML does not define). Values with nothing to escape
  print as before, as AnyCable prints them (D-102).
- Error texts and logs no longer show part of a password in URLs where a `/`, `?` or `#` in
  the password follows a part that reads as a port (`redis://user:4821#rest@host`), or where a
  comma in the password splits a URL list (`--redis_url`, `--redis_sentinels`, `--nats_servers`,
  `--rpc_host`). Such URLs are shown as invalid URLs are (D-080), by their scheme only, or as
  `***` in the Redis start records; a NATS server written without a scheme and with credentials
  is logged without them. Connections are unchanged (D-093).
- Postgres: a password with a `/` and an `@` (`postgres://user:pa/ss@word@db/app`) is read up
  to the `@` before the host. Before, part of it was read as the host, and the startup records
  and connection errors showed it.
- Error texts that quote a value (for example invalid flag and environment variable values)
  escape the characters Go escapes: separators such as U+00A0, format characters such as
  U+200B, private-use characters and noncharacters. Before, only control characters were
  escaped.

## [0.4.1] - 2026-11-26

Cabby 0.4.1 fixes stream history in the memory, SQLite and NATS brokers, the NATS session
keepalive, and Durable Streams live reads. It stays compatible with AnyCable server 1.6.21.

### Fixed

- Memory broker, and the history of the SQLite broker and the NATS broker's local copy: a
  message broadcast while the expiry pass dropped its stream could be missing from history, and
  the next message to that stream got the same offset. Every stored message is now kept with its
  own offset (D-087).
- NATS broker: a session keepalive that finished after a newer commit of the same session
  replaced it with the older state, which a restore then returned. The keepalive now writes
  only over the revision it read, and no longer writes back a session that expired or was
  deleted meanwhile (D-088).
- Durable Streams live reads (`live=long-poll`, `live=sse`): a message published while the read
  starts is no longer skipped. The subscription is installed before the history is read, live
  messages wait until the history is written, and messages the history delivered are not sent
  again. A long-poll with nothing to catch up on returns such a message instead of waiting for a
  later one or answering `204` (D-089).
- Durable Streams SSE: every client's `control` events carry its own `streamCursor`. Before, the
  SSE clients of a stream got the cursor of one of them with each live message. The data is still
  encoded once per message (D-090).

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
