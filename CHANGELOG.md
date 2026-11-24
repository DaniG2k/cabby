# Changelog

All notable changes to Cabby are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Cabby uses
[Semantic Versioning](https://semver.org/). While the version is 0.x, a minor
release may contain breaking changes; they are listed under "Changed" and "Removed".

Differences from the AnyCable Go server are listed by ID in
[`compat/deviations.md`](compat/deviations.md).

## [Unreleased]

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
  through Redis ([`docs/redis-broker.md`](docs/redis-broker.md), D-069).

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
