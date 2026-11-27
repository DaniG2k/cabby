# Compatibility

Cabby is a port of AnyCable's open-source Go server at the commit pinned in
[`compat/upstream.lock`](../compat/upstream.lock) (`anycable/anycable@26a1a703`, server version
1.6.21). It was built tests-first: the whole upstream test corpus, 923 cases (Go unit tests,
Benchi, go-mruby, feature tests and the AnyT conformance suite), was translated to Rust and
verified against the Go server before any Cabby code was written. 914 cases are verified against
Go; 9 test behavior Go can't expose and run against Cabby only. 922 pass against Cabby; 1 is
retired by design (telemetry, D-058)
([`compat/test-map.json`](../compat/test-map.json), summarized by `cargo xtask report`).

Cabby's own documentation is at [docs.cabbyhq.com](https://docs.cabbyhq.com/install); the
[AnyCable server docs](https://docs.anycable.io) describe the features the two share. Intended
differences from the Go server, such as Redis 6 or later for pub/sub, are recorded in
[`compat/deviations.md`](../compat/deviations.md), most with a pinning test. AnyCable Pro features
are out of scope.

Every finding from an [independent review](review/2026-11-fable-review.md) is fixed with a
regression test.

## Migrating from AnyCable

Swap the `anycable-go` binary for `cabby`; your `anycable.toml`, `ANYCABLE_*` variables and
flags keep working, and `anycable-rails` and your channels need no changes. The image
(`ghcr.io/danig2k/cabby`) differs from a plain binary in two defaults: it listens on `0.0.0.0` and
takes broadcasts over HTTP only. If your app broadcasts through Redis, set
`CABBY_BROADCAST_ADAPTER` or `ANYCABLE_BROADCAST_ADAPTER` (for example `http,redisx,redis`);
Cabby warns at startup when a Redis URL is set but no broadcast adapter uses it (with
`--broker=redis`, whenever no broadcast adapter reads from Redis). Rename
to `cabby.toml` and `CABBY_*` when convenient ([`docs/configuration.md`](configuration.md)).
Two things look different: `cabby --version` prints `cabby 0.5.1 (AnyCable 1.6.21 compatible)`
instead of `1.6.21`, and Cabby sends no telemetry. The `x-anycable-version` header still says
`1.6.21`.

## Supported for compatibility

These features work, are covered by the translated tests, and stay supported so that existing
AnyCable deployments can switch without changes. For new setups, prefer the alternative in the
last column.

| Feature | What it does | For new setups |
|---|---|---|
| Embedded NATS (`--embed_nats`) | Runs a `nats-server` as a supervised child process ([`docs/enats.md`](enats.md)). You install `nats-server` yourself; release artifacts don't include it | an external NATS server (`--nats_servers`), for example the official `nats` image |
| CableReady streams (`--cable_ready`) | Verifies signed CableReady stream names | Turbo Streams (`--turbo_streams`) or signed streams |
| Legacy Redis pub/sub broadcasting (`--broadcast_adapter=redis`) | Takes broadcasts from a Redis pub/sub channel | `http` or `redisx` (Redis streams) broadcasting |
| StatsD metrics (`--statsd_host`) | Sends metrics to a StatsD server | Prometheus (`--metrics_http`) |
| mruby metrics formatters (`--metrics_log_formatter`) | Formats metrics log lines with a Ruby script ([`docs/mruby.md`](mruby.md)). A build feature, off by default and not in release builds | Prometheus, or the default log format |

## Runtime dependencies

Two features run third-party code that Cabby doesn't rewrite in Rust: embedded NATS starts the
official `nats-server`, a Go binary, as a child process ([`docs/enats.md`](enats.md), D-026), and
the `mruby` build feature links mruby 1.2.0, a C library ([`docs/mruby.md`](mruby.md)). Release
builds contain neither.

## Fixes over the reference server

- **Redis silently drops a subscription** ([AnyCable issue 275](https://github.com/anycable/anycable/issues/275)):
  the Go server stops delivering broadcasts until it is restarted. Cabby resubscribes within about
  1 s, or notices a half-open connection within about 10 s through a heartbeat.
- **Redis Streams broadcasting** (`--broadcast_adapter=redisx`): a read takes at most 1000
  entries, so after an outage the backlog is shared by the servers that read it, and entries are
  not broadcast a second time by servers that claim them while the first works through a large
  reply (D-109).
  Claiming the entries of a stopped server goes through its whole pending list, 100 entries a
  pass, so entries whose handling keeps failing don't hold back the ones after them (D-110).
  A failure that repeats on every attempt (a value of another type at the stream key) is
  retried after a growing delay, up to 8 to 16 s, instead of every 250 ms (D-111).
