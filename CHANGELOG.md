# Changelog

All notable changes to Cabby are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and Cabby uses
[Semantic Versioning](https://semver.org/). While the version is 0.x, a minor
release may contain breaking changes; they are listed under "Changed" and "Removed".

Differences from the AnyCable Go server are listed by ID in
[`compat/deviations.md`](compat/deviations.md).

## [0.2.0] - Unreleased

Cabby 0.2.0 is compatible with AnyCable server 1.6.21: an AnyCable deployment that swaps
`anycable-go` for `cabby` and keeps its `anycable.toml` and `ANYCABLE_*` variables behaves as
before.

### Added

- The SQLite broker (`--broker=sqlite --sqlite_path=<file>`): stream history, offsets and
  sessions survive a graceful restart of a single node, without Redis or NATS (D-065).
- `cabby.toml` and `/etc/cabby/cabby.toml`, found before `anycable.toml` (D-060).
- `CABBY_*` environment variables, read before `ANYCABLE_*`, with one warning per setting
  when both are set to different values (D-061). `CABBY_NATS_SERVER_BIN` names the
  `nats-server` executable for embedded NATS (`CABBY_NATS_SERVER` still works).
- A built-in health check in the Docker image (`cabby-healthcheck`, `GET /health` on the
  main port).
- `THIRD_PARTY_LICENSES.md` in every archive and in the image (`/usr/share/doc/cabby/`).

### Changed

- `cabby --version` prints `cabby 0.2.0 (AnyCable 1.6.21 compatible)` instead of `1.6.21`,
  and `--help` and deprecation warnings name Cabby (D-059). Scripts that parse the version
  output need updating.
- `--help` lists the `CABBY_*` names; the `ANYCABLE_*` names still work (D-061).
- The Docker image sets `CABBY_HOST=0.0.0.0` and `CABBY_BROADCAST_ADAPTER=http` instead of
  their `ANYCABLE_*` names. Override them with `CABBY_HOST` and `CABBY_BROADCAST_ADAPTER`: an
  `ANYCABLE_*` value loses to the image's `CABBY_*` one, with a warning.
- mruby is a cargo feature, off by default and not in release builds. `--metrics_log_formatter`
  needs a build with `--features mruby`, and otherwise stops with an error that says so (D-062).

### Removed

- The usage telemetry code. The `cabby` binary never sent telemetry; now the code is gone, and
  `ANYCABLE_DISABLE_TELEMETRY` and the other telemetry variables are accepted and ignored (D-058).
- `RunnerOption::Telemetry` from the embedding API.

## [0.1.0] - 2026-11-23

The first release: binaries for Linux (x86_64 and arm64, glibc 2.28 or newer) and macOS (Apple
silicon), and the multi-arch image `ghcr.io/danig2k/cabby:0.1.0`.
