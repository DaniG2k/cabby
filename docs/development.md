# Development

## Build from source

You need:

- Rust 1.100 or newer, via [rustup](https://rustup.rs) (`rustup update stable`). Development uses
  the version pinned in [`rust-toolchain.toml`](../rust-toolchain.toml).
- `protoc`, the Protocol Buffers compiler, for the gRPC bindings (`brew install protobuf` or
  `apt install protobuf-compiler`).
- A C compiler (Xcode Command Line Tools on macOS, `build-essential` on Debian and Ubuntu). The
  TLS library compiles some C, and so does mruby if you enable it.

```sh
cargo install --locked --git https://github.com/DaniG2k/cabby cabby-server
```

This puts the `cabby` binary in `~/.cargo/bin`. mruby scripting for custom metrics output is off
by default; add `--features mruby` to build it in ([`docs/mruby.md`](mruby.md)).

## Layout

```text
crates/cabby-core      protocol, JSON, hub, brokers, pub/sub, identity, configuration
crates/cabby-server    node, RPC, WebSocket/SSE/Pusher/durable streams, adapters, CLI, `cabby` binary
crates/cabby-mruby-sys vendored mruby 1.2.0 and its FFI boundary
crates/cabby-bench     external load harness (benchmarks and soak tests)
crates/cabby-testkit   backend selection, Go reference bridge, fake RPC, process supervision
tests/                 translated upstream tests (unit/<go package>, conformance, features)
tools/xtask            inventory, compatibility runner, reports
bench/                 other servers under test (Action Cable, Soketi, Reverb)
compat/                pinned reference, baseline evidence, manifest, deviations, adapters
docs/                  contributor guides, benchmark results, reviews
```

## Tests and tools

Toolchains are pinned in `mise.toml` (Rust 1.100.0 and protoc). Redis and NATS run in Docker;
`services.sh` exports their addresses as `CABBY_TEST_*`. Tests that restart NATS need a
`nats-server` binary: set `CABBY_NATS_SERVER_BIN`, or have Go on `PATH` and the test kit builds
the pinned version. The test crates build `cabby-server` with the `mruby` feature; a plain
`cargo build -p cabby-server` leaves it out.

```sh
mise install
eval "$(compat/scripts/services.sh start)"   # Redis + NATS for integration tests (CABBY_TEST_*)
cargo xtask test --backend rust         # translated tests against Cabby
cargo xtask report --by-package
compat/scripts/check-deps.sh            # dependency licenses and bans (cargo-deny)
```

The Go reference server needs Go 1.27.3 (`mise exec go@1.27.3 -- …`, as below):

```sh
cargo xtask reference-checkout          # pinned upstream into compat/reference
cargo xtask reference-build             # Go reference binary + test bridge
cargo xtask test --backend go           # translated tests against Go: must pass
```

When you add or re-annotate a test under `tests/`, record the manifest with Go:
`mise exec go@1.27.3 -- cargo xtask inventory`, then `cargo xtask test --backend go --record` and
`--backend rust --record` (a filter limits both to the new test). Releases are built locally with
`compat/scripts/release.sh` ([`docs/releasing.md`](releasing.md)).

See [`docs/implementing.md`](implementing.md) and
[`docs/translating-tests.md`](translating-tests.md).
