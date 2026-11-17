# Cabby

An [AnyCable](https://github.com/anycable/anycable)-compatible real-time server written in
Rust. Cabby is an independent implementation; it is not affiliated with the AnyCable project
or Evil Martians.

> **Status: test port in progress.** There is no working server yet. We are translating the
> upstream test corpus to Rust and checking it against the Go implementation first; server code
> comes after (see [`SPEC.md`](SPEC.md)).

## Benchmarks: Go vs Rust

| Workload | AnyCable (Go) | Cabby (Rust) |
|---|---|---|
| Idle connections: memory per connection | pending | pending |
| Broadcast fan-out: delivered msg/s, p50/p95/p99 latency | pending | pending |
| Connection churn and resubscription | pending | pending |
| Slow readers mixed with healthy clients | pending | pending |
| Slow or unavailable RPC with queue saturation | pending | pending |
| Cluster broadcast and reconnect recovery | pending | pending |

No numbers yet. Results will only be published once Cabby passes the compatibility suite.
Both servers will run on the same hardware and settings, with the load generator outside the
measured process and repeated runs. A faster server that behaves differently isn't a valid
comparison. Methodology: [`SPEC.md` §10](SPEC.md#10-performance-evaluation).

## Compatibility progress

From `cargo xtask report` (source of truth: [`compat/test-map.json`](compat/test-map.json)):

| | Count |
|---|---:|
| Upstream cases inventoried (Go unit, Benchi, go-mruby, features, AnyT) | 923 |
| Translated to Rust | 22 |
| Translated cases verified against Go | 22 |
| Passing against Cabby | 0 |

Every case is listed individually, including skipped and runtime-specific ones.
Behavioral differences from Go, if any, are recorded in
[`compat/deviations.md`](compat/deviations.md).

## Layout

```text
crates/cabby-core      protocol types, generated RPC bindings, state, routing
crates/cabby-server    transports, RPC clients, adapters, config, `cabby` binary
crates/cabby-testkit   backend selection, Go reference bridge, process supervision
tests/                 translated upstream tests (unit/<go package>, conformance)
tools/xtask            inventory, compatibility runner, reports
compat/                pinned reference, baseline evidence, manifest, adapters
docs/                  contributor guides
```

## Development

Toolchains are pinned in `mise.toml` (Rust 1.100.0, Go 1.27.3 for the reference, protoc).

```sh
mise install
cargo xtask reference-checkout          # pinned upstream into compat/reference
cargo xtask reference-build             # Go reference binary + test bridge
cargo xtask test --backend go           # translated tests against Go: must pass
cargo xtask test --backend rust         # against Cabby: pass or explicitly pending
cargo xtask report
```

To translate more tests, see [`docs/translating-tests.md`](docs/translating-tests.md).

## License and attribution

Cabby is released under the [MIT License](LICENSE), the same license as AnyCable.

Translated tests and scenarios derive from AnyCable (MIT, © Vladimir Dementyev), AnyT
(MIT, © palkan) and go-mruby (MIT, © Mitchell Hashimoto). Their notices are in
[`compat/licenses/`](compat/licenses/).
