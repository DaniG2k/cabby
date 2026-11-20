# Pusher-protocol benchmark: Soketi, Reverb, AnyCable, Cabby

`compat/scripts/bench-compare-pusher.sh` compares four Pusher-compatible servers with the external
load harness (`crates/cabby-bench`) in Pusher mode (`--protocol=pusher`). Results go to
`docs/benchmarks/pusher-results/` (the script refuses `docs/benchmarks/results`, which holds the
Action Cable comparison), and the charts to `docs/benchmarks/pusher-*.svg`.

| Server | Version | Runs as | Installed by |
|---|---|---|---|
| Soketi | 1.6.1 (uWebSockets.js 20.10.0), Node 18.20.8 | `bench/soketi/sut` → `node .../soketi/bin/server.js start` | `bench/soketi/setup.sh` |
| Laravel Reverb | 1.14.0, Laravel 13.43, PHP 8.4.28 + ext-ev 1.3.1 | `bench/reverb/sut` → `php artisan reverb:start` | `bench/reverb/setup.sh` |
| AnyCable (Go) | pinned reference (`compat/upstream.lock`) | `target/reference-bin/anycable-go` with Pusher flags | `cargo xtask reference-build` |
| Cabby | this repository | `target/release/cabby` with Pusher flags | `cargo build --release -p cabby-server` |

## Running it

```sh
cargo build --release -p cabby-server -p cabby-bench
cargo xtask reference-build                  # or keep an existing target/reference-bin/anycable-go
bench/soketi/setup.sh                        # mise's Node 18 + npm ci into bench/soketi/node_modules
bench/reverb/setup.sh                        # builds PHP into bench/reverb/.php (~10 min), composer install
SMOKE=1 compat/scripts/bench-compare-pusher.sh /tmp/pusher-smoke   # small-scale check of all four
compat/scripts/bench-compare-pusher.sh       # the full run (idle machine), docs/benchmarks/pusher-results
compat/scripts/bench-summary.py docs/benchmarks/pusher-results
compat/scripts/bench-charts.py docs/benchmarks/pusher-results docs/benchmarks --protocol=pusher
```

Everything installs per user, inside the repository and gitignored: `bench/soketi/node_modules`,
`bench/reverb/.php` (PHP, its build tools and Composer), `bench/reverb/vendor`. Nothing needs sudo.

## What the harness does in Pusher mode

* **Clients** speak pusher-js's protocol 7: they connect to
  `/app/<key>?protocol=7&client=js&version=8.4.0&flash=false`, wait for
  `pusher:connection_established`, `pusher:subscribe` to public channels and wait for
  `pusher_internal:subscription_succeeded`. They answer `pusher:ping` with `pusher:pong` and send
  `pusher:ping` themselves after the advertised `activity_timeout` without traffic, as pusher-js does.
  A harness stream is a channel of the same name (`1`..`100`).
* **Broadcasts** are signed `POST /apps/<app_id>/events` requests (`auth_key`, `auth_timestamp`,
  `auth_version=1.0`, `body_md5`, HMAC-SHA256 `auth_signature`), one event
  `{"name":"bench","channel":..,"data":..}` per request. The event data is the harness's ledger payload
  (`<channel>:<seq>|<worker>|<dispatch time>`), so delivery is reconciled by id, latency is measured
  end to end, ordering is checked per publisher worker and generator saturation is reported exactly
  as in the Action Cable mode.
* **No batching.** The Action Cable comparison coalesces queued broadcasts into one POST under
  backpressure. AnyCable's Pusher API has no `batch_events` endpoint (it answers 200 and drops the
  batch), so Pusher mode publishes one event per POST for every server. `--publish-batch=N` (at most
  10, Pusher's limit) uses `batch_events` for servers that have it (Soketi, Reverb).
* **Reconnect floods** run the same waves as the Action Cable `churn` workload, without RPC: Pusher
  public channels need no application, so there is no connect/subscribe/disconnect latency and no
  disconnect queue. The run measures recovery (all clients reconnected and resubscribed), welcome and
  confirmation times and ping round trips (probes). The watchdog is client-driven (a `pusher:ping`
  after `--ping-interval` seconds without a server message, reconnect after `--ping-timeout`), because
  Pusher servers do not all ping on a schedule. Gauge-based keys are left out for every server
  (`server_metrics=false`), so the four print the same keys.

## Server settings

Production-like for every server, debug logging off, one Pusher app
(`cabby-bench` / `cabby-bench-key` / `cabby-bench-secret`, set by the harness):

* **Cabby, AnyCable:** the harness's usual flags (`--log_level=error`, `--broker=none`,
  `--pubsub=none`, public streams, no RPC) plus `--pusher_app_id`, `--pusher_app_key`,
  `--pusher_secret`. The REST API is on the main port.
* **Soketi:** defaults (single process, `local` adapter, `array` app manager, metrics off,
  `SOKETI_DEBUG=false`, unlimited connections and event rates), `NODE_ENV=production`. Soketi always
  starts its UDP cluster discovery (`node-discover`, port 11002) even with the local adapter; that is
  its default and is left on. The `sut` clears inherited `SOKETI_*` variables.
* **Reverb:** `APP_ENV=production`, `APP_DEBUG=false`, `LOG_LEVEL=error`, no `--debug`, scaling
  (Redis) off, the packaged `config/reverb.php` defaults (`activity_timeout` 30, `ping_interval` 60,
  `max_request_size` 10,000 bytes, rate limiting off). PHP is built from source with ext-ev, so
  ReactPHP's loop is `ExtEvLoop` (epoll) instead of `stream_select`, which stops at 1,024
  connections; Reverb's documentation asks for such a loop (it names ext-uv) beyond 1,000
  connections. `memory_limit=2G` replaces php.ini-production's per-request 128M. Opcache and JIT
  are off, as in a default CLI.

## Fairness caveats

* **Cores.** Each server is pinned to `SERVER_CPUS` (default 4 cores). Cabby and AnyCable use all of
  them; Soketi (one uWebSockets.js event loop plus Node's main thread) and Reverb (one PHP event loop)
  use about one core per process. That is how each is built: the Node and PHP servers scale out with
  more processes behind a load balancer and a Redis adapter (Soketi's cluster/Redis adapters, Reverb's
  `REVERB_SCALING_ENABLED`), which this single-node benchmark does not set up. Read their numbers as
  "one process", and the CPU chart as cores used, not cores available.
* **No batching** (above) costs every server one HTTP request per broadcast; the Action Cable
  comparison's coalescing does not apply here, so the fan-out numbers are not comparable across the
  two comparisons.
* **Pings are client-driven.** In Pusher mode AnyCable and Cabby send no server pings (Pusher uses
  client-to-server pings), Soketi neither; Reverb pings connections idle for its `ping_interval`
  (60 s), longer than any workload here. Every server answers the clients' `pusher:ping`.
* **PHP build.** Reverb's PHP is a minimal CLI build (no curl, which only Laravel's Pusher broadcaster
  uses, not the Reverb server). Distribution PHP packages are built with similar flags (`-O2`).
