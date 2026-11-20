# Action Cable benchmark server

Rails' built-in WebSocket server as the third server in Cabby's benchmark
(`compat/scripts/bench-compare.sh`), next to AnyCable (Go) and Cabby (Rust). It's a minimal
`Rails::Application` with only Action Cable (`app.rb`), served by Puma. `sut` lets the
`cabby-bench` harness launch it like the other two servers.

## Setup

```sh
cd bench/actioncable
mise install ruby@4.0.9                     # the version in .ruby-version
mise exec ruby@4.0.9 -- bundle config set --local path vendor/bundle
mise exec ruby@4.0.9 -- bundle install      # into vendor/bundle (gitignored)
./sut --version                             # actioncable 8.1.4.1 (puma 8.1.2, ruby 4.0.9)
```

`sut` finds Ruby with `mise where ruby@<.ruby-version>`. Set `ACTIONCABLE_RUBY` to use another
`ruby` binary. `bench-compare.sh` skips Action Cable when `sut --version` fails.

## Versions and settings

| Setting | Value | Why |
|---|---|---|
| Ruby | 4.0.9, YJIT on | YJIT is on because Rails 8.1 enables it in production (`config.yjit`). |
| Rails | 8.1.4.1 (`railties` + `actioncable`) | The latest stable Rails. Only the gems Action Cable needs. |
| Puma | 8.1.2, single mode, 5 threads (min = max) | Single mode: see the adapter row. 5 is Puma's default maximum. |
| Environment | `production`, eager loading, `api_only` | Production behaviour: no reloading, minimal middleware. |
| Subscription adapter | `async` (in-process) | One process holds every connection, like the Go and Rust servers. No Redis hop. |
| Worker pool | Action Cable's default, 4 threads | See below. |
| Logging | `warn`, no log tags (`error` under the harness) | See below. |
| Origin check | off | The harness's clients send no `Origin` header. |

**Worker pool.** Action Cable runs every connection callback (`connect`, `disconnect`), every
client command (subscribe and the channel's `subscribed`), and every per-subscriber broadcast
delivery as a task on a pool of `config.action_cable.worker_pool_size` threads (default 4). The
benchmark keeps that default. It's what a Rails app runs unless someone tunes it. Under the GVL,
more threads don't run more Ruby in parallel; they only help when callbacks block on I/O. That's
the case in `reconnect_floods`, where AnyCable gets `rpc_concurrency` 28 by default. For
experiments only, `ACTIONCABLE_WORKER_POOL_SIZE` overrides the size. Results with a non-default
pool aren't comparable to the published ones.

**Puma.** Action Cable hijacks each WebSocket from Puma right after the handshake. From then on
the connection lives on Action Cable's event loop (nio4r) and worker pool. Puma's threads only
serve handshakes and broadcast POSTs, so 5 is enough. `force_shutdown_after 1` stops Puma
quickly on the harness's SIGTERM despite the open WebSockets.

**Logging.** In production at `info`, Action Cable logs every connection open and close and every
subscription. At `debug` it also logs every broadcast and every transmission. `sut` maps the
harness's `--log_level` (`error` by default) to `RAILS_LOG_LEVEL`, and the harness discards
the output.

## How the harness talks to it

`sut` accepts the AnyCable CLI flags the harness passes. It uses `--host`, `--port`,
`--http_broadcast_port` (bound too if different), `--path`, `--http_broadcast_path` and
`--log_level`, ignores the rest, and `exec`s Puma. The harness's PID is therefore the Ruby server
process, also through `bench-compare.sh`'s `taskset` exec-wrapper. Readiness is Puma's listening
socket. Puma loads the app before binding, so Rails is booted by then.

- **Public streams** (`stress_publications`). Clients subscribe with
  `{"channel":"$pubsub","stream_name":"<n>"}`. `"$pubsub"` can't be constantized, so a prepended
  `Subscriptions#add` maps it to `PubSubChannel` (`stream_from params[:stream_name]`). That's
  Action Cable 8.1's method with only the class lookup changed. There's no authentication.
- **HTTP broadcasts.** `POST /broadcast` takes AnyCable's payload: an object
  `{"stream":..,"data":..}` or an array of them. For each one it calls
  `ActionCable.server.broadcast(stream, value)`. As in AnyCable, `data` that is valid JSON is
  delivered as that JSON value and anything else as a string. Clients therefore receive the same
  `message` the harness's ledger reconciles: the string `<stream>:<seq>|<worker>|<ns>`. The
  endpoint is a Rack app mapped in `config.ru`, in front of the Rails middleware. In a real app
  the broadcast call would run inside application code, so request routing is not part of the
  measurement.
- **Metrics.** None. The harness probes `/metrics` once, gets a 404, and reports gauges as
  unknown (`server_metrics=false`). `stress_publications` needs no gauges. Resources (RSS, CPU,
  threads, sockets) come from `/proc/<pid>` as for the other servers.

### `reconnect_floods` (the `churn` workload)

AnyCable runs this scenario against the harness's fake HTTP RPC app. Every connect, subscribe
command and disconnect costs one RPC call with 5 ms (±30%) latency. Action Cable has no RPC: the
application runs in-process. The harness exports the fake app's latency to the server process
(`CABBY_BENCH_APP_LATENCY_{CONNECT,COMMAND,DISCONNECT}_MS`, `CABBY_BENCH_APP_LATENCY_JITTER`). The
app sleeps that long, with the same jitter distribution, in `Connection#connect`,
`BenchChannel#subscribed` and `Connection#disconnect`. A sleep releases the GVL like the I/O it
stands for, such as a database query. The scenario still measures connect, subscribe and
recovery times. Consequences:

- `rpc_*_calls` are 0, and `max_command_stall_seconds` is -1 (it's measured on RPC calls).
- AnyCable has a disconnect queue; Action Cable runs `disconnect` on the worker pool, so there's
  no queue gauge to read. `wave_<n>_drained_seconds` is -1, and the last wave ends at recovery.
- The probe clients send AnyCable's client `ping` command. Action Cable doesn't implement it (it
  logs an unrecognized command), so `probe_*` is empty (`probe_count=0`).

## Fairness caveats

- **One core of Ruby.** The GVL lets only one thread run Ruby at a time, so Action Cable uses
  about 1 core however many the server is pinned to. Go and Rust use all 4. That's how Action
  Cable runs in a single process. Scaling it out means several processes behind Redis, which
  adds a pub/sub hop the other servers don't have here.
- **The default path per message.** Each broadcast is JSON-encoded once, then decoded and
  re-encoded for every subscriber on the worker pool. That's Action Cable's default
  `stream_from`. Passing a custom coder or callback could skip work, but typical apps don't.
- **Ordering.** Per-subscriber deliveries are separate worker-pool tasks, so under load two
  broadcasts to the same client can arrive out of order. The harness counts that as
  `ordering_violations` and exits 1. It's reported in the table, not hidden. A small smoke run at
  40k msg/s saw 7.
- **Saturation.** Where Action Cable can't sustain a workload, `bench-summary.py` says so: the
  delivered share, missing messages, the delivered rate against the expected one, and the
  publisher's evidence (dropped publications, POST round trips). The charts show "not
  sustained" instead of a bar.
