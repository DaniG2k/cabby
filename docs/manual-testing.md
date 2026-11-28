# Testing Cabby by hand

A walk-through you can do in about ten minutes with two or three terminals. Every step says
what to run and what you should see. Run the commands from the repository root
(`~/Documents/apps/cabby`).

You need the toolchains from `mise.toml` (`mise install`) and Node 22 or newer for the small
WebSocket client in `examples/` (`mise exec node@26 -- node …` uses the Node that mise installed).

## 1. Build the server

```sh
cargo build --release -p cabby-server
```

The binary is `target/release/cabby`. `target/release/cabby --help` lists the same flags as
AnyCable, with `CABBY_*` environment variables (`ANYCABLE_*` works too).

## 2. Start Cabby (terminal A)

```sh
target/release/cabby --public --broadcast_adapter=http --http_broadcast_port=8080 \
  --metrics_http=/metrics --sse
```

- `--public` accepts every connection and subscription without a Rails app (for testing only).
- `--broadcast_adapter=http --http_broadcast_port=8080` takes broadcasts as HTTP requests on the
  main port, so no Redis is needed.
- `--metrics_http=/metrics` and `--sse` turn on the Prometheus endpoint and Server-Sent Events.

You should see, among others (times are in your local time zone):

```text
INF Starting Cabby 0.5.4 (AnyCable 1.6.22 compatible) (pid: …, open file limit: …, gomaxprocs: …)
WRN Server is running in the public mode
INF Accept broadcast requests at http://localhost:8080/_broadcast (no authorization)
INF Handle WebSocket connections at http://localhost:8080/cable
INF Handle health requests at http://localhost:8080/health
INF Handle SSE requests at http://localhost:8080/events
INF Serve metrics at http://localhost:8080/metrics
```

The start line shows Cabby's version and the AnyCable server version it is compatible with.
`open file limit` caps the number of connections: raise it with `ulimit -n 65536` in that
terminal before starting Cabby if you want to test thousands of clients.

## 3. Check that it is alive (terminal B)

```sh
curl -i localhost:8080/health
```

Expect `HTTP/1.1 200 OK` and the body `Ah, ha, ha, ha, stayin' alive, stayin' alive.` (the same
health text as AnyCable).

## 4. Connect a WebSocket client (terminal B)

```sh
mise exec node@26 -- node examples/ws-client.mjs demo
```

It connects with the Action Cable protocol and subscribes to the public stream `demo`:

```text
10:31:36.847 connected to ws://localhost:8080/cable
10:31:36.855 <- {"type":"welcome","sid":"…"}
10:31:36.855 -> {"command":"subscribe","identifier":"{\"channel\":\"$pubsub\",\"stream_name\":\"demo\"}"}
10:31:36.856 <- {"type":"confirm_subscription","identifier":"{\"channel\":\"$pubsub\",\"stream_name\":\"demo\"}"}
```

Leave it running. (Pings arrive every 3 seconds and are hidden; `SHOW_PINGS=1` shows them.)

## 5. Broadcast a message (terminal C)

```sh
curl -i -X POST localhost:8080/_broadcast -H 'Content-Type: application/json' \
  -d '{"stream":"demo","data":"{\"text\":\"hello from curl\"}"}'
```

curl prints `HTTP/1.1 201 Created`, and terminal B receives:

```text
<- {"identifier":"{\"channel\":\"$pubsub\",\"stream_name\":\"demo\"}","message":{"text":"hello from curl"}}
```

This is the request a Rails app makes when it broadcasts through AnyCable's HTTP adapter.

## 6. Receive the same stream over Server-Sent Events (terminal C)

```sh
curl -N 'localhost:8080/events?stream=demo'
```

You get a welcome and a confirmation as SSE events. Repeat step 5 in another terminal and the
message arrives here and in terminal B:

```text
event: welcome
data: {"type":"welcome","sid":"…"}

event: confirm_subscription
data: {"type":"confirm_subscription","identifier":"{\"channel\":\"$pubsub\",\"stream_name\":\"demo\"}"}

data: {"text":"hello from curl"}
```

Stop it with Ctrl-C.

## 7. Look at the metrics

```sh
curl -s localhost:8080/metrics | grep -E '^anycable_go_(clients_num|broadcast_msg_total)'
```

With the client from step 4 still connected:

```text
anycable_go_broadcast_msg_total 1
anycable_go_clients_num 1
```

The metric names are AnyCable's, so existing dashboards and alerts work unchanged. Gauges such
as `clients_num` refresh every 5 seconds (`--stats_refresh_interval`).

## 8. Stop the server

Press Ctrl-C in terminal A:

```text
INF Shutting down... (hit Ctrl-C to stop immediately or wait for up to 30s for graceful shutdown)
INF all active RPC calls finished
```

The client in terminal B is told to reconnect elsewhere, as AnyCable does during a deploy:

```text
<- {"type":"disconnect","reason":"server_restart","reconnect":true}
closed (code 1001, Server restart)
```

and the server exits with status 0.

## Next steps

- **With a Rails app:** run Cabby in place of `anycable-go` (same flags; `CABBY_*` or
  `ANYCABLE_*` variables; drop `--public`). `anycable-rails` and your channels need no changes.
- **The full compatibility suite:** `cargo xtask test --backend rust` (needs
  `eval "$(compat/scripts/services.sh start)"` for Redis and NATS).
- **Benchmarks:** see [`benchmarks.md`](benchmarks.md) and `compat/scripts/bench-compare.sh`.
