# Embedded NATS (`enats`): strategy

Upstream links the NATS server into the AnyCable binary as a Go library
(`github.com/nats-io/nats-server/v2` v2.15.1, pinned by `compat/reference/go.mod`).
`enats.Service` builds `server.Options` from `enats.Config`, starts the server in-process,
waits up to 5 s for it to accept connections, and probes JetStream over an in-process
connection. `--embed_nats` turns it on, and the NATS broker and pub/sub then connect to it.

SPEC.md §3 requires an explicit packaging strategy and asks final claims to disclose any
remaining Go runtime dependency. It does not require rewriting NATS.

## Options considered

1. **Run the official `nats-server` binary as a supervised child process.** (chosen)
2. Embed the Go server through FFI: build it as a C archive with cgo and call it from Rust.
   This puts a Go runtime inside the Cabby process and needs `unsafe` FFI, which the
   workspace forbids (`unsafe_code = "forbid"`). It also makes the build depend on a Go
   toolchain and cgo.
3. Write a NATS server in Rust. No maintained implementation exists (with clustering,
   gateways and JetStream), and rewriting it is out of scope.
4. Drop embedding and require an external NATS deployment. That leaves the feature unported.

## The chosen design (`cabby_server::enats::Service`)

- **Binary lookup:** `CABBY_NATS_SERVER`, then `nats-server` next to the Cabby executable,
  then `nats-server` on `PATH`. Releases should bundle the binary next to `cabby`, at the
  version pinned by the reference `go.mod` (the test kit builds exactly that version into
  `target/test-services/nats-server`).
- **Configuration:** `Service::server_config` writes a nats-server configuration file that
  mirrors upstream's `server.Options`:
  - server name: the configured name, or the service address plus a random suffix, as upstream;
  - host and port from `service_addr`;
  - `debug`, `trace` and `max_payload`;
  - JetStream with `store_dir`, defaulting to `$TMPDIR/nats-data/<server name>`;
  - the cluster (name, host, port, routes);
  - the gateway (name, host, port, advertise address, remote gateways in
    `name:url1,url2` form).

  Address parsing and error texts follow upstream.
- **Startup:** `start` spawns `nats-server -c <file>` and waits up to 5 s
  (upstream `serverStartTimeout`) for the port to accept TCP connections. If the process
  exits early, the error includes its output. As upstream does, `start` doesn't wait for
  JetStream: `wait_jetstream_ready(max_seconds)` polls JetStream's account info over a
  regular client connection.
- **Logs:** the server's output is always drained. It is echoed to stderr when `debug` or
  `trace` is set. Upstream routes server logs to its logger only in debug mode. Only the last
  64 KiB are kept, for start-up and exit errors.
- **Supervision:** once started, the child is checked every 200 ms. If it exits before
  `shutdown`, `embedded NATS server exited unexpectedly` is logged (`context=enats`, with the
  exit status and the end of its output) and the runner exits 1 with that error (D-044):
  upstream's in-process server can't die and leave AnyCable running.
- **Shutdown:** `shutdown` (and `Drop`) sends SIGINT for a graceful stop (JetStream
  flushed) and kills the process if it hasn't exited after 10 s. Upstream disables
  JetStream, then shuts the in-process server down.

## Differences from upstream (disclosure)

- Embedded NATS adds a **runtime dependency on the Go-built `nats-server` binary**. It is a
  separate process, not part of the Cabby binary.
- If Cabby is killed with SIGKILL, the child server keeps running. Linux
  `PR_SET_PDEATHSIG` would need `unsafe`.
- Startup failures report the server's exit status and output instead of
  `server.NewServer` errors.
- JetStream readiness is probed over TCP instead of an in-process connection.

## Status

- Done: `enats::Config` (TOML, defaults), plus `enats::Service` with start, readiness,
  JetStream readiness, description and shutdown. Unit tests cover configuration generation
  and a real start/stop against the test-services `nats-server`
  (`cabby-server` `enats::tests`).
- Not yet done: wiring `--embed_nats` into the server launch (the launcher and transports
  belong to the server milestone), so the CLI doesn't start the service yet.
- Translated tests that need a NATS server (the broker, pub/sub and node integration
  cases) use an external `nats-server` process (`cabby_testkit::nats::NatsServer`, or the
  shared `CABBY_NATS_URL`), as upstream uses `enats`. They pass on Cabby.
- Still pending because they need the CLI wiring and the transport layer:
  - `features/enats_broker.testfile#scenario`
  - `features/enats_clusters.testfile#scenario`
