# Embedded NATS (`enats`): strategy

> **Compatibility feature.** Embedded NATS is supported and tested, and stays for deployments
> that use it with AnyCable today. For new setups, run a NATS server next to Cabby (for example
> the official `nats` image) and point `--nats_servers` at it. Cabby's release artifacts don't
> include `nats-server`.

Upstream links the NATS server into the AnyCable binary as a Go library
(`github.com/nats-io/nats-server/v2` v2.15.1, pinned by `compat/reference/go.mod`).
`enats.Service` builds `server.Options` from `enats.Config`, starts the server in-process,
waits up to 5 s for it to accept connections, and probes JetStream over an in-process
connection. `--embed_nats` turns it on, and the NATS broker and pub/sub then connect to it.

This document is the packaging strategy. The Go runtime dependency it keeps is disclosed in
[`docs/compatibility.md`](compatibility.md#runtime-dependencies); Cabby does not rewrite NATS.

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

- **Binary lookup:** `CABBY_NATS_SERVER_BIN` (or its older spelling `CABBY_NATS_SERVER`), then
  `nats-server` next to the Cabby executable, then `nats-server` on `PATH`. Release artifacts
  don't bundle it: install the version you want (the test kit builds the one pinned by the
  reference `go.mod` into `target/test-services/nats-server`).
- **Configuration:** `Service::server_config` writes a nats-server configuration file that
  mirrors upstream's `server.Options`:
  - server name: the configured name, or the service address plus a random suffix, as upstream;
  - host and port from `service_addr`;
  - `debug`, `trace` and `max_payload`;
  - JetStream with `store_dir`, defaulting to `$TMPDIR/nats-data/<server name>`;
  - the cluster (name, host, port, routes);
  - the gateway (name, host, port, advertise address, remote gateways in
    `name:url1,url2` form).

  Address parsing and error texts follow upstream. IPv6 addresses are given in brackets
  (`nats://[::1]:4222`); the file holds the bare host, and routes and gateway URLs as given.
- **Startup:** `start` spawns `nats-server -c <file>` and waits up to 5 s
  (upstream `serverStartTimeout`) for the port to accept TCP connections (on `127.0.0.1` when
  the host is empty, which listens on every interface). If the process
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
- **Signals:** the child runs in a process group of its own, so Ctrl-C in a terminal (SIGINT
  to Cabby's process group) reaches Cabby only, which stops the child after the components that
  use it, as upstream's in-process server (`NoSigs`) is.
- **When Cabby is killed:** a watchdog, `sh` started next to the child (in its own process
  group too), waits for the end of a pipe only Cabby holds open; the system closes it when Cabby
  exits however it ends (SIGKILL included). The watchdog then sends the child SIGINT and removes
  its configuration file. `shutdown`, and the supervision when the child exits on its own, end
  the watchdog first, so it never signals a reused process ID. Without `/bin/sh` (an image
  without a shell) there is no watchdog: the child outlives a killed Cabby, as before.

## Differences from upstream (disclosure)

- Embedded NATS adds a **runtime dependency on the Go-built `nats-server` binary**. It is a
  separate process, not part of the Cabby binary.
- If Cabby is killed with SIGKILL, the child server stops through its watchdog (see
  "When Cabby is killed"), shortly after rather than at once; Linux `PR_SET_PDEATHSIG`
  would need `unsafe`. Without `/bin/sh` the child keeps running.
- Startup failures report the server's exit status and output instead of
  `server.NewServer` errors.
- JetStream readiness is probed over TCP instead of an in-process connection.

## Status

Done. `--embed_nats` starts the service from the CLI, and the NATS broker and pub/sub connect to
it. The translated cases that need a NATS server use an external `nats-server` process
(`cabby_testkit::nats::NatsServer`, or the shared `CABBY_TEST_NATS_URL`), as upstream uses
`enats`, and the scenarios `features/enats_broker.testfile` and `features/enats_clusters.testfile`
run the CLI with `--embed_nats`. All of them pass on Cabby (`cargo xtask report --by-package`).
