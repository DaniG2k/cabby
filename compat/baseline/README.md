# Go reference baseline

Reference: `anycable/anycable@26a1a703` (see `../upstream.lock`). Recorded 2026-11-17 on
Linux x86_64. Raw `go test -json` streams and runner logs sit next to this file.

| Suite | Command | Result | Artifact |
|---|---|---|---|
| Main module, no mruby | `go test -count=1 -race -json ./...` | 738 pass, 1 skip | `go-unit-nomrb.jsonl` |
| Main module, `mrb` tag | `go test -count=1 -race -tags mrb -json ./...` | 739 pass, 1 skip | `go-unit-mrb.jsonl` |
| Benchi (`etc/benchi`) | `go test -modfile=<tidied> -race -json ./...` | 65 pass | `go-benchi.jsonl` |
| Vendored go-mruby | `go test -vet=off -json ./...` | 57 pass | `go-mruby-vendored.jsonl` |
| AnyT conformance, 9 Makefile targets | `compat/scripts/reference-conformance.sh` | all pass (32–41 tests each) | `conformance/` |
| Feature scenarios (16 `.testfile`) | `ruby features/runner.rb <file>` | 16/16 pass | `features/` |
| Benchmark scenarios (2 `.benchfile`) | — | not yet run | — |

Counts are `go test -json` test events (top-level tests plus subtests), not upstream case
counts. Case-level mapping lives in `../test-map.json`.

The single skip in both main-module runs is `node TestDisconnectQueue_Shutdown/Stops_after_timeout`,
skipped in source. SPEC §6 requires a deterministic active equivalent in the Rust port.

## Environment adaptations (each one is needed to reproduce, none changes source)

1. **Benchi `go.mod` is stale at the pinned commit.** `go test` refuses to run (`updates to
   go.mod needed`): the root module moved to grpc 1.85.0 / nats.go 1.55.0 and Benchi's
   indirect requirements were not re-tidied. We run with `-modfile` pointing to a tidied copy in
   `../reference-adapters/benchi-modfile/` (`go.mod.patch` shows the two-line diff). Upstream
   status: `upstream_failure` (build), not a behavior failure.
2. **Vendored go-mruby fails `go vet`.** Modern `go vet` (run implicitly by `go test`) rejects
   `Example_CustomFunction`, `Example_Decode`, `Example_SimulateFiles` as malformed example
   names, so the package never builds. Run with `-vet=off`; the three examples then execute and
   their `// Output:` checks pass (they are among the 57 passing events).
3. **libmruby needs Ruby < 3.** Built `libmruby.a` (mruby 1.2.0) in `ruby:2.7` with bison, as upstream
   CI does. The host links it with gcc 15.
4. **Conformance needs `REDIS_URL`.** Without it the Makefile silently falls back to the HTTP
   broadcast adapter. For the SSL target AnyT then posts plain HTTP to the TLS port and 15 broadcast
   tests error. Upstream CI sets `REDIS_URL` job-wide, so the script does too.
5. **Feature runner deadlocks on small pipes (latent upstream bug).** `BenchRunner#run` waits
   for exit (`poll_for_exit`) before reading the child's stdout/stderr pipe. `--print-config`
   writes 9604 bytes. Inside a container running as root, the root user is over
   `fs.pipe-user-pages-soft`, so new pipes get 8 KiB and the child blocks at byte 8168, which shows up as
   "Command expected to finish in 120s". Running as an unprivileged user (64 KiB pipes)
   passes. The Rust feature driver must drain output concurrently. That's an improvement, not a
   behavior change (recorded in `../deviations.md`).
