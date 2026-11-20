# Security review: Fable 5.2, November 2026

An independent security review of Cabby at `d7d6aa0`, by a reviewer not involved in building
it, done statically (a benchmark was running, so nothing was compiled). The reproduction
scripts are in [`repro/security/`](repro/security/); each takes a server binary as its first
argument. `${SCRATCH}` in them stands for a scratch directory.

## Verdict (reviewer)

> Cabby is a careful, defensively-written port: `unsafe` is forbidden workspace-wide and
> confined to `cabby-mruby-sys` (not reachable from network input), secret comparisons are
> constant-time, JWT `alg` is pinned to HMAC (no `none`/algorithm confusion), per-connection
> buffers and broker queues are bounded, and `Cargo.lock` has no known-vulnerable or
> unmaintained crates against the RustSec database. An unauthenticated client can panic a
> command-pool worker with a single query parameter (`?pi=<huge>`), though the panic is
> contained to that connection. When Pusher compatibility is enabled, the `$pusher` channel is
> reachable from the plain Action Cable endpoint, where the Pusher signature check is not
> installed: an authorization bypass Cabby inherits from the Go reference.

## Findings

| ID | Severity | Finding | Go | Repro |
|---|---|---|---|---|
| F1 | medium | `?pi=<huge>` (client-supplied ping interval) overflows `Duration` arithmetic in `Session::start_timers` (`node/session.rs:719`) before authentication: the session's setup panics (caught by the pool; process survives), a panic is logged per attempt. | wraps silently, no panic | `02_pi_panic.py` |
| F2 | high when Pusher is enabled | `$pusher` is routed for every endpoint (`cli/runner.rs:636-641`), but the signature check for `private-`/`presence-` channels lives only in the Pusher endpoint's executor (`pusher.rs:1360-1395`). Subscribing to `{"channel":"$pusher","stream":"private-x"}` on `/cable` is confirmed unsigned: read private channels, join presence as anyone. | same (upstream design) | `01_pusher_bypass.py` |
| F3 | medium, unconfirmed | Client messages deeper than 128 levels are decoded on a fresh thread with a 64 MiB stack (`gojson/deep.rs`), with no cap on how many run at once; possible memory amplification, and an untested risk of stack overflow on some path. | decodes in-goroutine up to 10,000 levels | `03_deep_json.py` |
| F4 | low | In public mode, presence entries and subscriptions keyed by client input grow without a per-connection cap. | same | — |

## Checked and found sound (reviewer)

- JWT: HMAC only; `none`/RS/ES/EdDSA rejected; signature checked before claims; `exp`/`nbf` enforced; constant-time verification; base64url decoding like golang-jwt.
- Signed, Turbo and CableReady streams: constant-time digest comparison; an empty key refuses; unsigned streams only with `public`.
- API, HTTP broadcast and Pusher REST: constant-time comparisons; method and auth checked before the body is read; `body_md5` is part of the signed string.
- Origin and CORS match Go; `ACAO: *` only without configured origins; disallowed WebSocket origins get 403.
- No path traversal; API routing strips query and fragment; `%` escapes need two hex digits.
- NATS/Redis subjects: reaching a broadcast needs the authenticated path; async-nats rejects wildcards in subscribe subjects.
- Network-path `expect`/`unwrap` calls are guarded (besides F1); Go's known panics (malformed Pusher/presence frames, bad disconnect rate, malformed metrics tag) are caught errors in Cabby.
- Threads and descriptors: blocked-RPC threads capped (D-036); push-model sessions; `max_conn`; accept loop backs off on EMFILE.
- No secrets in logs (URLs, headers, identifiers and signed-stream digests are masked); log-injection-safe quoting in text and JSON.
- Randomness from the OS CSPRNG.
- `unsafe` only in `cabby-mruby-sys`; mruby is reachable only from the operator's formatter script.
- TLS: rustls, TLS 1.2 minimum, 5 s handshake timeout; client TLS verifies by default.
- Supply chain: all 233 registry packages checked against `rustsec/advisory-db` (2026-11-20): 0 matches.
- GitHub Actions: actions pinned by SHA, no `pull_request_target`, read-only token, no script injection.

## Status

| ID | Status |
|---|---|
| F1 | confirmed (Cabby: a contained panic per attempt; Go: `?pi=6148914691236517206` or `?pi=9223372036` stops the process with `panic: invalid argument to Int63n`). Fixed as D-054: `?pi=` is clamped to a day, session timer intervals to ten years, timer deadlines cannot overflow |
| F2 | confirmed on both servers. Fixed as D-055: `$pusher` subscriptions are refused outside the Pusher endpoint. The public-channel namespace (repro case 1b) is how Pusher works and is unchanged; operator notes in [`docs/pusher.md`](../pusher.md) |
| F3 | no crash, but one 64 MiB-stack thread per concurrent deep message. Fixed as D-056: at most 4 such threads; a deep message that waits 250 ms for one is dropped with a warning, its session keeps reading |
| F4 | left at parity with Go (no per-connection cap): any cap is an observable limit Go does not have; connections can be bounded with `--max_conn` and frames with `--max_message_size`; documented here as parity |
