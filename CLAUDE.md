# Cabby: working notes for Claude Code

Cabby is an AnyCable-compatible server in Rust, a port of the Go reference pinned in
`compat/upstream.lock` (anycable/anycable@26a1a703). The repository is the source of truth for
plan and progress. Don't rely on earlier conversations.

## Read first
- `SPEC.md`: the plan and milestones (M0–M7), rules, and the owner's end-of-project requests (§11).
- `docs/translating-tests.md`: how upstream tests were translated (annotations, facades, Go bridge).
- `docs/implementing.md`: rules for the implementation phase (M2+). **Tests are the spec.**
- `compat/deviations.md`: every observable difference from Go, adopted or proposed.
- `cargo xtask report --by-package`: live progress, from `compat/test-map.json`.

## Where things stand (2026-11-21)
- **Done:** all 923 upstream cases pass (1191 tests). The general review
  (`docs/review/2026-11-fable-review.md`) and the security review
  (`docs/review/2026-11-security-review.md`) are fixed. The README has the three-way benchmark
  (Action Cable vs AnyCable vs Cabby), with raw results in `docs/benchmarks/results`.
- **Next:**
  1. Pusher-protocol benchmark (Cabby/AnyCable Pusher modes vs Soketi and Laravel Reverb), needing
     a Pusher client mode in `crates/cabby-bench`.
  2. A soak test for AnyCable #265.
- **Security report to AnyCable: not sent, by the owner's decision.** `?pi=9223372036` crashes the
  Go server, and `$pusher` is reachable from `/cable` without a signature; both are fixed in Cabby
  (D-054, D-055). Keep the details out of the README.
- **Go is removed from this machine.** Go-backend runs (`cargo xtask test --backend go`) happen in
  CI (Go is no longer in `mise.toml`; see the comment there to run it locally). Kept: `target/reference-bin/anycable-go` (the
  benchmarks need it) and `target/test-services/nats-server` (Cabby's tests need it). Move both
  aside before any `cargo clean`.
- **Benchmarks only run on this Linux box,** with the machine idle (`taskset`, `/proc`). The
  Action Cable app needs its gems:
  `cd bench/actioncable && mise exec ruby@4.0.9 -- bundle config set --local path vendor/bundle && mise exec ruby@4.0.9 -- bundle install`.
- Owner preference: run tests and benchmarks yourself and report; don't walk the owner through
  manual testing unless asked.

## Setup
```sh
mise install                       # Rust 1.100.0, Go 1.27.3, protoc (mise.toml); Docker needed
cargo xtask reference-checkout     # pinned upstream into compat/reference (gitignored)
cargo xtask reference-mruby        # libmruby for the `mrb` tag (built in Docker, Ruby 2.7)
cargo xtask reference-build        # Go reference binary + bridges into target/
eval "$(compat/scripts/services.sh start)"   # Redis + NATS in Docker, prints env to export
cargo xtask test --backend go      # translated tests against the Go reference
cargo xtask test --backend rust    # against Cabby: pass or explicitly pending
```
CI (GitHub Actions) runs the same steps. On macOS everything but the benchmarks should work
(Docker Desktop for Redis/NATS and the libmruby build); the benchmarks need Linux.

## Conventions
- Commits end with `Co-Authored-By: Claude …`, as in the existing history. Never add Claude
  attribution to upstream PRs.
- Don't commit `compat/reference/` or agent worktrees (`.claude/`).
- Parallel agent branches: after merging, run `python3 compat/scripts/normalize-tests-manifest.py`,
  de-duplicate `pub mod` lines in registries, then `cargo xtask inventory`,
  `cargo xtask test --backend go --record` and `--backend rust`.
- Use zsh-safe shell: write `${VAR}:path`, not `$VAR:path` (zsh treats `:p`, `:c` and similar as modifiers).
