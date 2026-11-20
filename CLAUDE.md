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

## Where things stand (2026-11-20)
- **M0–M6 done.** All 923 upstream cases pass against Cabby (1173 tests); Go suite clean
  (9 not_observable, 4 upstream_failure = AnyCable #275, fixed in Cabby as D-003).
- **Independent review** (Fable 5.2): `docs/review/2026-11-fable-review.md`, with repro
  scripts. All findings fixed except **H7 (logging parity)**; status table at the end.
- **H7 in progress** on branch `h7-logging-wip` (pushed, builds): partial log calls in hub,
  node, session, disconnect queue. Left: the remaining Go log call sites, a coverage table in
  `docs/logging.md`, and a logprobe conformance test comparing both backends' log output
  (`docs/review/repro/core/logprobe.py`). Reuse `cabby_core::logger::set_sink`/`log`.
  Deviation IDs D-050..D-054 are reserved for it.
- **Then M7 benchmarks**: three-way comparison (Action Cable vs AnyCable vs Cabby) with the
  external harness, on the fixed build, **on the Linux box with the machine idle** (the harness
  uses `taskset` and `/proc`; it doesn't run on macOS). `compat/scripts/bench-compare.sh`
  (warm-up, shuffled rounds, 5 runs; Action Cable app in `bench/actioncable`, gems via
  `mise exec ruby@4.0.9 -- bundle install` into `vendor/bundle`), then `bench-summary.py` and
  `bench-charts.py`, and rewrite the README benchmark section from the new results. The
  current README numbers are the older Go-vs-Cabby run.
- **Then the end items** in SPEC §11: remove the local Go toolchain once done, and lift the
  README's "don't use in production" note once H7 and the benchmarks are in.
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
