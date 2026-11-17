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

## Where things stand
- M0 (Go baseline) is done. M1 (test port) is done or finishing: all 923 upstream cases are
  translated, and nearly all are reviewed and verified against Go.
- M2+ (implementation) is in progress: stubs are `pending!`, and a case counts as done once it
  passes with `cargo xtask test --backend rust` and is recorded with `--record`.

## Setup
```sh
mise install                       # Rust 1.100.0, Go 1.27.3, protoc (mise.toml)
cargo xtask reference-checkout     # pinned upstream into compat/reference (gitignored)
cargo xtask reference-mruby        # libmruby for the `mrb` tag (built in Docker, Ruby 2.7)
cargo xtask reference-build        # Go reference binary + bridges into target/
eval "$(compat/scripts/services.sh start)"   # Redis + NATS in Docker, prints env to export
cargo xtask test --backend go      # translated tests against the Go reference
cargo xtask test --backend rust    # against Cabby: pass or explicitly pending
```
CI (GitHub Actions) runs the same steps.

## Conventions
- Commits end with `Co-Authored-By: Claude …`, as in the existing history. Never add Claude
  attribution to upstream PRs.
- Don't commit `compat/reference/` or agent worktrees (`.claude/`).
- Parallel agent branches: after merging, run `python3 compat/scripts/normalize-tests-manifest.py`,
  de-duplicate `pub mod` lines in registries, then `cargo xtask inventory`,
  `cargo xtask test --backend go --record` and `--backend rust`.
- Use zsh-safe shell: write `${VAR}:path`, not `$VAR:path` (zsh treats `:p`, `:c` and similar as modifiers).
