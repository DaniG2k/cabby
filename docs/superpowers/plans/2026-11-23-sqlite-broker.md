# SQLite Broker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `cabby --broker=sqlite --sqlite_path=<file>` keeps the epoch, stream offsets, stream history and session state across a graceful restart of a single node, and resyncs clients cleanly after a crash, with no external service.

**Architecture:** `SqliteBroker` (new, `crates/cabby-core/src/broker/sqlite.rs`) wraps the existing in-memory broker `broker::Memory`, which stays the live path for every read, offset and fan-out. A bounded write queue feeds one persister OS thread that writes batches to a WAL-mode SQLite file (write-behind). A loader fills `Memory` from the file at startup, before Cabby accepts connections. The runner selects it with `--broker=sqlite` and gives its final write the time left of the shutdown timeout.

**Tech Stack:** Rust 1.100 (edition 2024), `rusqlite =0.41.1` with `bundled` (SQLite compiled in, no system library), `std::fs::File::try_lock` for the lock file, Cabby's metrics registry and logger, the `cabby-compat` test runner (`cargo xtask`).

**Source of truth:** `docs/superpowers/specs/2026-11-23-sqlite-broker-design.md` (the approved spec). Section numbers below (§3, §5…) refer to it.


> **Owner decisions (2026-11-23):**
> - Final flush budget: the remaining shutdown time, with a 1-second floor (Task 17).
> - Telemetry: none. `main` now includes the v0.1.0 release commits, so the binary's default runner has no telemetry option (D-058, proposed); Phase A removes the module. The `ANYCABLE_DISABLE_TELEMETRY=true` in the test commands below is harmless and can be dropped.
> - Benchmark host and paths in Task 23 are private notes: scrub host aliases before pushing this branch.

---

## Ground rules for this branch

Read once before Task 0. They come from `CLAUDE.md`, `docs/implementing.md` and `docs/translating-tests.md`.

- Work only in the worktree `<worktree>`, branch `sqlite-broker`. Don't push to GitHub (Task 23 pushes the branch to the Linux box over SSH only).
- Run every cargo command through mise: `mise exec -- cargo …` (Rust 1.100.0 and protoc 36.2 come from `mise.toml`). Export `CARGO_BUILD_JOBS=2` if other agents share the machine.
- **Tests are the spec.** Never edit a translated upstream test (`tests/unit/**`, `tests/conformance/anyt_*.rs`, the ported `tests/features/*.rs`). The new tests in this plan are Cabby's own: unit tests inside the crates, plus two new modules in the compat crate (`tests/conformance/sqlite.rs`, `tests/features/sqlite.rs`).
- **Observable changes need a deviation first.** `--broker=sqlite` is recorded as **D-065** in `compat/deviations.md` (Phase A reserves D-058 to D-064). Task 1 records it as *Proposed*; Task 22 moves it to *Implemented in Cabby*.
- Every `#[test]` in the compat crate needs a `/// regression-for:` (or `/// upstream:`/`/// external-for:`) annotation naming an existing upstream case ID from `compat/test-map.json` (without the `26a1a703:` prefix), or `cargo xtask inventory` fails. Cabby-only behaviour becomes a `rust_only` entry. The Go arm of a scenario AnyCable cannot run panics with `unimplemented!("not_observable: …")`; `cargo xtask test --backend go --record` then stores `reference_status: not_observable`.
- No `unsafe` (`unsafe_code = "forbid"`), fmt and `clippy -D warnings` clean at every commit, typed errors (`thiserror`), never hold a lock across a network wait.
- Known baseline: on this Mac, `cargo clippy` of `cabby-testkit` (so also `-p cabby-compat` and `--workspace`) already fails before any change with `clippy::large_enum_variant` at `crates/cabby-testkit/src/reference/benchi.rs:514` (`enum Client`), checked 2026-11-23 on `d9d2167`. It is not this branch's to fix; a clippy step whose only error is that one counts as clean. CI (Linux) is the reference.
- No sleeps for synchronization in tests: wait on observations with deadlines. Inherent windows (a clock that must move, a lock timeout) get a comment.
- Commit at the end of every task. Messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; the worktree's git identity is already the noreply address. Note every new dependency in its commit message.
- zsh-safe shell: write `${VAR}:path`, never `$VAR:path`.

## Decisions this plan makes where the spec is silent or the code disagrees

1. `Memory::set_epoch` already exists (`crates/cabby-core/src/broker.rs:519`, used by the NATS broker), so §3's `set_epoch` needs no new code; the loader calls it.
2. Two more small `Memory` methods, besides the four in §3: `commit_session_data(sid, data) -> i64` and `touch_session_deadline(sid) -> Option<i64>`. They return the expiry the memory broker set, so the persisted `expires_at_ms` is exactly the in-memory one, and a touch for an unknown session is never queued. `commit_session`/`touch_session` delegate to them, so `memory` behaves as before.
3. No `WriteOp::Epoch`. The loader writes the epoch and `clean_shutdown = '0'` synchronously in one transaction before Cabby accepts connections (§5 step 3 needs that durable anyway); nothing changes the epoch at runtime.
4. The schema adds `CREATE INDEX streams_deadline ON streams (deadline)`: §4 prunes `streams` by deadline "using indexes, never full scans", and the §4 schema has no index for it.
5. `streams.max_seq` upserts take the maximum of the stored and the batch's newest offset, except when the stored row's deadline has passed (the memory broker dropped the stream and restarted its offsets at 1): then the batch's value wins. Concurrent broadcasts on one stream can enqueue their history ops out of offset order, so "last op wins" could move `max_seq` backwards.
6. Overflow is push-then-trim: the incoming op counts as queued, so a history op arriving at a queue full of session ops is itself the dropped one (§3: "drops the oldest History op; only if none is queued…").
7. The metrics registry has no labels, so "`sqlite_dropped_total`, labelled by kind" becomes `sqlite_dropped_total` plus `sqlite_dropped_history_total`, `sqlite_dropped_session_touch_total` and `sqlite_dropped_session_commit_total`.
8. A third startup-line variant for a fresh file: `(epoch abcd, new database)`, and a fourth after moving a corrupt file: `(epoch abcd, new database, the corrupt file was moved to …)`.
9. No cargo feature gate. §3 says "the feature is on by default": the SQLite broker is always compiled in. (A `sqlite` cargo feature would need `default-features = false` on the workspace `cabby-core` dependency and every dependent crate to opt in; not worth it for ~1 MB.)
10. The end-to-end "final flush fails" test (§9) can't make the file read-only from outside: `chmod` doesn't affect the descriptors Cabby already holds. The unit test uses a read-only connection (`SQLITE_READONLY`); the binary test holds SQLite's write lock from another connection (`BEGIN EXCLUSIVE`), so every write Cabby tries at shutdown fails with `SQLITE_BUSY`.
11. `docs/configuration.md` doesn't exist in this repo, so there is no configuration page to update here (the site has one, see Task 24).

## File map

**cabby-core** (`crates/cabby-core/`)
- Modify `src/broker.rs`: `Recorded`, `Memory::handle_broadcast_recorded`, `restore_stream`, `restore_session_with_deadline`, `commit_session_data`, `touch_session_deadline`; `Memstream::add_recorded`, `Memstream::restore`; `MemoryInner::session_deadline`, `cache_session`; the test clock moves to `#[cfg(test)] pub(crate) mod test_clock`; `mod sqlite` and its re-exports.
- Create `src/broker/sqlite.rs`: module docs, `SqliteOptions`, `SqliteObserver`, `NoopObserver`, `SqliteBroker` and its `Broker` impl, the shared `#[cfg(test)] testing` helpers.
- Create `src/broker/sqlite/units.rs`: every unit conversion (§4).
- Create `src/broker/sqlite/store.rs`: connection settings, schema and migrations, `meta`, batch writes, pruning, checkpoint, error classification.
- Create `src/broker/sqlite/queue.rs`: `WriteOp`, `OpKind`, `Dropped`, the bounded `WriteQueue`.
- Create `src/broker/sqlite/files.rs`: directory and file modes, the lock file, moving a corrupt file aside.
- Create `src/broker/sqlite/persister.rs`: `Database`, `Timing`, `ErrorLog`, `Persister` (batches, retries, pruning, the final flush).
- Create `src/broker/sqlite/loader.rs`: `LoadReport`, `Previous`, `Loaded`, `load`.
- Modify `Cargo.toml`: `rusqlite.workspace = true`.

**cabby-server** (`crates/cabby-server/`)
- Modify `src/config.rs`: `SqliteConfig`, `Config::sqlite`, `[sqlite]` in `to_toml`, tests (TOML, round trip, preset precedence).
- Modify `src/cli/options.rs`: `SQLITE` help category, `sqlite_flags` (`--sqlite_path`, `CABBY_SQLITE_PATH`), tests.
- Create `src/cli/sqlite.rs`: `sqlite_options` (startup validation), `MetricsObserver`, `SqliteRunnerBroker` (shutdown with the time left).
- Modify `src/cli.rs`: `mod sqlite;`.
- Modify `src/cli/runner.rs`: `new_sqlite_broker` and the `"sqlite"` arm of `new_broker`.

**Tests and tooling**
- Modify `Cargo.toml` (workspace): `rusqlite` workspace dependency.
- Modify `crates/cabby-testkit/Cargo.toml`, `crates/cabby-testkit/src/lib.rs`; create `crates/cabby-testkit/src/sqlite_broker.rs` (helpers shared by both compat modules).
- Create `tests/conformance/sqlite.rs` (D-065 pin), modify `tests/conformance/main.rs`.
- Create `tests/features/sqlite.rs` (§9 end-to-end), modify `tests/features/main.rs`.
- Modify `compat/test-map.json` (generated: `cargo xtask inventory`, `--record`).
- Modify `compat/deviations.md`, `README.md`.
- Create `bench/sqlite/cabby-memory`, `bench/sqlite/cabby-sqlite`; modify `compat/scripts/bench-compare.sh`; create `docs/benchmarks/sqlite-results/` (generated).

## Task list

0. Baseline
1. Record D-065 as proposed
2. The `rusqlite` dependency and the cross-compile check
3. `Memory`: the shared test clock and `handle_broadcast_recorded`
4. `Memory::restore_stream`
5. `Memory` session helpers: `restore_session_with_deadline`, `commit_session_data`, `touch_session_deadline`
6. The `sqlite` module skeleton, test helpers and `units`
7. `store`: opening, pragmas, schema, migrations, `meta`, corruption classification
8. `queue`: the bounded write queue
9. `store`: batch writes and the `streams` upsert
10. `store`: pruning, load queries, checkpoint, and their query plans
11. `files`: modes, the lock file, moving a corrupt file aside
12. `persister`: batching, retries, rate-limited logs, pruning, the final flush
13. `loader`: lock, checks, epoch, streams, sessions, startup line
14. `SqliteBroker`
15. Configuration: `[sqlite] path` and the preset precedence
16. CLI: `--sqlite_path` and `CABBY_SQLITE_PATH`
17. Runner wiring, startup validation, metrics, shutdown budget
18. Testkit helpers for scenarios through the binary
19. D-065 conformance scenario
20. End-to-end feature tests (§9)
21. Manifest: inventory and recorded statuses
22. Docs: deviation entry and README
23. Benchmark on the Linux box
24. Handoff: what the cabby-site repo needs (§10)

---

### Task 0: Baseline

**Files:** none changed (one gitignored symlink).

- [ ] **Step 1: Check the branch**

Run:
```sh
cd <worktree>
git status --short --branch
git log --oneline -3
```
Expected:
```
## sqlite-broker
d9d2167 Spec: SQLite broker, second review
204bb2f Spec: SQLite broker, revised after design review
473150f Spec: SQLite broker (durable history and sessions on one node)
```
(plus the plan commit on top once this plan is committed). The tree must be clean.

- [ ] **Step 2: Link the pinned upstream checkout**

`compat/reference/` is gitignored and missing in this worktree. The AnyT SSL variant (`etc/ssl/server.crt`), `features/mruby_printers` (`features/simple_logger.rb`) and `cargo xtask inventory` read it. Link the main checkout's copy (the inventory tool canonicalizes it, see `tools/xtask/src/inventory.rs` `go_ast`):

```sh
ln -s <repo>/compat/reference compat/reference
git status --short
```
Expected: no output (the link is ignored by `.gitignore` `/compat/reference/`).

- [ ] **Step 3: Unit test counts**

Run:
```sh
mise exec -- cargo test -p cabby-core --lib 2>&1 | grep "^test result"
mise exec -- cargo test -p cabby-server 2>&1 | grep "^test result"
```
Expected (measured on 2026-11-23):
```
test result: ok. 75 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; …
test result: ok. 162 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; …
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; …
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; …
```
(cabby-core lib; cabby-server lib, bin `cabby`, doctests). Write the numbers down: later tasks state the expected totals as "baseline + n".

- [ ] **Step 4: Compatibility suite, Rust backend**

Run (Docker must be up; the nats-server test binary lives only in the main checkout):
```sh
eval "$(compat/scripts/services.sh start)"
export CABBY_NATS_SERVER=<repo>/target/test-services/nats-server
mise exec -- cargo xtask test --backend rust 2>&1 | tee /tmp/cabby-baseline.txt | grep -E "^backend|^  tests|compatibility error"
```
Expected on macOS:
```
backend rust: 1191 Rust tests
  tests:  {"fail": 3, "pass": 1188}
Error: 6 compatibility error(s)
```
The 3 failures are `conformance::threads::{broadcast_fan_out_does_not_add_threads, idle_connections_do_not_add_threads, idle_pusher_connections_do_not_add_threads}`: they read `/proc/<pid>/status` and only run on Linux (each prints a `FAIL` and a `REGRESSION` line, hence 6 errors). On the Linux box and in CI the suite is green. A one-off `failed to start broadcaster … Address already in use` is a port collision of the HTTP broadcast listener; rerun that test alone (`mise exec -- cargo xtask test --backend rust <test name>`). Without the Step 2 link, 32 `anyt_ssl` tests and `mruby_printers` fail as well.

- [ ] **Step 5: Lint baseline**

Run: `mise exec -- cargo clippy --workspace --all-targets -- -D warnings 2>&1 | grep -E "^error" -A2 | head -4`
Expected on macOS: only `error: large size difference between variants` at `crates/cabby-testkit/src/reference/benchi.rs:514` (the known baseline in the ground rules). `mise exec -- cargo clippy -p cabby-core -p cabby-server --all-targets -- -D warnings` is clean.

- [ ] **Step 6: Commit**

Nothing to commit (the link is ignored). Go on to Task 1.

---

### Task 1: Record D-065 as proposed

The repo rule: an observable change gets its deviation entry, with its pinning test, before the implementation.

**Files:**
- Modify: `compat/deviations.md` (the *Proposed (not implemented yet)* table, below the `D-005` row)

- [ ] **Step 1: Add the row**

In `compat/deviations.md`, in the table under `## Proposed (not implemented yet)`, add this row directly below the line that starts with `| D-005 | Memory stability |`:

```markdown
| D-065 | SQLite broker (`--broker=sqlite`, `--sqlite_path`, `CABBY_SQLITE_PATH`, `[sqlite] path`) | No such broker: `--broker=sqlite` stops the server at startup (`Unsupported broker adapter: sqlite`) and `--sqlite_path` is not a flag | A Cabby addition, observable only when chosen, apart from the new `--sqlite_path` line in `--help` and the `[sqlite]` section of `--print-config`: the memory broker plus a write-behind copy in one SQLite file. A graceful restart keeps the epoch, stream offsets, history (within `history_limit` and `history_ttl`) and sessions (with their original expiry); after a crash the epoch rotates, so clients resync. Presence is not persisted | durable history on one node without Redis or NATS (`docs/superpowers/specs/2026-11-23-sqlite-broker-design.md`) | `conformance::sqlite::deviation_d065_sqlite_broker_resumes_history_and_sessions_after_a_graceful_restart` | not observable: AnyCable has no SQLite broker |
```

- [ ] **Step 2: Check the table still renders**

Run: `grep -c "^| D-065 |" compat/deviations.md`
Expected: `1`

- [ ] **Step 3: Commit**

```sh
git add compat/deviations.md
git commit -m "Deviations: propose D-065, the SQLite broker

Recorded before the implementation, with its pinning conformance scenario.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The `rusqlite` dependency and the cross-compile check

**Files:**
- Modify: `Cargo.toml` (workspace `[workspace.dependencies]`, after the `redis = …` line)
- Modify: `crates/cabby-core/Cargo.toml` (`[dependencies]`)

`rusqlite` 0.41.1 was published on 2026-10-28 (0.42.0 on 2026-11-11 and 0.43.0 on 2026-11-15 are less than two weeks old). It builds `libsqlite3-sys` 0.39.1, bundling SQLite 3.54.1. `default-features = false` drops the wasm-only FFI; `cache` keeps `prepare_cached`.

- [ ] **Step 1: Add the workspace dependency**

In `Cargo.toml`, directly after the `redis = { version = "=1.8.1", … }` line, add:

```toml
# SQLite broker (D-065): SQLite compiled in from the bundled amalgamation (public domain), so no
# system library is needed; `cache` keeps `prepare_cached`.
rusqlite = { version = "=0.41.1", default-features = false, features = ["bundled", "cache"] }
```

- [ ] **Step 2: Use it in cabby-core**

In `crates/cabby-core/Cargo.toml`, in `[dependencies]`, after `prost.workspace = true`, add:

```toml
rusqlite.workspace = true
```

- [ ] **Step 3: Build**

Run: `mise exec -- cargo build -p cabby-core 2>&1 | tail -3`
Expected: `Finished \`dev\` profile …` (the first build compiles `libsqlite3-sys` with the C compiler).

Run: `mise exec -- cargo tree -p cabby-core -i libsqlite3-sys --depth 1`
Expected:
```
libsqlite3-sys v0.39.1
└── rusqlite v0.41.1
    └── cabby-core v0.1.0 (…/crates/cabby-core)
```

- [ ] **Step 4: Cross-compile check for the Linux release targets**

The release builds are `cargo zigbuild` builds for glibc 2.28 (§3). The repo has no zigbuild setup yet, so run it through mise (Zig 0.15.2 and cargo-zigbuild 0.24.1 are installed on first use; nothing is added to `mise.toml`):

```sh
mise exec -- rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
for target in x86_64-unknown-linux-gnu.2.28 aarch64-unknown-linux-gnu.2.28; do
  mise exec zig@0.15.2 cargo:cargo-zigbuild@0.24.1 -- \
    cargo zigbuild --release -p cabby-core --target "${target}" 2>&1 | tail -1
done
```
Expected, twice: `Finished \`release\` profile [optimized] target(s) in …`. This compiles the bundled SQLite C code with `zig cc` for both targets. (Task 22 repeats it for the whole `cabby` binary.) If `cargo zigbuild` is not wanted locally, the same two commands run on the Linux box.

- [ ] **Step 5: Commit**

```sh
git add Cargo.toml Cargo.lock crates/cabby-core/Cargo.toml
git commit -m "cabby-core: add rusqlite for the SQLite broker

New dependency: rusqlite =0.41.1 (2026-10-28, MIT) with the bundled feature, which
builds libsqlite3-sys 0.39.1 with SQLite 3.54.1 (public domain) compiled in; no
system library. Checked with cargo zigbuild for x86_64/aarch64-unknown-linux-gnu.2.28.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `Memory`: the shared test clock and `handle_broadcast_recorded`

`handle_broadcast_recorded` does exactly what `Memory::handle_broadcast` (`broker.rs:674-687`) does and returns where the message landed, taken from the critical section of `Memstream::add` (`broker.rs:308-315`) that assigns it. The test clock moves out of `mod tests` so the SQLite module's tests can use it.

**Files:**
- Modify: `crates/cabby-core/src/broker.rs` (after `Entry`, lines 267-274; `Memstream::add`, lines 308-315; `impl Memory`, lines 491-539; `Broker for Memory::handle_broadcast`, lines 674-687; `mod tests`, lines 983-1005)

- [ ] **Step 1: Move the test clock**

In `crates/cabby-core/src/broker.rs`, delete the `TestClock` struct and its two `impl` blocks from `mod tests` (lines 988-1005, from `/// A clock tests move by hand.` to the end of `impl Clock for TestClock`). Insert this module directly above `#[cfg(test)]\nmod tests {`:

```rust
/// A clock tests move by hand (shared by the memory and SQLite broker tests).
#[cfg(test)]
pub(crate) mod test_clock {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{Clock, lock};

    /// Where every [`TestClock`] starts, in unix seconds.
    pub(crate) const T0: i64 = 1_700_000_000;

    pub(crate) struct TestClock(Mutex<SystemTime>);

    impl TestClock {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self(Mutex::new(UNIX_EPOCH + Duration::from_secs(T0.unsigned_abs()))))
        }

        pub(crate) fn advance(&self, d: Duration) {
            *lock(&self.0) += d;
        }
    }

    impl Clock for TestClock {
        fn now(&self) -> SystemTime {
            *lock(&self.0)
        }
    }
}
```

and add at the top of `mod tests`, after `use crate::common::StreamMessageMetadata;`:

```rust
    use super::test_clock::TestClock;
```

- [ ] **Step 2: Check the move**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests 2>&1 | grep "^test result"`
Expected: `test result: ok. 9 passed; …` (the existing broker tests, unchanged; the filter does not match `broker::nats::tests`).

- [ ] **Step 3: Write the failing tests**

Add to `mod tests` in `crates/cabby-core/src/broker.rs`, after `memory_handle_broadcast_stores_unless_transient`:

```rust
    #[test]
    fn memory_handle_broadcast_recorded_reports_offset_and_timestamp() {
        let clock = TestClock::new();
        let rec = Arc::new(Recorder::default());
        let broker = Memory::new(Some(rec.clone()), None, &Config::new(), clock.clone());
        broker.set_epoch("e1");

        assert_eq!(
            Some(Recorded { offset: 1, timestamp: 1_700_000_000 }),
            broker.handle_broadcast_recorded(&msg("s", false)).unwrap()
        );
        clock.advance(Duration::from_secs(2));
        assert_eq!(None, broker.handle_broadcast_recorded(&msg("s", true)).unwrap());
        assert_eq!(
            Some(Recorded { offset: 2, timestamp: 1_700_000_002 }),
            broker.handle_broadcast_recorded(&msg("s", false)).unwrap()
        );

        // Fan-out and storage are exactly handle_broadcast's.
        assert_eq!(*lock(&rec.0), ["broadcast s 1 e1", "broadcast s 0 ", "broadcast s 2 e1"]);
        assert_eq!(2, broker.history_since("s", 0).unwrap().unwrap_or_default().len());
    }

    #[test]
    fn memory_concurrent_recorded_broadcasts_get_distinct_offsets() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        let offsets: Vec<u64> = std::thread::scope(|s| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    s.spawn(|| {
                        (0..50)
                            .map(|_| broker.handle_broadcast_recorded(&msg("s", false)).unwrap().unwrap().offset)
                            .collect::<Vec<u64>>()
                    })
                })
                .collect();
            workers.into_iter().flat_map(|w| w.join().unwrap()).collect()
        });

        let mut unique = offsets.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(offsets.len(), unique.len(), "every broadcast got its own offset");
        assert_eq!((1..=400).collect::<Vec<u64>>(), unique);
    }
```

- [ ] **Step 4: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests::memory_ 2>&1 | grep -E "^error" | head -3`
Expected: `error[E0422]: cannot find struct, variant or union type \`Recorded\` in this scope` and `error[E0599]: no method named \`handle_broadcast_recorded\` found for struct \`Memory\` in the current scope`.

- [ ] **Step 5: Implement**

In `crates/cabby-core/src/broker.rs`, after the `Entry` struct (line 274), add:

```rust
/// Where [`Memory::handle_broadcast_recorded`] stored a message: the offset and
/// timestamp assigned in the same critical section, so concurrent broadcasts
/// can't be mixed up (a later `peak()` could return another message's offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recorded {
    pub offset: u64,
    /// Unix seconds, as [`Entry::timestamp`].
    pub timestamp: i64,
}
```

Replace `Memstream::add` (lines 308-315) with:

```rust
    pub fn add(&self, data: &str) -> u64 {
        self.add_recorded(data).offset
    }

    /// [`Memstream::add`], returning the assigned offset and timestamp.
    fn add_recorded(&self, data: &str) -> Recorded {
        let now = self.clock.now();
        let mut st = write(&self.state);
        st.offset += 1;
        let recorded = Recorded { offset: st.offset, timestamp: unix_secs(now) };
        let entry = Entry { offset: recorded.offset, timestamp: recorded.timestamp, data: data.to_owned() };
        self.append(&mut st, entry, now);
        recorded
    }
```

In `impl Memory`, after `pub fn expire(&self)` (line 538), add:

```rust
    /// [`Broker::handle_broadcast`], returning where a stored message landed
    /// (`None` for a transient one, which is only fanned out).
    pub fn handle_broadcast_recorded(&self, msg: &StreamMessage) -> Result<Option<Recorded>, BrokerError> {
        if msg.meta.as_ref().is_some_and(|m| m.transient) {
            if let Some(b) = &self.inner.broadcaster {
                b.broadcast(msg);
            }
            return Ok(None);
        }
        let recorded = self.inner.stream_or_create(&msg.stream).add_recorded(&msg.data);
        let msg = StreamMessage { epoch: self.epoch(), offset: recorded.offset, ..msg.clone() };
        if let Some(b) = &self.inner.broadcaster {
            b.broadcast(&msg);
        }
        Ok(Some(recorded))
    }
```

Replace the body of `fn handle_broadcast` in `impl Broker for Memory` (lines 674-687) with:

```rust
    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        self.handle_broadcast_recorded(msg).map(drop)
    }
```

- [ ] **Step 6: Run the broker tests and the translated memory-broker tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests 2>&1 | grep "^test result"`
Expected: `test result: ok. 11 passed; …`

Run (the translated `broker` package against Rust; no services needed for the memory cases):
```sh
mise exec -- cargo xtask test --backend rust memory_test 2>&1 | grep -E "^backend|^  tests"
```
Expected: every test `pass`, no `fail` (`memory` behaviour unchanged).

- [ ] **Step 7: Lint and commit**

```sh
mise exec -- cargo fmt --all
mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker.rs
git commit -m "broker: Memory::handle_broadcast_recorded

Returns the offset and timestamp assigned under the stream lock (for the SQLite
broker's write-behind queue); handle_broadcast delegates to it. The test clock
moves to broker::test_clock for reuse.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected clippy tail: `Finished …` with no warnings.

---

### Task 4: `Memory::restore_stream`

Installs a stream's last offset, its deadline (unix seconds, as `MemstreamState::deadline`) and its contiguous tail. It refuses anything else, because `Memstream::filter_by_offset` (`broker.rs:358-377`) computes indexes as `offset - low`, which assumes contiguous offsets, and the stream invariant is "no entries, or the newest entry's offset equals the stream offset".

**Files:**
- Modify: `crates/cabby-core/src/broker.rs` (`impl Memstream`; `impl Memory`; `mod tests`)

- [ ] **Step 1: Write the failing tests**

Add to `mod tests`:

```rust
    #[test]
    fn memory_restore_stream_installs_offset_and_entries() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        let epoch = broker.epoch();
        let entries = vec![
            Entry { offset: 7, timestamp: 1_700_000_000, data: "g".into() },
            Entry { offset: 8, timestamp: 1_700_000_000, data: "h".into() },
        ];
        broker.restore_stream("s", 8, 1_700_003_000, entries).unwrap();

        let stored: Vec<(u64, String)> =
            broker.history_since("s", 0).unwrap().unwrap_or_default().into_iter().map(|m| (m.offset, m.data)).collect();
        assert_eq!(vec![(7, "g".to_owned()), (8, "h".to_owned())], stored);
        // A client that saw offset 7 gets 8 (filter_by_offset needs the offset within the stored range).
        let after: Vec<u64> = broker.history_from("s", &epoch, 7).unwrap().into_iter().map(|m| m.offset).collect();
        assert_eq!(vec![8], after);
        assert_eq!(Some(8), broker.peak("s").unwrap().map(|m| m.offset));
        assert_eq!(9, broker.add("s", "i"), "the next broadcast continues after the restored offset");
    }

    #[test]
    fn memory_restored_empty_stream_keeps_its_offset_until_its_deadline() {
        let clock = TestClock::new();
        let broker = Memory::new(None, None, &Config::new(), clock.clone());
        broker.restore_stream("s", 5, 1_700_000_010, Vec::new()).unwrap();

        broker.expire();
        assert_eq!(Some(vec![]), broker.history_since("s", 0).unwrap(), "kept before its deadline");
        assert_eq!(None, broker.peak("s").unwrap(), "no entries");

        clock.advance(Duration::from_secs(11));
        broker.expire();
        assert_eq!(None, broker.history_since("s", 0).unwrap(), "dropped after its deadline");
        assert_eq!(1, broker.add("s", "x"));
    }

    #[test]
    fn memory_restored_empty_stream_continues_after_its_offset() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        broker.restore_stream("s", 5, 1_700_000_010, Vec::new()).unwrap();
        assert_eq!(6, broker.add("s", "x"));
    }

    #[test]
    fn memory_restore_stream_rejects_gaps_and_unanchored_tails() {
        let broker = Memory::new(None, None, &Config::new(), TestClock::new());
        let e = |offset| Entry { offset, timestamp: 1_700_000_000, data: String::new() };

        assert!(broker.restore_stream("gap", 3, 0, vec![e(1), e(3)]).is_err());
        assert!(broker.restore_stream("unanchored", 4, 0, vec![e(2), e(3)]).is_err());
        assert!(broker.restore_stream("ok", 3, 0, vec![e(2), e(3)]).is_ok());
        assert_eq!(None, broker.history_since("gap", 0).unwrap(), "a refused stream is not created");
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests::memory_restore 2>&1 | grep -E "^error" | head -2`
Expected: `error[E0599]: no method named \`restore_stream\` found for struct \`Memory\` in the current scope`

- [ ] **Step 3: Implement**

In `impl Memstream`, after `fn deadline(&self) -> i64 { … }` (line 337), add:

```rust
    /// Installs restored state: the last assigned offset, the deadline (unix
    /// seconds) and the newest entries in ascending offsets, at most `limit`.
    fn restore(&self, offset: u64, deadline: i64, entries: Vec<Entry>) {
        let mut st = write(&self.state);
        st.offset = offset;
        st.deadline = deadline;
        st.data = VecDeque::from(entries);
        while st.data.len() > self.limit {
            st.data.pop_front();
        }
    }
```

In `impl Memory`, after `handle_broadcast_recorded`, add:

```rust
    /// Installs a stream loaded from storage: `max_offset` is its last assigned
    /// offset, `deadline` (unix seconds) when it may be dropped, and `entries`
    /// its newest entries, in ascending and contiguous offsets ending at
    /// `max_offset` (or none).
    pub fn restore_stream(
        &self,
        name: &str,
        max_offset: u64,
        deadline: i64,
        entries: Vec<Entry>,
    ) -> Result<(), BrokerError> {
        let contiguous = entries.windows(2).all(|w| w[1].offset == w[0].offset + 1);
        let anchored = entries.last().is_none_or(|e| e.offset == max_offset);
        if !contiguous || !anchored {
            return Err(BrokerError {
                message: format!("restored entries of {name} must be contiguous and end at offset {max_offset}"),
            });
        }
        self.inner.stream_or_create(name).restore(max_offset, deadline, entries);
        Ok(())
    }
```

- [ ] **Step 4: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests 2>&1 | grep "^test result"`
Expected: `test result: ok. 15 passed; …`

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker.rs
git commit -m "broker: Memory::restore_stream

Installs a loaded stream's offset, deadline and contiguous tail; refuses gaps and
tails that don't end at the stream offset (filter_by_offset assumes contiguity).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `Memory` session helpers

`restore_session_with_deadline` keeps a loaded session's original expiry (`commit_session` would reset it to `now + sessions_ttl` on every restart). `commit_session_data` and `touch_session_deadline` return the expiry they set (unix milliseconds, the priority of `SessionsState::expiration`), so the SQLite broker persists exactly that value. `commit_session` and `touch_session` (`broker.rs:735-763`) delegate to them.

**Files:**
- Modify: `crates/cabby-core/src/broker.rs` (`impl MemoryInner`, lines 541-630; `impl Memory`; `Broker for Memory::commit_session`/`touch_session`; `mod tests`)

- [ ] **Step 1: Write the failing tests**

Add to `mod tests`:

```rust
    #[test]
    fn memory_restored_session_keeps_its_original_expiry() {
        let clock = TestClock::new();
        let broker = Memory::new(None, None, &Config::new(), clock.clone());
        // Expires 2 s from now, not sessions_ttl (300 s) from now.
        broker.restore_session_with_deadline("sid", b"state".to_vec(), 1_700_000_002_000);
        assert_eq!(Some(b"state".to_vec()), broker.restore_session("sid").unwrap());

        clock.advance(Duration::from_millis(1_999));
        broker.expire();
        assert!(broker.restore_session("sid").unwrap().is_some(), "still cached 1 ms before its expiry");

        clock.advance(Duration::from_millis(1));
        broker.expire();
        assert_eq!(None, broker.restore_session("sid").unwrap(), "expired at its original deadline");
    }

    #[test]
    fn memory_session_commit_and_touch_report_their_expiry() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.sessions_ttl = 10;
        let broker = Memory::new(None, None, &config, clock.clone());

        assert_eq!(None, broker.touch_session_deadline("sid"), "nothing cached yet");
        assert_eq!(1_700_000_010_000, broker.commit_session_data("sid", b"a".to_vec()));
        clock.advance(Duration::from_secs(3));
        assert_eq!(Some(1_700_000_013_000), broker.touch_session_deadline("sid"));
        assert_eq!(Some(b"a".to_vec()), broker.restore_session("sid").unwrap());
    }

    #[test]
    fn memory_commit_and_touch_session_still_expire_after_the_ttl() {
        struct State;
        impl Cacheable for State {
            fn to_cache_entry(&self) -> Result<Vec<u8>, BrokerError> {
                Ok(b"state".to_vec())
            }
        }
        let clock = TestClock::new();
        let mut config = Config::new();
        config.sessions_ttl = 1;
        let broker = Memory::new(None, None, &config, clock.clone());

        broker.commit_session("sid", &State).unwrap();
        clock.advance(Duration::from_millis(600));
        broker.touch_session("sid").unwrap();
        clock.advance(Duration::from_millis(600));
        broker.expire();
        assert!(broker.restore_session("sid").unwrap().is_some(), "the touch moved the expiry");
        clock.advance(Duration::from_millis(400));
        broker.expire();
        assert_eq!(None, broker.restore_session("sid").unwrap());
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests::memory_ 2>&1 | grep -E "^error" | head -3`
Expected: `error[E0599]: no method named \`restore_session_with_deadline\` found …`, and the same for `commit_session_data` and `touch_session_deadline`.

- [ ] **Step 3: Implement**

In `impl MemoryInner`, after `fn now_millis(&self) -> i64 { … }` (line 560), add:

```rust
    /// When a session committed or touched now expires (unix milliseconds).
    fn session_deadline(&self) -> i64 {
        self.now_millis() + self.config.sessions_ttl * 1000
    }

    /// Caches `data` for `sid` until `deadline` (unix milliseconds).
    fn cache_session(&self, sid: &str, data: Vec<u8>, deadline: i64) {
        let mut guard = lock(&self.sessions);
        let st = &mut *guard;
        let expiration = match st.entries.get(sid) {
            Some(entry) => {
                st.expiration.update(entry.expiration, deadline);
                entry.expiration
            }
            None => st.expiration.push_item(sid.to_owned(), deadline),
        };
        st.entries.insert(sid.to_owned(), SessionEntry { data, expiration });
    }
```

In `impl Memory`, after `restore_stream`, add:

```rust
    /// Caches session state with an explicit expiry (unix milliseconds), as
    /// loaded from storage.
    pub fn restore_session_with_deadline(&self, sid: &str, data: Vec<u8>, expires_at_ms: i64) {
        self.inner.cache_session(sid, data, expires_at_ms);
    }

    /// [`Broker::commit_session`] for serialized state; returns the expiry it
    /// set (unix milliseconds).
    pub fn commit_session_data(&self, sid: &str, data: Vec<u8>) -> i64 {
        let deadline = self.inner.session_deadline();
        self.inner.cache_session(sid, data, deadline);
        deadline
    }

    /// [`Broker::touch_session`]; returns the new expiry (unix milliseconds), or
    /// `None` when nothing is cached for `sid`.
    pub fn touch_session_deadline(&self, sid: &str) -> Option<i64> {
        let deadline = self.inner.session_deadline();
        let mut guard = lock(&self.inner.sessions);
        let st = &mut *guard;
        let entry = st.entries.get(sid)?;
        st.expiration.update(entry.expiration, deadline);
        Some(deadline)
    }
```

In `impl Broker for Memory`, replace `fn commit_session` and `fn touch_session` (lines 735-749 and 755-763) with:

```rust
    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let data = session.to_cache_entry()?;
        self.commit_session_data(sid, data);
        Ok(())
    }
```

and

```rust
    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        self.touch_session_deadline(sid);
        Ok(())
    }
```

- [ ] **Step 4: Run the tests and the translated session tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::tests 2>&1 | grep "^test result"`
Expected: `test result: ok. 18 passed; …`

Run: `mise exec -- cargo xtask test --backend rust restore_session 2>&1 | grep -E "^backend|^  tests"` and `mise exec -- cargo xtask test --backend rust memory_test 2>&1 | grep -E "^  tests"`
Expected: only `pass` counts.

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker.rs
git commit -m "broker: Memory session helpers for the SQLite broker

restore_session_with_deadline keeps a loaded session's original expiry;
commit_session_data and touch_session_deadline return the expiry they set, and
commit_session/touch_session delegate to them (memory behaviour unchanged).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The `sqlite` module skeleton, test helpers and `units`

The module lives under `broker` (like `broker/nats.rs`), so it can use the parent's private helpers (`lock`, `nanoid`, `unix_secs`, `unix_millis`), as `nats.rs` does (`use super::{…, lock, nanoid, read, write}`). Until Task 14 wires it up, nothing outside the module uses it and some helpers are only used by later tasks' tests, so its `mod` line allows dead code; Task 14 removes that attribute.

**Files:**
- Modify: `crates/cabby-core/src/broker.rs` (after `pub use nats::{JETSTREAM_READY_TIMEOUT, Nats};`, line 896)
- Create: `crates/cabby-core/src/broker/sqlite.rs`
- Create: `crates/cabby-core/src/broker/sqlite/units.rs`

- [ ] **Step 1: Declare the module**

In `crates/cabby-core/src/broker.rs`, after `pub use nats::{JETSTREAM_READY_TIMEOUT, Nats};` (line 896), add:

```rust

// Wired up in Task 14 of docs/superpowers/plans/2026-11-23-sqlite-broker.md;
// until then only its tests use it.
#[allow(dead_code)]
mod sqlite;
```

- [ ] **Step 2: Create the module root**

Create `crates/cabby-core/src/broker/sqlite.rs`:

```rust
//! The SQLite broker: a Cabby addition (deviation D-065; design in
//! `docs/superpowers/specs/2026-11-23-sqlite-broker-design.md`).
//!
//! The memory broker stays the live path: it serves every read, assigns
//! offsets and fans out. Beside it, a bounded write queue (`queue`) feeds one
//! persister thread (`persister`) that writes batches to a WAL-mode SQLite
//! file (`store`); at startup the loader (`loader`) fills the memory broker
//! from the file before Cabby accepts connections. A graceful shutdown writes
//! the last batch and marks the file clean, so the next start keeps the
//! epoch; after a crash the next start rotates it.
//!
//! Units: history timestamps and stream deadlines are unix seconds, session
//! expiries unix milliseconds (`units`).

use std::sync::OnceLock;

use crate::logger::Logger;

mod units;

/// The broker's logger (`context=broker provider=sqlite`).
fn log() -> &'static Logger {
    static LOG: OnceLock<Logger> = OnceLock::new();
    LOG.get_or_init(|| Logger::default().with("context", "broker").with("provider", "sqlite"))
}

/// Helpers shared by the module's tests.
#[cfg(test)]
pub(crate) mod testing {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A fresh directory under the system temp dir, removed on drop.
    pub(crate) struct TempDir(PathBuf);

    impl TempDir {
        pub(crate) fn new() -> Self {
            static SEQ: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "cabby-sqlite-test-{}-{}",
                std::process::id(),
                SEQ.fetch_add(1, Ordering::SeqCst)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("creating a temp dir");
            Self(dir)
        }

        pub(crate) fn path(&self) -> &Path {
            &self.0
        }

        /// A file directly in the directory.
        pub(crate) fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }

        /// A database path whose parent directory does not exist yet.
        pub(crate) fn db(&self) -> PathBuf {
            self.0.join("data").join("cabby.db")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
```

- [ ] **Step 3: Write the units module and its tests**

Create `crates/cabby-core/src/broker/sqlite/units.rs`:

```rust
//! Every unit conversion of the SQLite broker (spec §4). History timestamps
//! and stream deadlines are unix seconds, as `Entry::timestamp` and the memory
//! broker's stream deadline; session expiries are unix milliseconds, as the
//! memory broker's session expiration queue. Offsets are `u64` in memory and
//! `INTEGER` (`i64`) in SQLite.

use std::time::SystemTime;

use crate::broker::{Clock, unix_millis, unix_secs};

/// Unix seconds of `t`.
pub(super) fn secs(t: SystemTime) -> i64 {
    unix_secs(t)
}

/// Unix milliseconds of `t`.
pub(super) fn millis(t: SystemTime) -> i64 {
    unix_millis(t)
}

pub(super) fn now_secs(clock: &dyn Clock) -> i64 {
    secs(clock.now())
}

pub(super) fn now_millis(clock: &dyn Clock) -> i64 {
    millis(clock.now())
}

/// When a stream last written at `ts_secs` may be dropped: ten history TTLs
/// later, as `Memstream::append` computes it.
pub(super) fn stream_deadline(ts_secs: i64, history_ttl: i64) -> i64 {
    ts_secs.saturating_add(history_ttl.saturating_mul(10))
}

/// The oldest timestamp still within `history_ttl` at `now_secs`: older
/// entries are expired, as `Memstream::expire` drops them.
pub(super) fn history_cutoff(now_secs: i64, history_ttl: i64) -> i64 {
    now_secs.saturating_sub(history_ttl)
}

/// An offset as an SQLite integer (offsets beyond `i64::MAX` don't occur).
pub(super) fn offset_to_sql(offset: u64) -> i64 {
    i64::try_from(offset).unwrap_or(i64::MAX)
}

/// A stored offset; `None` for a negative (damaged) value.
pub(super) fn offset_from_sql(seq: i64) -> Option<u64> {
    u64::try_from(seq).ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::broker::test_clock::{T0, TestClock};
    use crate::broker::{Broker, Config, Memory};

    #[test]
    fn clock_readings_are_unix_seconds_and_milliseconds() {
        let clock = TestClock::new();
        clock.advance(Duration::from_millis(1_500));
        assert_eq!(T0 + 1, now_secs(clock.as_ref()));
        assert_eq!(T0 * 1000 + 1_500, now_millis(clock.as_ref()));
    }

    #[test]
    fn stream_deadline_matches_the_memory_broker() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.history_ttl = 2;
        let memory = Memory::new(None, None, &config, clock.clone());
        memory.add("s", "a");

        let deadline = stream_deadline(T0, config.history_ttl);
        clock.advance(Duration::from_secs((deadline - T0).unsigned_abs()));
        memory.expire();
        assert!(memory.history_since("s", 0).unwrap().is_some(), "kept at its deadline");

        clock.advance(Duration::from_secs(1));
        memory.expire();
        assert_eq!(None, memory.history_since("s", 0).unwrap(), "dropped after it");
    }

    #[test]
    fn history_cutoff_matches_the_memory_broker() {
        let clock = TestClock::new();
        let mut config = Config::new();
        config.history_ttl = 10;
        let memory = Memory::new(None, None, &config, clock.clone());
        memory.add("s", "a");

        clock.advance(Duration::from_secs(10));
        memory.expire();
        assert_eq!(T0, history_cutoff(T0 + 10, 10));
        assert_eq!(1, memory.history_since("s", 0).unwrap().unwrap_or_default().len(), "ts == cutoff is kept");

        clock.advance(Duration::from_secs(1));
        memory.expire();
        assert!(T0 < history_cutoff(T0 + 11, 10));
        assert_eq!(0, memory.history_since("s", 0).unwrap().unwrap_or_default().len(), "ts < cutoff expires");
    }

    #[test]
    fn offsets_convert_to_sql_integers() {
        assert_eq!(5, offset_to_sql(5));
        assert_eq!(i64::MAX, offset_to_sql(u64::MAX));
        assert_eq!(Some(5), offset_from_sql(5));
        assert_eq!(None, offset_from_sql(-1));
    }
}
```

- [ ] **Step 4: Run the tests**

These tests pin the conversions against the memory broker, which already exists, so they pass once the module compiles.

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::units 2>&1 | grep "^test result"`
Expected: `test result: ok. 4 passed; …`

To see the pin work, temporarily change `saturating_mul(10)` to `saturating_mul(9)` in `stream_deadline`, rerun: `stream_deadline_matches_the_memory_broker` fails with `dropped after it`. Revert.

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker.rs crates/cabby-core/src/broker/sqlite.rs crates/cabby-core/src/broker/sqlite/units.rs
git commit -m "broker::sqlite: module skeleton and unit conversions

History timestamps and stream deadlines in unix seconds, session expiries in unix
milliseconds, offsets as SQLite integers; pinned against the memory broker.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: `store`: opening, pragmas, schema, migrations, `meta`, corruption classification

**Files:**
- Create: `crates/cabby-core/src/broker/sqlite/store.rs`
- Modify: `crates/cabby-core/src/broker/sqlite.rs` (`mod store;`)

- [ ] **Step 1: Write the module and its tests**

Add `mod store;` below `mod units;` in `crates/cabby-core/src/broker/sqlite.rs` (keep the `mod` lines sorted: `store`, then `units`).

Create `crates/cabby-core/src/broker/sqlite/store.rs`:

```rust
//! The database file (spec §4): connection settings, schema and migrations,
//! and the `meta` table.

use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension};

/// How long a statement waits for a lock another connection holds.
pub(super) const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Schema migrations: entry `i` takes a file from version `i` to `i + 1`, so
/// the current version is `MIGRATIONS.len()`. Append new versions; never edit
/// a released one.
pub(super) const MIGRATIONS: &[&str] = &[SCHEMA_V1];

/// `meta` holds `schema_version`, `epoch` and `clean_shutdown`. It exists in
/// every version, so the version can be read before migrating.
const CREATE_META: &str = "CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID";

/// Version 1: the spec §4 schema, plus `streams_deadline` so pruning streams
/// by deadline uses an index.
const SCHEMA_V1: &str = "
-- One row per stream ever written, so a quiet stream keeps its offsets after
-- its entries expire (the memory broker keeps a stream for 10 x history_ttl).
CREATE TABLE streams (
  stream   TEXT PRIMARY KEY,
  max_seq  INTEGER NOT NULL,
  deadline INTEGER NOT NULL   -- unix seconds
) WITHOUT ROWID;
CREATE INDEX streams_deadline ON streams (deadline);

-- Rowid tables: payloads can be large.
CREATE TABLE history (
  stream TEXT    NOT NULL,
  seq    INTEGER NOT NULL,  -- the stream offset ('offset' is an SQL keyword)
  ts     INTEGER NOT NULL,  -- unix seconds
  data   TEXT    NOT NULL
);
CREATE UNIQUE INDEX history_stream_seq ON history (stream, seq);
CREATE INDEX history_ts ON history (ts);

CREATE TABLE sessions (
  sid           TEXT    PRIMARY KEY,
  data          BLOB    NOT NULL,  -- the Cacheable bytes commit_session receives
  expires_at_ms INTEGER NOT NULL   -- unix milliseconds
);
CREATE INDEX sessions_expires ON sessions (expires_at_ms);
";

/// A database failure, classified as spec §6 needs it.
#[derive(Debug, thiserror::Error)]
pub(super) enum StoreError {
    /// `SQLITE_CORRUPT`, `SQLITE_NOTADB` or a failed `quick_check`: the only
    /// errors for which the file is moved aside.
    #[error("corrupt: {0}")]
    Corrupt(String),
    /// Written by a newer Cabby: refused, never moved aside.
    #[error("schema version {found} is newer than this Cabby supports ({supported}); upgrade Cabby or move the file away")]
    NewerSchema { found: i64, supported: i64 },
    /// Any other SQLite error (busy, permissions, I/O, disk full…).
    #[error("{0}")]
    Sqlite(rusqlite::Error),
    #[error("{0}")]
    Other(String),
}

impl From<rusqlite::Error> for StoreError {
    fn from(e: rusqlite::Error) -> Self {
        match e.sqlite_error_code() {
            Some(ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase) => StoreError::Corrupt(e.to_string()),
            _ => StoreError::Sqlite(e),
        }
    }
}

/// Opens (creating if needed) the database: WAL, `synchronous=NORMAL`, a busy
/// timeout, `quick_check`, then migrations up to the current version.
pub(super) fn open(path: &Path) -> Result<Connection, StoreError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags)?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let mode: String = conn.pragma_update_and_check(None, "journal_mode", "WAL", |r| r.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::Other(format!("journal_mode is {mode}, the SQLite broker needs WAL")));
    }
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    quick_check(&conn)?;
    migrate(&conn, MIGRATIONS)?;
    Ok(conn)
}

/// `PRAGMA quick_check`: anything but a single `ok` row is corruption.
pub(super) fn quick_check(conn: &Connection) -> Result<(), StoreError> {
    let mut stmt = conn.prepare("PRAGMA quick_check")?;
    let rows: Vec<String> = stmt.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
    if rows == ["ok"] {
        Ok(())
    } else {
        Err(StoreError::Corrupt(format!("quick_check: {}", rows.join("; "))))
    }
}

/// The stored schema version, 0 for a fresh file.
pub(super) fn schema_version(conn: &Connection) -> Result<i64, StoreError> {
    match meta_get(conn, "schema_version")? {
        None => Ok(0),
        Some(v) => v
            .parse::<i64>()
            .ok()
            .filter(|v| *v >= 0)
            .ok_or_else(|| StoreError::Corrupt(format!("invalid schema_version {v:?}"))),
    }
}

/// Brings the file up to `migrations.len()`, one transaction per version;
/// refuses a file whose version is newer.
pub(super) fn migrate(conn: &Connection, migrations: &[&str]) -> Result<(), StoreError> {
    conn.execute_batch(CREATE_META)?;
    let supported = i64::try_from(migrations.len()).unwrap_or(i64::MAX);
    let found = schema_version(conn)?;
    if found > supported {
        return Err(StoreError::NewerSchema { found, supported });
    }
    for (version, sql) in (1_i64..).zip(migrations).skip(usize::try_from(found).unwrap_or(0)) {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(sql)?;
        meta_set(&tx, "schema_version", &version.to_string())?;
        tx.commit()?;
    }
    Ok(())
}

pub(super) fn meta_get(conn: &Connection, key: &str) -> Result<Option<String>, StoreError> {
    Ok(conn.query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0)).optional()?)
}

pub(super) fn meta_set(conn: &Connection, key: &str, value: &str) -> Result<(), StoreError> {
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        [key, value],
    )?;
    Ok(())
}

/// Records the run's epoch and that it has not shut down cleanly yet, in one
/// transaction, before Cabby accepts connections (spec §5 step 3).
pub(super) fn mark_running(conn: &mut Connection, epoch: &str) -> Result<(), StoreError> {
    let tx = conn.transaction()?;
    meta_set(&tx, "epoch", epoch)?;
    meta_set(&tx, "clean_shutdown", "0")?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::sqlite::testing::TempDir;

    fn schema_objects(conn: &Connection) -> Vec<String> {
        let sql = "SELECT name FROM sqlite_schema WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%' ORDER BY name";
        conn.prepare(sql).unwrap().query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    }

    #[test]
    fn a_fresh_file_gets_the_current_schema_in_wal_mode() {
        let dir = TempDir::new();
        let conn = open(&dir.file("cabby.db")).unwrap();

        assert_eq!(1, schema_version(&conn).unwrap());
        assert_eq!(
            vec![
                "history",
                "history_stream_seq",
                "history_ts",
                "meta",
                "sessions",
                "sessions_expires",
                "streams",
                "streams_deadline"
            ],
            schema_objects(&conn)
        );
        let mode: String = conn.pragma_query_value(None, "journal_mode", |r| r.get(0)).unwrap();
        assert_eq!("wal", mode);
        let synchronous: i64 = conn.pragma_query_value(None, "synchronous", |r| r.get(0)).unwrap();
        assert_eq!(1, synchronous, "synchronous=NORMAL");
    }

    #[test]
    fn reopening_keeps_the_data_and_the_version() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        meta_set(&open(&path).unwrap(), "epoch", "abcd").unwrap();

        let conn = open(&path).unwrap();
        assert_eq!(Some("abcd".to_owned()), meta_get(&conn, "epoch").unwrap());
        assert_eq!(1, schema_version(&conn).unwrap());
    }

    #[test]
    fn migrations_run_forward_from_the_stored_version() {
        let dir = TempDir::new();
        let conn = open(&dir.file("cabby.db")).unwrap();
        conn.execute("INSERT INTO streams (stream, max_seq, deadline) VALUES ('s', 3, 10)", []).unwrap();

        migrate(&conn, &[SCHEMA_V1, "CREATE TABLE extra (x INTEGER)"]).unwrap();

        assert_eq!(2, schema_version(&conn).unwrap());
        assert!(schema_objects(&conn).contains(&"extra".to_owned()));
        let max_seq: i64 = conn.query_row("SELECT max_seq FROM streams WHERE stream = 's'", [], |r| r.get(0)).unwrap();
        assert_eq!(3, max_seq, "rows survive a migration");
    }

    #[test]
    fn a_newer_schema_is_refused_and_not_treated_as_corruption() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        meta_set(&open(&path).unwrap(), "schema_version", "99").unwrap();

        match open(&path) {
            Err(StoreError::NewerSchema { found: 99, supported: 1 }) => {}
            other => panic!("expected NewerSchema, got {other:?}"),
        }
    }

    #[test]
    fn a_file_that_is_not_a_database_is_corrupt() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        std::fs::write(&path, vec![0x42_u8; 8192]).unwrap();

        assert!(matches!(open(&path), Err(StoreError::Corrupt(_))));
    }

    #[test]
    fn a_damaged_page_is_corrupt() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        {
            let conn = open(&path).unwrap();
            for i in 0..200 {
                conn.execute("INSERT INTO history (stream, seq, ts, data) VALUES ('s', ?1, 0, ?2)", (i, "x".repeat(100)))
                    .unwrap();
            }
        } // The last connection closes: the WAL is checkpointed into the file and removed.
        let mut bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() > 3 * 4096, "several pages");
        bytes[4096..3 * 4096].fill(0xFF);
        std::fs::write(&path, bytes).unwrap();

        assert!(matches!(open(&path), Err(StoreError::Corrupt(_))));
    }

    #[test]
    fn only_corruption_codes_count_as_corrupt() {
        use rusqlite::ffi;
        let classify = |code| StoreError::from(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None));

        assert!(matches!(classify(ffi::SQLITE_CORRUPT), StoreError::Corrupt(_)));
        assert!(matches!(classify(ffi::SQLITE_NOTADB), StoreError::Corrupt(_)));
        for code in [ffi::SQLITE_BUSY, ffi::SQLITE_PERM, ffi::SQLITE_CANTOPEN, ffi::SQLITE_READONLY, ffi::SQLITE_FULL] {
            assert!(matches!(classify(code), StoreError::Sqlite(_)), "code {code} is not corruption");
        }
    }

    #[test]
    fn mark_running_records_the_epoch_and_an_unclean_state() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        meta_set(&conn, "clean_shutdown", "1").unwrap();

        mark_running(&mut conn, "e1").unwrap();

        assert_eq!(Some("e1".to_owned()), meta_get(&conn, "epoch").unwrap());
        assert_eq!(Some("0".to_owned()), meta_get(&conn, "clean_shutdown").unwrap());
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::store 2>&1 | grep -E "^test |^test result"`
Expected: 8 tests, `test result: ok. 8 passed; …`.

To see the corruption classification guard work, temporarily remove `| ErrorCode::NotADatabase` from `From<rusqlite::Error>`: `a_file_that_is_not_a_database_is_corrupt` and `only_corruption_codes_count_as_corrupt` fail. Revert.

- [ ] **Step 3: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite.rs crates/cabby-core/src/broker/sqlite/store.rs
git commit -m "broker::sqlite: open, schema v1, migrations and corruption classification

WAL, synchronous=NORMAL, a 5 s busy timeout and quick_check on open; versioned
migrations in meta.schema_version, newer versions refused; only SQLITE_CORRUPT,
SQLITE_NOTADB and a failed quick_check count as corruption.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: `queue`: the bounded write queue

`std::sync::mpsc::sync_channel` can't drop its oldest item (§3), so the queue is a `Mutex` plus a `Condvar`. Each kind of op waits in its own lane, tagged with a global sequence number: dropping the oldest op of a kind is O(1), and batches still come out in enqueue order (a session commit followed by a touch must be applied in that order).

**Files:**
- Create: `crates/cabby-core/src/broker/sqlite/queue.rs`
- Modify: `crates/cabby-core/src/broker/sqlite.rs` (`mod queue;`, test helpers)

- [ ] **Step 1: Add the op helpers to the test module**

In `crates/cabby-core/src/broker/sqlite.rs`, add `mod queue;` above `mod store;`, and inside `pub(crate) mod testing { … }` add, below the `use` lines:

```rust
    use super::queue::WriteOp;
    use crate::broker::test_clock::T0;

    /// A history op at the test clock's start.
    pub(crate) fn history(stream: &str, offset: u64) -> WriteOp {
        history_at(stream, offset, T0)
    }

    pub(crate) fn history_at(stream: &str, offset: u64, ts_secs: i64) -> WriteOp {
        WriteOp::History { stream: stream.into(), offset, ts_secs, data: format!("m{offset}") }
    }

    /// A session commit whose data is the session ID's bytes.
    pub(crate) fn commit(sid: &str, expires_at_ms: i64) -> WriteOp {
        WriteOp::SessionCommit { sid: sid.into(), data: sid.as_bytes().to_vec(), expires_at_ms }
    }

    pub(crate) fn touch(sid: &str, expires_at_ms: i64) -> WriteOp {
        WriteOp::SessionTouch { sid: sid.into(), expires_at_ms }
    }
```

- [ ] **Step 2: Write the queue with failing tests**

Create `crates/cabby-core/src/broker/sqlite/queue.rs`. Write the types, the tests, and `todo!()` bodies first:

```rust
//! The bounded write queue between the broker and the persister thread (spec §3).
//!
//! Enqueueing never blocks the broadcast path. When the queue would hold more
//! than `capacity` ops, it drops the oldest history op; only if none is
//! queued, the oldest session touch, then the oldest session commit. The
//! incoming op counts as queued, so a history op arriving at a queue full of
//! session ops is itself the one dropped. `push` returns the kind it dropped,
//! so the broker counts every drop (spec §7).

use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

use crate::broker::lock;

/// One write for the database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteOp {
    /// A stored broadcast: `ts_secs` is its unix-seconds timestamp.
    History { stream: String, offset: u64, ts_secs: i64, data: String },
    /// Session state with its absolute expiry (unix milliseconds).
    SessionCommit { sid: String, data: Vec<u8>, expires_at_ms: i64 },
    /// A new expiry (unix milliseconds) for a stored session.
    SessionTouch { sid: String, expires_at_ms: i64 },
}

/// The kind of a [`WriteOp`] (what an overflow dropped).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OpKind {
    History,
    SessionTouch,
    SessionCommit,
}

impl WriteOp {
    pub(crate) fn kind(&self) -> OpKind {
        match self {
            WriteOp::History { .. } => OpKind::History,
            WriteOp::SessionTouch { .. } => OpKind::SessionTouch,
            WriteOp::SessionCommit { .. } => OpKind::SessionCommit,
        }
    }
}

/// Which kind goes first when the queue is full.
const DROP_ORDER: [OpKind; 3] = [OpKind::History, OpKind::SessionTouch, OpKind::SessionCommit];

type Lane = VecDeque<(u64, WriteOp)>;

#[derive(Default)]
struct Lanes {
    history: Lane,
    touches: Lane,
    commits: Lane,
    next_seq: u64,
    /// Set by [`WriteQueue::close`]: the deadline of the final flush.
    closed: Option<Instant>,
}

impl Lanes {
    fn lane(&mut self, kind: OpKind) -> &mut Lane {
        match kind {
            OpKind::History => &mut self.history,
            OpKind::SessionTouch => &mut self.touches,
            OpKind::SessionCommit => &mut self.commits,
        }
    }

    fn len(&self) -> usize {
        self.history.len() + self.touches.len() + self.commits.len()
    }

    fn front_seq(&self, kind: OpKind) -> Option<u64> {
        let lane = match kind {
            OpKind::History => &self.history,
            OpKind::SessionTouch => &self.touches,
            OpKind::SessionCommit => &self.commits,
        };
        lane.front().map(|(seq, _)| *seq)
    }

    /// The oldest op across the lanes.
    fn pop_oldest(&mut self) -> Option<WriteOp> {
        let (_, kind) = DROP_ORDER.into_iter().filter_map(|kind| self.front_seq(kind).map(|seq| (seq, kind))).min()?;
        self.lane(kind).pop_front().map(|(_, op)| op)
    }

    fn take(&mut self, max: usize) -> Vec<WriteOp> {
        let mut out = Vec::with_capacity(max.min(self.len()));
        while out.len() < max {
            match self.pop_oldest() {
                Some(op) => out.push(op),
                None => break,
            }
        }
        out
    }
}

/// The queue; see the module docs.
pub(crate) struct WriteQueue {
    lanes: Mutex<Lanes>,
    ready: Condvar,
    capacity: usize,
    batch: usize,
}

impl WriteQueue {
    /// At most `capacity` ops; a waiting taker wakes once `batch` are queued.
    pub(crate) fn new(capacity: usize, batch: usize) -> Self {
        todo!()
    }

    /// Adds `op` without blocking; returns the kind of the op dropped to stay
    /// within capacity, if any. Ignored once closed.
    pub(crate) fn push(&self, op: WriteOp) -> Option<OpKind> {
        todo!()
    }

    pub(crate) fn len(&self) -> usize {
        todo!()
    }

    /// Waits up to `wait` for a full batch (or for `close`), then takes up to
    /// one batch, oldest first.
    pub(crate) fn take_batch(&self, wait: Duration) -> Vec<WriteOp> {
        todo!()
    }

    /// Takes everything queued, oldest first.
    pub(crate) fn take_all(&self) -> Vec<WriteOp> {
        todo!()
    }

    /// Waits up to `wait` unless closed: the pause before retrying a failed write.
    pub(crate) fn pause(&self, wait: Duration) {
        todo!()
    }

    /// Stops accepting ops and wakes the taker; `deadline` bounds the final
    /// flush. The first call's deadline wins.
    pub(crate) fn close(&self, deadline: Instant) {
        todo!()
    }

    pub(crate) fn closed(&self) -> Option<Instant> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::broker::sqlite::testing::{commit, history, touch};

    #[test]
    fn overflow_drops_history_then_touches_then_commits() {
        let q = WriteQueue::new(3, 1000);
        assert_eq!(None, q.push(history("s", 1)));
        assert_eq!(None, q.push(touch("a", 1)));
        assert_eq!(None, q.push(commit("b", 1)));

        assert_eq!(Some(OpKind::History), q.push(commit("c", 1)), "the history op goes first");
        assert_eq!(Some(OpKind::SessionTouch), q.push(commit("d", 1)), "then the touch");
        assert_eq!(Some(OpKind::SessionCommit), q.push(commit("e", 1)), "then the oldest commit");

        assert_eq!(3, q.len());
        assert_eq!(vec![commit("c", 1), commit("d", 1), commit("e", 1)], q.take_all());
    }

    #[test]
    fn a_history_op_arriving_at_a_queue_full_of_sessions_is_the_one_dropped() {
        let q = WriteQueue::new(2, 1000);
        q.push(commit("a", 1));
        q.push(touch("a", 2));

        assert_eq!(Some(OpKind::History), q.push(history("s", 1)));
        assert_eq!(vec![commit("a", 1), touch("a", 2)], q.take_all());
    }

    #[test]
    fn the_queue_never_exceeds_its_capacity_and_reports_every_drop() {
        let q = WriteQueue::new(100, 1000);
        let mut dropped = 0;
        for offset in 0..1_000 {
            if q.push(history("s", offset)) == Some(OpKind::History) {
                dropped += 1;
            }
            assert!(q.len() <= 100);
        }
        assert_eq!(100, q.len());
        assert_eq!(900, dropped);
        assert_eq!(history("s", 900), q.take_all()[0], "the newest are kept");
    }

    #[test]
    fn batches_keep_enqueue_order_across_kinds() {
        let q = WriteQueue::new(10, 1000);
        let ops = vec![commit("a", 1), history("s", 1), touch("a", 2), history("s", 2)];
        for op in &ops {
            q.push(op.clone());
        }
        assert_eq!(ops, q.take_all());
    }

    #[test]
    fn take_batch_returns_a_full_batch_at_once_and_a_partial_one_after_the_wait() {
        let q = WriteQueue::new(100, 3);
        for offset in 1..=5 {
            q.push(history("s", offset));
        }

        let started = Instant::now();
        assert_eq!(vec![history("s", 1), history("s", 2), history("s", 3)], q.take_batch(Duration::from_secs(10)));
        assert!(started.elapsed() < Duration::from_secs(5), "a full batch does not wait");

        let started = Instant::now();
        assert_eq!(2, q.take_batch(Duration::from_millis(50)).len());
        assert!(started.elapsed() >= Duration::from_millis(50), "a partial batch waits for the tick");
    }

    #[test]
    fn a_full_batch_wakes_the_waiting_taker() {
        let q = Arc::new(WriteQueue::new(100, 2));
        let taker = {
            let q = q.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                (q.take_batch(Duration::from_secs(10)), started.elapsed())
            })
        };
        q.push(history("s", 1));
        q.push(history("s", 2));

        let (batch, waited) = taker.join().unwrap();
        assert_eq!(2, batch.len());
        assert!(waited < Duration::from_secs(5), "woken by the full batch, not the timeout");
    }

    #[test]
    fn close_wakes_the_taker_and_later_pushes_are_ignored() {
        let q = Arc::new(WriteQueue::new(100, 1000));
        q.push(history("s", 1));
        let taker = {
            let q = q.clone();
            std::thread::spawn(move || q.take_batch(Duration::from_secs(10)))
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        q.close(deadline);
        q.close(deadline + Duration::from_secs(5));

        assert_eq!(1, taker.join().unwrap().len());
        assert_eq!(Some(deadline), q.closed(), "the first deadline wins");
        assert_eq!(None, q.push(history("s", 2)));
        assert_eq!(0, q.len());
    }

    #[test]
    fn pause_returns_early_when_closed() {
        let q = WriteQueue::new(100, 1000);
        q.close(Instant::now());
        let started = Instant::now();
        q.pause(Duration::from_secs(10));
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
```

- [ ] **Step 3: Run the tests to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::queue 2>&1 | grep -E "^test result|panicked" | head -3`
Expected: `test result: FAILED. 0 passed; 8 failed; …`, the panics reading `not yet implemented`.

- [ ] **Step 4: Implement**

Replace the `todo!()` bodies in `impl WriteQueue`:

```rust
    pub(crate) fn new(capacity: usize, batch: usize) -> Self {
        Self { lanes: Mutex::new(Lanes::default()), ready: Condvar::new(), capacity, batch: batch.max(1) }
    }

    pub(crate) fn push(&self, op: WriteOp) -> Option<OpKind> {
        let mut lanes = lock(&self.lanes);
        if lanes.closed.is_some() {
            return None;
        }
        let seq = lanes.next_seq;
        lanes.next_seq += 1;
        lanes.lane(op.kind()).push_back((seq, op));
        let mut dropped = None;
        if lanes.len() > self.capacity {
            for kind in DROP_ORDER {
                if lanes.lane(kind).pop_front().is_some() {
                    dropped = Some(kind);
                    break;
                }
            }
        }
        if lanes.len() >= self.batch {
            self.ready.notify_one();
        }
        dropped
    }

    pub(crate) fn len(&self) -> usize {
        lock(&self.lanes).len()
    }

    pub(crate) fn take_batch(&self, wait: Duration) -> Vec<WriteOp> {
        let lanes = lock(&self.lanes);
        let (mut lanes, _) = self
            .ready
            .wait_timeout_while(lanes, wait, |l| l.len() < self.batch && l.closed.is_none())
            .unwrap_or_else(PoisonError::into_inner);
        lanes.take(self.batch)
    }

    pub(crate) fn take_all(&self) -> Vec<WriteOp> {
        let mut lanes = lock(&self.lanes);
        let n = lanes.len();
        lanes.take(n)
    }

    pub(crate) fn pause(&self, wait: Duration) {
        let lanes = lock(&self.lanes);
        let _ = self.ready.wait_timeout_while(lanes, wait, |l| l.closed.is_none());
    }

    pub(crate) fn close(&self, deadline: Instant) {
        lock(&self.lanes).closed.get_or_insert(deadline);
        self.ready.notify_all();
    }

    pub(crate) fn closed(&self) -> Option<Instant> {
        lock(&self.lanes).closed
    }
```

- [ ] **Step 5: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::queue 2>&1 | grep "^test result"`
Expected: `test result: ok. 8 passed; …`

- [ ] **Step 6: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite.rs crates/cabby-core/src/broker/sqlite/queue.rs
git commit -m "broker::sqlite: bounded write queue

Mutex + Condvar with one lane per op kind: never blocks, never exceeds its capacity,
drops the oldest history op, then touch, then commit, reports every drop, and keeps
enqueue order across kinds.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: `store`: batch writes and the `streams` upsert

**Files:**
- Modify: `crates/cabby-core/src/broker/sqlite/store.rs`

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `store.rs` (and extend its `use` lines as shown):

```rust
    use std::collections::BTreeSet;

    use crate::broker::sqlite::testing::{commit, history, history_at, touch};
    use crate::broker::test_clock::T0;

    fn history_rows(conn: &Connection) -> Vec<(String, i64, i64, String)> {
        let sql = "SELECT stream, seq, ts, data FROM history ORDER BY stream, seq";
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn stream_rows(conn: &Connection) -> Vec<(String, i64, i64)> {
        let sql = "SELECT stream, max_seq, deadline FROM streams ORDER BY stream";
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn session_rows(conn: &Connection) -> Vec<(String, Vec<u8>, i64)> {
        let sql = "SELECT sid, data, expires_at_ms FROM sessions ORDER BY sid";
        conn.prepare(sql)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn a_batch_writes_history_sessions_and_its_streams() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let ops = vec![
            history("s", 1),
            history("s", 2),
            history_at("t", 1, T0 + 5),
            commit("a", 10),
            touch("a", 20),
            touch("missing", 30),
        ];

        let touched = write_batch(&mut conn, &ops, 300, T0, false).unwrap();

        assert_eq!(BTreeSet::from(["s".to_owned(), "t".to_owned()]), touched);
        assert_eq!(
            vec![
                ("s".to_owned(), 1, T0, "m1".to_owned()),
                ("s".to_owned(), 2, T0, "m2".to_owned()),
                ("t".to_owned(), 1, T0 + 5, "m1".to_owned()),
            ],
            history_rows(&conn)
        );
        // Deadlines are ten history TTLs after each stream's newest message.
        assert_eq!(vec![("s".to_owned(), 2, T0 + 3000), ("t".to_owned(), 1, T0 + 5 + 3000)], stream_rows(&conn));
        // The commit, then its touch; a touch for an unknown session changes nothing.
        assert_eq!(vec![("a".to_owned(), b"a".to_vec(), 20)], session_rows(&conn));
        assert_eq!(None, meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn clean_shutdown_is_written_with_the_batch() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();

        write_batch(&mut conn, &[history("s", 1)], 300, T0, true).unwrap();

        assert_eq!(Some("1".to_owned()), meta_get(&conn, "clean_shutdown").unwrap());
        assert_eq!(1, history_rows(&conn).len());
    }

    #[test]
    fn a_failed_batch_leaves_nothing_behind() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let conn = open(&path).unwrap();
        let mut read_only = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();

        assert!(write_batch(&mut read_only, &[history("s", 1), commit("a", 1)], 300, T0, true).is_err());

        assert!(history_rows(&conn).is_empty());
        assert!(session_rows(&conn).is_empty());
        assert_eq!(None, meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn a_stream_keeps_its_highest_offset_when_ops_arrive_out_of_order() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();

        write_batch(&mut conn, &[history("s", 5)], 300, T0, false).unwrap();
        write_batch(&mut conn, &[history("s", 4)], 300, T0, false).unwrap();

        assert_eq!(vec![("s".to_owned(), 5, T0 + 3000)], stream_rows(&conn));
    }

    #[test]
    fn a_stream_past_its_deadline_restarts_its_offsets() {
        // The memory broker drops a stream ten TTLs after its last message;
        // the next broadcast starts again at offset 1.
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        write_batch(&mut conn, &[history_at("s", 9, T0)], 300, T0, false).unwrap();

        write_batch(&mut conn, &[history_at("s", 1, T0 + 4000)], 300, T0 + 4000, false).unwrap();

        assert_eq!(vec![("s".to_owned(), 1, T0 + 7000)], stream_rows(&conn));
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::store 2>&1 | grep -E "^error" | head -2`
Expected: `error[E0425]: cannot find function \`write_batch\` in this scope`

- [ ] **Step 3: Implement**

In `store.rs`, replace the `use` block at the top with:

```rust
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, ErrorCode, OpenFlags, OptionalExtension, params};

use super::queue::WriteOp;
use super::units;
```

and add, after `mark_running`:

```rust
/// The streams a batch wrote (pruning by limit visits only these).
pub(super) type Touched = BTreeSet<String>;

const INSERT_HISTORY: &str = "INSERT INTO history (stream, seq, ts, data) VALUES (?1, ?2, ?3, ?4)
  ON CONFLICT (stream, seq) DO UPDATE SET ts = excluded.ts, data = excluded.data";
const UPSERT_SESSION: &str = "INSERT INTO sessions (sid, data, expires_at_ms) VALUES (?1, ?2, ?3)
  ON CONFLICT (sid) DO UPDATE SET data = excluded.data, expires_at_ms = excluded.expires_at_ms";
const TOUCH_SESSION: &str = "UPDATE sessions SET expires_at_ms = ?2 WHERE sid = ?1";
/// Keeps the highest offset (concurrent broadcasts may enqueue out of order),
/// unless the stored row's deadline has passed: the memory broker dropped that
/// stream and restarted its offsets, so the batch's offset is the stream's.
const UPSERT_STREAM: &str = "INSERT INTO streams (stream, max_seq, deadline) VALUES (?1, ?2, ?3)
  ON CONFLICT (stream) DO UPDATE SET
    max_seq = CASE WHEN streams.deadline < ?4 THEN excluded.max_seq
                   ELSE max(streams.max_seq, excluded.max_seq) END,
    deadline = max(streams.deadline, excluded.deadline)";

/// Writes `ops` in one transaction, in order, and upserts `streams` for every
/// stream they wrote. With `clean_shutdown`, marks the shutdown clean in the
/// same transaction (spec §5). `now_secs` decides whether a stored stream row
/// is stale. Nothing is written if any statement fails.
pub(super) fn write_batch(
    conn: &mut Connection,
    ops: &[WriteOp],
    history_ttl: i64,
    now_secs: i64,
    clean_shutdown: bool,
) -> Result<Touched, StoreError> {
    let tx = conn.transaction()?;
    // The newest offset and timestamp of each stream in the batch.
    let mut newest: BTreeMap<&str, (u64, i64)> = BTreeMap::new();
    {
        let mut history = tx.prepare_cached(INSERT_HISTORY)?;
        let mut commit = tx.prepare_cached(UPSERT_SESSION)?;
        let mut touch = tx.prepare_cached(TOUCH_SESSION)?;
        for op in ops {
            match op {
                WriteOp::History { stream, offset, ts_secs, data } => {
                    history.execute(params![stream, units::offset_to_sql(*offset), ts_secs, data])?;
                    let entry = newest.entry(stream.as_str()).or_insert((*offset, *ts_secs));
                    *entry = (entry.0.max(*offset), entry.1.max(*ts_secs));
                }
                WriteOp::SessionCommit { sid, data, expires_at_ms } => {
                    commit.execute(params![sid, data, expires_at_ms])?;
                }
                WriteOp::SessionTouch { sid, expires_at_ms } => {
                    touch.execute(params![sid, expires_at_ms])?;
                }
            }
        }
        let mut streams = tx.prepare_cached(UPSERT_STREAM)?;
        for (stream, (offset, ts)) in &newest {
            streams.execute(params![
                stream,
                units::offset_to_sql(*offset),
                units::stream_deadline(*ts, history_ttl),
                now_secs
            ])?;
        }
    }
    if clean_shutdown {
        meta_set(&tx, "clean_shutdown", "1")?;
    }
    tx.commit()?;
    Ok(newest.into_keys().map(str::to_owned).collect())
}
```

- [ ] **Step 4: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::store 2>&1 | grep "^test result"`
Expected: `test result: ok. 13 passed; …`

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite/store.rs
git commit -m "broker::sqlite: batch writes

One transaction per batch: history, session commits and touches in order, a streams
upsert per stream written (highest offset unless the row went stale), and the
clean_shutdown flag in the same transaction when asked.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: `store`: pruning, load queries, checkpoint, and their query plans

Pruning runs every 30 s (§3, §4) and must never scan. The loader's queries are defined here too, so one test checks every plan with `EXPLAIN QUERY PLAN`.

**Files:**
- Modify: `crates/cabby-core/src/broker/sqlite/store.rs`

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `store.rs`:

```rust
    /// The `EXPLAIN QUERY PLAN` details of `sql`, one line per step.
    fn plan(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> String {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        stmt.query_map(params, |r| r.get::<_, String>(3)).unwrap().map(Result::unwrap).collect::<Vec<_>>().join("\n")
    }

    #[test]
    fn prune_and_load_statements_use_indexes() {
        let dir = TempDir::new();
        let conn = open(&dir.file("cabby.db")).unwrap();
        let single: [(&str, &str); 5] = [
            (PRUNE_BY_TTL, "history_ts"),
            (PRUNE_STREAMS, "streams_deadline"),
            (PRUNE_SESSIONS, "sessions_expires"),
            (LOAD_STREAMS, "streams_deadline"),
            (LOAD_SESSIONS, "sessions_expires"),
        ];
        let mut plans: Vec<(&str, &str, String)> =
            single.iter().map(|(sql, index)| (*sql, *index, plan(&conn, sql, [0_i64]))).collect();
        plans.push((PRUNE_BY_LIMIT, "history_stream_seq", plan(&conn, PRUNE_BY_LIMIT, params!["s", 2_i64])));
        plans.push((LOAD_TAIL, "history_stream_seq", plan(&conn, LOAD_TAIL, params!["s", 9_i64, 100_i64])));

        for (sql, index, plan) in plans {
            assert!(plan.contains(&format!("INDEX {index}")), "{sql}\nuses {index}, plan:\n{plan}");
            assert!(!plan.contains("SCAN "), "{sql}\nscans, plan:\n{plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{sql}\nsorts, plan:\n{plan}");
        }
    }

    #[test]
    fn pruning_drops_expired_history_entries_beyond_the_limit_stale_streams_and_expired_sessions() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let mut ops: Vec<WriteOp> = (1..=5).map(|offset| history("s", offset)).collect();
        ops.extend([history_at("old", 1, T0 - 100), commit("live", (T0 + 1) * 1000), commit("gone", T0 * 1000 - 1)]);
        write_batch(&mut conn, &ops, 300, T0, false).unwrap();
        conn.execute("INSERT INTO streams (stream, max_seq, deadline) VALUES ('stale', 1, ?1)", [T0 - 1]).unwrap();

        let streams = Touched::from(["s".to_owned()]);
        let prune_at = Prune { history_cutoff: T0 - 60, now_secs: T0, now_ms: T0 * 1000, history_limit: 2, streams: &streams };
        prune(&mut conn, &prune_at).unwrap();

        let history: Vec<(String, i64)> = history_rows(&conn).into_iter().map(|(s, seq, ..)| (s, seq)).collect();
        assert_eq!(vec![("s".to_owned(), 4), ("s".to_owned(), 5)], history, "TTL and limit");
        let streams: Vec<String> = stream_rows(&conn).into_iter().map(|(s, ..)| s).collect();
        assert_eq!(vec!["old", "s"], streams, "the stale stream row is gone; 'old' is within its deadline");
        let sessions: Vec<String> = session_rows(&conn).into_iter().map(|(sid, ..)| sid).collect();
        assert_eq!(vec!["live"], sessions);
    }

    #[test]
    fn pruning_by_limit_visits_only_the_streams_it_is_given() {
        let dir = TempDir::new();
        let mut conn = open(&dir.file("cabby.db")).unwrap();
        let ops: Vec<WriteOp> = (1..=5).flat_map(|offset| [history("s", offset), history("t", offset)]).collect();
        write_batch(&mut conn, &ops, 300, T0, false).unwrap();

        let streams = Touched::from(["s".to_owned()]);
        let prune_at = Prune { history_cutoff: T0 - 60, now_secs: T0, now_ms: T0 * 1000, history_limit: 2, streams: &streams };
        prune(&mut conn, &prune_at).unwrap();

        let seqs = |stream: &str| -> Vec<i64> {
            history_rows(&conn).into_iter().filter(|(s, ..)| s == stream).map(|(_, seq, ..)| seq).collect()
        };
        assert_eq!(vec![4, 5], seqs("s"));
        assert_eq!(vec![1, 2, 3, 4, 5], seqs("t"));
    }

    #[test]
    fn checkpoint_empties_the_wal() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let mut conn = open(&path).unwrap();
        write_batch(&mut conn, &[history("s", 1)], 300, T0, false).unwrap();
        let wal = dir.file("cabby.db-wal");
        assert!(std::fs::metadata(&wal).unwrap().len() > 0);

        checkpoint(&conn).unwrap();

        assert_eq!(0, std::fs::metadata(&wal).unwrap().len());
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::store 2>&1 | grep -E "^error" | head -3`
Expected: `error[E0425]: cannot find value \`PRUNE_BY_TTL\` in this scope` (and the others).

- [ ] **Step 3: Implement**

Add to `store.rs`, after `write_batch`:

```rust
/// History older than the TTL (index `history_ts`).
pub(super) const PRUNE_BY_TTL: &str = "DELETE FROM history WHERE ts < ?1";
/// One stream's entries beyond `history_limit`, measured from its stored
/// newest offset (index `history_stream_seq`).
pub(super) const PRUNE_BY_LIMIT: &str =
    "DELETE FROM history WHERE stream = ?1 AND seq <= (SELECT max_seq - ?2 FROM streams WHERE stream = ?1)";
/// Streams past their deadline (index `streams_deadline`).
pub(super) const PRUNE_STREAMS: &str = "DELETE FROM streams WHERE deadline < ?1";
/// Expired sessions (index `sessions_expires`).
pub(super) const PRUNE_SESSIONS: &str = "DELETE FROM sessions WHERE expires_at_ms < ?1";
/// Streams the memory broker would still keep (`Memory::expire` keeps
/// `deadline >= now`).
pub(super) const LOAD_STREAMS: &str = "SELECT stream, max_seq, deadline FROM streams WHERE deadline >= ?1";
/// A stream's newest entries at or below its stored offset, newest first.
pub(super) const LOAD_TAIL: &str =
    "SELECT seq, ts, data FROM history WHERE stream = ?1 AND seq <= ?2 ORDER BY seq DESC LIMIT ?3";
/// Sessions not yet expired (the memory broker expires at `deadline <= now`).
pub(super) const LOAD_SESSIONS: &str = "SELECT sid, data, expires_at_ms FROM sessions WHERE expires_at_ms > ?1";

/// What a prune pass removes, computed by the persister from its clock.
pub(super) struct Prune<'a> {
    /// History older than this (unix seconds) is expired.
    pub(super) history_cutoff: i64,
    /// Streams whose deadline is before this (unix seconds) are dropped.
    pub(super) now_secs: i64,
    /// Sessions expiring before this (unix milliseconds) are dropped.
    pub(super) now_ms: i64,
    pub(super) history_limit: i64,
    /// Streams written since the last prune.
    pub(super) streams: &'a Touched,
}

/// One prune pass, in one transaction.
pub(super) fn prune(conn: &mut Connection, p: &Prune<'_>) -> Result<(), StoreError> {
    let tx = conn.transaction()?;
    tx.execute(PRUNE_BY_TTL, [p.history_cutoff])?;
    {
        let mut by_limit = tx.prepare_cached(PRUNE_BY_LIMIT)?;
        for stream in p.streams {
            by_limit.execute(params![stream, p.history_limit])?;
        }
    }
    tx.execute(PRUNE_STREAMS, [p.now_secs])?;
    tx.execute(PRUNE_SESSIONS, [p.now_ms])?;
    tx.commit()?;
    Ok(())
}

/// `PRAGMA wal_checkpoint(TRUNCATE)`: copies the WAL into the database file
/// and truncates it, so the file is self-contained for backups (spec §5).
pub(super) fn checkpoint(conn: &Connection) -> Result<(), StoreError> {
    let blocked: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    if blocked != 0 {
        return Err(StoreError::Other("the WAL checkpoint was blocked by another connection".into()));
    }
    Ok(())
}
```

- [ ] **Step 4: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::store 2>&1 | grep "^test result"`
Expected: `test result: ok. 17 passed; …`

The plan test was checked against SQLite 3.54.1 while writing this plan; the details it sees are `SEARCH history USING INDEX history_ts (ts<?)`, `SEARCH streams USING INDEX streams_deadline (deadline<?)`, `SEARCH sessions USING INDEX sessions_expires (expires_at_ms<?)`, `SEARCH history USING COVERING INDEX history_stream_seq (stream=? AND seq<?)` plus `SCALAR SUBQUERY 1` / `SEARCH streams USING PRIMARY KEY (stream=?)`, and `SEARCH history USING INDEX history_stream_seq (stream=? AND seq<?)` for the tail.

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite/store.rs
git commit -m "broker::sqlite: pruning, load queries and the WAL checkpoint

Prunes history by TTL and by limit, stale streams and expired sessions; every prune
and load statement is checked to use its index (EXPLAIN QUERY PLAN).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: `files`: modes, the lock file, moving a corrupt file aside

SQLite's WAL mode lets several processes open one database, and `locking_mode=EXCLUSIVE` only locks on the first write (§3), so the guard is an OS advisory lock on `<path>.lock` (`std::fs::File::try_lock`, stable since Rust 1.89, `flock` on Unix: per open file, released by the kernel when the process dies). SQLite gives a database's `-wal` and `-shm` files the database file's mode, so creating the database file with `0600` covers them (checked in Task 13).

**Files:**
- Create: `crates/cabby-core/src/broker/sqlite/files.rs`
- Modify: `crates/cabby-core/src/broker/sqlite.rs` (`mod files;`)

- [ ] **Step 1: Write the module with failing tests**

Add `mod files;` above `mod queue;` in `sqlite.rs`. Create `crates/cabby-core/src/broker/sqlite/files.rs` with `todo!()` bodies first:

```rust
//! Files around the database (spec §2, §3, §6): the parent directory (mode
//! 0700), the database file (0600), the lock file and moving a corrupt
//! database aside.

use std::fs::{DirBuilder, File, OpenOptions, TryLockError};
use std::io::ErrorKind;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

pub(super) const DIR_MODE: u32 = 0o700;
pub(super) const FILE_MODE: u32 = 0o600;
/// How often a held lock is tried again (spec §5 step 1).
const LOCK_RETRY: Duration = Duration::from_millis(100);

#[derive(Debug)]
pub(super) enum LockError {
    /// Another process still holds the lock.
    Held,
    Io(std::io::Error),
}

/// `<db><suffix>`, e.g. `cabby.db.lock`, `cabby.db-wal`.
pub(super) fn sibling(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

pub(super) fn lock_path(db: &Path) -> PathBuf {
    sibling(db, ".lock")
}

/// Creates the database's parent directory, and missing ancestors, with mode
/// 0700. An existing directory is left as it is.
pub(super) fn prepare_dir(db: &Path) -> std::io::Result<()> {
    todo!()
}

/// Creates the database file with mode 0600 unless it exists.
pub(super) fn create_db_file(db: &Path) -> std::io::Result<()> {
    todo!()
}

/// Takes the advisory lock on `<db>.lock`, trying every 100 ms for up to `timeout`.
pub(super) fn acquire_lock(db: &Path, timeout: Duration) -> Result<File, LockError> {
    todo!()
}

/// Renames the database and its `-wal` and `-shm` files to
/// `<db>.corrupt-<now_secs>` (`…-wal`, `…-shm`); returns the new database path.
pub(super) fn move_aside(db: &Path, now_secs: i64) -> std::io::Result<PathBuf> {
    todo!()
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::broker::sqlite::testing::TempDir;

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn the_lock_is_exclusive_until_released() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        let first = acquire_lock(&db, Duration::ZERO).unwrap();

        let started = Instant::now();
        assert!(matches!(acquire_lock(&db, Duration::from_millis(250)), Err(LockError::Held)));
        assert!(started.elapsed() >= Duration::from_millis(250), "retried until the timeout");

        drop(first);
        assert!(acquire_lock(&db, Duration::ZERO).is_ok(), "released with its file");
    }

    #[test]
    fn a_lock_released_while_waiting_is_taken() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        let first = acquire_lock(&db, Duration::ZERO).unwrap();
        let waiter = {
            let db = db.clone();
            std::thread::spawn(move || acquire_lock(&db, Duration::from_secs(10)).is_ok())
        };
        drop(first);
        assert!(waiter.join().unwrap());
    }

    #[test]
    fn created_directories_and_files_are_private() {
        let dir = TempDir::new();
        let db = dir.path().join("a").join("b").join("cabby.db");

        prepare_dir(&db).unwrap();
        create_db_file(&db).unwrap();
        let _lock = acquire_lock(&db, Duration::ZERO).unwrap();

        assert_eq!(DIR_MODE, mode(&dir.path().join("a")));
        assert_eq!(DIR_MODE, mode(&dir.path().join("a").join("b")));
        assert_eq!(FILE_MODE, mode(&db));
        assert_eq!(FILE_MODE, mode(&lock_path(&db)));
    }

    #[test]
    fn an_existing_database_file_is_kept() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        std::fs::write(&db, b"data").unwrap();

        create_db_file(&db).unwrap();

        assert_eq!(b"data".to_vec(), std::fs::read(&db).unwrap());
    }

    #[test]
    fn a_directory_under_a_file_cannot_be_prepared() {
        let dir = TempDir::new();
        std::fs::write(dir.file("blocker"), b"").unwrap();
        assert!(prepare_dir(&dir.path().join("blocker").join("sub").join("cabby.db")).is_err());
    }

    #[test]
    fn moving_aside_renames_the_database_and_its_wal_and_shm() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        for (path, content) in [(db.clone(), "db"), (sibling(&db, "-wal"), "wal"), (sibling(&db, "-shm"), "shm")] {
            std::fs::write(path, content).unwrap();
        }

        let moved = move_aside(&db, 1_700_000_000).unwrap();

        assert_eq!(dir.file("cabby.db.corrupt-1700000000"), moved);
        assert_eq!("db", std::fs::read_to_string(&moved).unwrap());
        assert_eq!("wal", std::fs::read_to_string(sibling(&moved, "-wal")).unwrap());
        assert_eq!("shm", std::fs::read_to_string(sibling(&moved, "-shm")).unwrap());
        assert!(!db.exists() && !sibling(&db, "-wal").exists() && !sibling(&db, "-shm").exists());
    }

    #[test]
    fn moving_aside_without_wal_and_shm() {
        let dir = TempDir::new();
        let db = dir.file("cabby.db");
        std::fs::write(&db, "db").unwrap();
        let moved = move_aside(&db, 5).unwrap();
        assert!(moved.exists() && !db.exists());
    }
}
```

- [ ] **Step 2: Run the tests to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::files 2>&1 | grep "^test result"`
Expected: `test result: FAILED. 0 passed; 7 failed; …`

- [ ] **Step 3: Implement**

Replace the four `todo!()` bodies:

```rust
pub(super) fn prepare_dir(db: &Path) -> std::io::Result<()> {
    match db.parent() {
        Some(dir) if !dir.as_os_str().is_empty() && !dir.exists() => {
            DirBuilder::new().recursive(true).mode(DIR_MODE).create(dir)
        }
        _ => Ok(()),
    }
}

pub(super) fn create_db_file(db: &Path) -> std::io::Result<()> {
    match OpenOptions::new().write(true).create_new(true).mode(FILE_MODE).open(db) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

pub(super) fn acquire_lock(db: &Path, timeout: Duration) -> Result<File, LockError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(FILE_MODE)
        .open(lock_path(db))
        .map_err(LockError::Io)?;
    let deadline = Instant::now() + timeout;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(LockError::Held);
                }
                std::thread::sleep(LOCK_RETRY.min(left));
            }
            Err(TryLockError::Error(e)) => return Err(LockError::Io(e)),
        }
    }
}

pub(super) fn move_aside(db: &Path, now_secs: i64) -> std::io::Result<PathBuf> {
    let target = sibling(db, &format!(".corrupt-{now_secs}"));
    std::fs::rename(db, &target)?;
    for side in ["-wal", "-shm"] {
        let from = sibling(db, side);
        if from.exists() {
            std::fs::rename(&from, sibling(&target, side))?;
        }
    }
    Ok(target)
}
```

- [ ] **Step 4: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::files 2>&1 | grep "^test result"`
Expected: `test result: ok. 7 passed; …`

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite.rs crates/cabby-core/src/broker/sqlite/files.rs
git commit -m "broker::sqlite: private files, the lock file and moving a corrupt file aside

Directory 0700 and files 0600 when created; an advisory lock on <path>.lock via
File::try_lock, retried every 100 ms; corrupt files renamed with their -wal/-shm.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: `persister`: batching, retries, rate-limited logs, pruning, the final flush

The persister owns the connection and the lock file and runs on one OS thread (Task 14 spawns it). A tick takes a batch (waiting up to 50 ms unless 1,000 ops are queued), writes it in one transaction, and keeps it for the next tick if the write fails. Write failures are logged once, then at most every 10 s, and the recovery once (§6). The final flush (§5) writes everything left and sets `clean_shutdown = '1'` in the same transaction, only if it succeeds before the deadline; then it checkpoints the WAL, closes the database and releases the lock.

**Files:**
- Create: `crates/cabby-core/src/broker/sqlite/persister.rs`
- Modify: `crates/cabby-core/src/broker/sqlite.rs` (`mod persister;`, `SqliteObserver`, `NoopObserver`, the `Recording` test observer)

- [ ] **Step 1: Add the observer trait and the test observer**

In `crates/cabby-core/src/broker/sqlite.rs`, add `mod persister;` between `mod files;` and `mod queue;`, replace `use std::sync::OnceLock;` with:

```rust
use std::sync::OnceLock;
use std::time::Duration;
```

and add, after `fn log()`:

```rust
/// Where the broker reports its write path; the server turns these into
/// metrics (spec §7). Calls come from the broadcast path and the persister
/// thread, so they must be cheap.
pub trait SqliteObserver: Send + Sync {
    /// Ops waiting to be written, after each write attempt.
    fn queue_size(&self, ops: usize);
    /// A batch was written in `took`.
    fn batch_written(&self, took: Duration);
    /// A batch write failed; the batch is kept and retried.
    fn write_failed(&self);
    /// An op of `kind` was dropped because the queue was full.
    fn dropped(&self, kind: queue::OpKind);
}

/// Reports nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObserver;

impl SqliteObserver for NoopObserver {
    fn queue_size(&self, _ops: usize) {}
    fn batch_written(&self, _took: Duration) {}
    fn write_failed(&self) {}
    fn dropped(&self, _kind: queue::OpKind) {}
}
```

Inside `pub(crate) mod testing { … }`, add below the existing `use` lines:

```rust
    use std::sync::Mutex;
    use std::time::Duration;

    use super::SqliteObserver;
    use super::queue::OpKind;
    use crate::broker::lock;

    /// Records what the broker reports.
    #[derive(Default)]
    pub(crate) struct Recording {
        queue_sizes: Mutex<Vec<usize>>,
        batches: Mutex<u64>,
        failures: Mutex<u64>,
        dropped: Mutex<Vec<OpKind>>,
    }

    impl Recording {
        pub(crate) fn queue_sizes(&self) -> Vec<usize> {
            lock(&self.queue_sizes).clone()
        }

        pub(crate) fn batches(&self) -> u64 {
            *lock(&self.batches)
        }

        pub(crate) fn failures(&self) -> u64 {
            *lock(&self.failures)
        }

        pub(crate) fn dropped(&self) -> Vec<OpKind> {
            lock(&self.dropped).clone()
        }
    }

    impl SqliteObserver for Recording {
        fn queue_size(&self, ops: usize) {
            lock(&self.queue_sizes).push(ops);
        }

        fn batch_written(&self, _took: Duration) {
            *lock(&self.batches) += 1;
        }

        fn write_failed(&self) {
            *lock(&self.failures) += 1;
        }

        fn dropped(&self, kind: OpKind) {
            lock(&self.dropped).push(kind);
        }
    }
```

- [ ] **Step 2: Write the persister skeleton and its failing tests**

Create `crates/cabby-core/src/broker/sqlite/persister.rs`:

```rust
//! The writer thread (spec §3, §5, §6).
//!
//! Live delivery never waits for the disk: a failing disk only fills the
//! bounded queue (which then drops history first) and the log (the first
//! failure, then at most one line per interval, then the recovery).

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;

use super::queue::{WriteOp, WriteQueue};
use super::store::{self, Touched};
use super::{SqliteObserver, log, units};
use crate::broker::{Clock, Config};
use crate::{log_error, log_info, log_warn};

/// A batch is written as soon as this many ops are queued (spec §3).
pub(super) const BATCH_SIZE: usize = 1_000;
/// Pause between attempts of the final write.
const FINAL_RETRY: Duration = Duration::from_millis(100);

/// The open database, its lock file and its path, owned by the persister.
pub(super) struct Database {
    pub(super) conn: Connection,
    pub(super) lock: File,
    pub(super) path: PathBuf,
}

/// The persister's intervals (spec §3, §6), fixed in production.
#[derive(Debug, Clone, Copy)]
pub(super) struct Timing {
    /// A partial batch is written after this long.
    pub(super) flush_interval: Duration,
    pub(super) prune_interval: Duration,
    /// At most one log line per interval while writes keep failing.
    pub(super) error_log_interval: Duration,
}

impl Timing {
    pub(super) fn standard() -> Self {
        Self {
            flush_interval: Duration::from_millis(50),
            prune_interval: Duration::from_secs(30),
            error_log_interval: Duration::from_secs(10),
        }
    }
}

/// Rate-limits write-failure logs: the first failure of a run, then at most one
/// per `interval`; the recovery once.
#[derive(Debug)]
pub(super) struct ErrorLog {
    interval: Duration,
    failing: bool,
    last_logged: Option<Instant>,
}

impl ErrorLog {
    pub(super) fn new(interval: Duration) -> Self {
        Self { interval, failing: false, last_logged: None }
    }

    /// Records a failure at `now`; true when it should be logged.
    pub(super) fn failed(&mut self, now: Instant) -> bool {
        todo!()
    }

    /// Records a success; true when it ends a run of failures.
    pub(super) fn succeeded(&mut self) -> bool {
        todo!()
    }
}

pub(super) struct Persister {
    db: Database,
    queue: Arc<WriteQueue>,
    history_ttl: i64,
    history_limit: i64,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn SqliteObserver>,
    timing: Timing,
    /// A batch that failed to write, retried before anything newer.
    pending: Vec<WriteOp>,
    /// Streams written since the last prune.
    touched: Touched,
    last_prune: Instant,
    errors: ErrorLog,
}

impl Persister {
    pub(super) fn new(
        db: Database,
        queue: Arc<WriteQueue>,
        config: &Config,
        clock: Arc<dyn Clock>,
        observer: Arc<dyn SqliteObserver>,
        timing: Timing,
    ) -> Self {
        Self {
            db,
            queue,
            history_ttl: config.history_ttl,
            history_limit: i64::try_from(config.history_limit).unwrap_or(i64::MAX),
            clock,
            observer,
            timing,
            pending: Vec::new(),
            touched: Touched::new(),
            last_prune: Instant::now(),
            errors: ErrorLog::new(timing.error_log_interval),
        }
    }

    /// The thread body: ticks until the queue closes, then the final flush.
    /// Returns whether the shutdown was marked clean.
    pub(super) fn run(mut self) -> bool {
        todo!()
    }

    /// One step: write the next batch (or retry the failed one after a pause),
    /// report the queue size, and prune when due.
    pub(super) fn tick(&mut self) {
        todo!()
    }

    /// Writes everything still queued, marking the shutdown clean only if that
    /// succeeds before `deadline`; then checkpoints the WAL, closes the
    /// database and releases the lock. Returns whether it marked it clean.
    pub(super) fn finish(mut self, deadline: Instant) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use rusqlite::OpenFlags;

    use super::*;
    use crate::broker::sqlite::testing::{Recording, TempDir, history};
    use crate::broker::test_clock::{T0, TestClock};

    fn timing(flush_interval: Duration) -> Timing {
        Timing { flush_interval, prune_interval: Duration::from_secs(3600), error_log_interval: Duration::from_secs(10) }
    }

    fn persister(
        dir: &TempDir,
        conn: Connection,
        queue: &Arc<WriteQueue>,
        observer: &Arc<Recording>,
        config: &Config,
        timing: Timing,
    ) -> Persister {
        let lock = File::create(dir.file("cabby.db.lock")).unwrap();
        let db = Database { conn, lock, path: dir.file("cabby.db") };
        Persister::new(db, queue.clone(), config, TestClock::new(), observer.clone(), timing)
    }

    fn query_i64s(path: &Path, sql: &str) -> Vec<i64> {
        let conn = Connection::open(path).unwrap();
        let mut stmt = conn.prepare(sql).unwrap();
        stmt.query_map([], |r| r.get(0)).unwrap().map(Result::unwrap).collect()
    }

    fn history_count(dir: &TempDir) -> i64 {
        query_i64s(&dir.file("cabby.db"), "SELECT count(*) FROM history")[0]
    }

    #[test]
    fn write_failures_are_logged_first_then_every_interval_and_the_recovery_once() {
        let t0 = Instant::now();
        let mut log = ErrorLog::new(Duration::from_secs(10));
        assert!(log.failed(t0), "the first failure");
        assert!(!log.failed(t0 + Duration::from_secs(1)));
        assert!(!log.failed(t0 + Duration::from_secs(9)));
        assert!(log.failed(t0 + Duration::from_secs(10)), "one line per interval");
        assert!(!log.failed(t0 + Duration::from_secs(11)));
        assert!(log.succeeded(), "the recovery");
        assert!(!log.succeeded(), "logged once");
        assert!(log.failed(t0 + Duration::from_secs(12)), "a new run of failures is logged at once");
    }

    #[test]
    fn a_full_batch_is_written_without_waiting_for_the_tick() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 3));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_secs(60)));
        for offset in 1..=3 {
            queue.push(history("s", offset));
        }

        let started = Instant::now();
        p.tick();

        assert!(started.elapsed() < Duration::from_secs(30), "did not wait for the 60 s tick");
        assert_eq!(3, history_count(&dir));
        assert_eq!(1, observer.batches());
    }

    #[test]
    fn a_partial_batch_is_written_after_the_flush_interval() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(50)));
        queue.push(history("s", 1));

        let started = Instant::now();
        p.tick();

        assert!(started.elapsed() >= Duration::from_millis(50), "waited for the tick");
        assert_eq!(1, history_count(&dir));
    }

    #[test]
    fn each_batch_upserts_the_streams_it_wrote() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 3));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_secs(60)));
        for op in [history("s", 1), history("s", 2), history("t", 1)] {
            queue.push(op);
        }

        p.tick();

        let path = dir.file("cabby.db");
        assert_eq!(vec![2, 1], query_i64s(&path, "SELECT max_seq FROM streams ORDER BY stream"));
        assert_eq!(vec![T0 + 3000, T0 + 3000], query_i64s(&path, "SELECT deadline FROM streams ORDER BY stream"));
    }

    #[test]
    fn a_failed_batch_is_kept_and_retried_on_the_next_tick() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let conn = store::open(&path).unwrap();
        conn.busy_timeout(Duration::from_millis(20)).unwrap();
        let queue = Arc::new(WriteQueue::new(100, 1));
        let observer = Arc::new(Recording::default());
        let mut p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(20)));
        // Another connection holds SQLite's write lock: the batch fails with SQLITE_BUSY.
        let blocker = Connection::open(&path).unwrap();
        blocker.execute_batch("BEGIN EXCLUSIVE").unwrap();
        queue.push(history("s", 1));

        p.tick();
        assert_eq!(1, observer.failures());
        assert_eq!(Some(&1), observer.queue_sizes().last(), "the failed op is still waiting");

        blocker.execute_batch("ROLLBACK").unwrap();
        p.tick();
        assert_eq!(1, history_count(&dir));
        assert_eq!((1, 1), (observer.failures(), observer.batches()));
        assert_eq!(Some(&0), observer.queue_sizes().last());
    }

    #[test]
    fn pruning_runs_when_due() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_limit = 2;
        let mut t = timing(Duration::from_millis(10));
        t.prune_interval = Duration::ZERO;
        let queue = Arc::new(WriteQueue::new(100, 5));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let mut p = persister(&dir, conn, &queue, &observer, &config, t);
        for offset in 1..=5 {
            queue.push(history("s", offset));
        }

        p.tick();

        assert_eq!(vec![4, 5], query_i64s(&dir.file("cabby.db"), "SELECT seq FROM history ORDER BY seq"));
    }

    #[test]
    fn the_final_flush_writes_everything_marks_the_shutdown_clean_and_leaves_no_wal() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        queue.push(history("s", 1));
        queue.push(history("s", 2));

        assert!(p.finish(Instant::now() + Duration::from_secs(5)));

        assert!(!dir.file("cabby.db-wal").exists(), "checkpointed and closed");
        assert_eq!(2, history_count(&dir));
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        assert_eq!(Some("1".to_owned()), store::meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn a_final_flush_to_a_read_only_database_leaves_the_shutdown_unclean() {
        let dir = TempDir::new();
        let path = dir.file("cabby.db");
        let setup = store::open(&path).unwrap();
        store::meta_set(&setup, "clean_shutdown", "0").unwrap();
        let read_only = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let p = persister(&dir, read_only, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        queue.push(history("s", 1));

        assert!(!p.finish(Instant::now() + Duration::from_millis(300)));

        assert!(observer.failures() >= 1);
        assert_eq!(Some("0".to_owned()), store::meta_get(&setup, "clean_shutdown").unwrap());
        assert_eq!(0, history_count(&dir));
    }

    #[test]
    fn without_time_left_the_final_write_is_not_attempted() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        queue.push(history("s", 1));

        assert!(!p.finish(Instant::now()));

        assert_eq!(0, history_count(&dir));
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        assert_eq!(None, store::meta_get(&conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn run_ends_with_the_final_flush_once_the_queue_closes() {
        let dir = TempDir::new();
        let queue = Arc::new(WriteQueue::new(100, 1000));
        let observer = Arc::new(Recording::default());
        let conn = store::open(&dir.file("cabby.db")).unwrap();
        let p = persister(&dir, conn, &queue, &observer, &Config::new(), timing(Duration::from_millis(10)));
        let writer = std::thread::spawn(move || p.run());

        queue.push(history("s", 1));
        queue.close(Instant::now() + Duration::from_secs(5));

        assert!(writer.join().unwrap(), "marked clean");
        assert_eq!(1, history_count(&dir));
    }
}
```

- [ ] **Step 3: Run the tests to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::persister 2>&1 | grep "^test result"`
Expected: `test result: FAILED. 0 passed; 10 failed; …` (`not yet implemented`).

- [ ] **Step 4: Implement**

Replace the `todo!()` bodies of `ErrorLog`:

```rust
    pub(super) fn failed(&mut self, now: Instant) -> bool {
        let log = !self.failing || self.last_logged.is_none_or(|t| now.duration_since(t) >= self.interval);
        self.failing = true;
        if log {
            self.last_logged = Some(now);
        }
        log
    }

    pub(super) fn succeeded(&mut self) -> bool {
        std::mem::replace(&mut self.failing, false)
    }
```

Replace the three `todo!()` bodies of `Persister`, and add the two private helpers below `tick`:

```rust
    pub(super) fn run(mut self) -> bool {
        while self.queue.closed().is_none() {
            self.tick();
        }
        let deadline = self.queue.closed().unwrap_or_else(Instant::now);
        self.finish(deadline)
    }

    pub(super) fn tick(&mut self) {
        if self.pending.is_empty() {
            self.pending = self.queue.take_batch(self.timing.flush_interval);
        } else {
            self.queue.pause(self.timing.flush_interval);
        }
        if !self.pending.is_empty() {
            let ops = std::mem::take(&mut self.pending);
            if !self.write(&ops, false) {
                self.pending = ops;
            }
        }
        self.observer.queue_size(self.queue.len() + self.pending.len());
        if self.last_prune.elapsed() >= self.timing.prune_interval {
            self.prune();
        }
    }

    /// One batch; true when it was written.
    fn write(&mut self, ops: &[WriteOp], clean_shutdown: bool) -> bool {
        let started = Instant::now();
        let now_secs = units::now_secs(self.clock.as_ref());
        match store::write_batch(&mut self.db.conn, ops, self.history_ttl, now_secs, clean_shutdown) {
            Ok(touched) => {
                self.observer.batch_written(started.elapsed());
                self.touched.extend(touched);
                if self.errors.succeeded() {
                    log_info!(log(), "SQLite writes recovered", "path" => self.db.path.display().to_string());
                }
                true
            }
            Err(e) => {
                self.observer.write_failed();
                if self.errors.failed(Instant::now()) {
                    log_error!(log(), "failed to write to the SQLite database, retrying",
                        "path" => self.db.path.display().to_string(), "ops" => ops.len(), "error" => e.to_string());
                }
                false
            }
        }
    }

    fn prune(&mut self) {
        self.last_prune = Instant::now();
        let now = self.clock.now();
        let now_secs = units::secs(now);
        let prune = store::Prune {
            history_cutoff: units::history_cutoff(now_secs, self.history_ttl),
            now_secs,
            now_ms: units::millis(now),
            history_limit: self.history_limit,
            streams: &self.touched,
        };
        match store::prune(&mut self.db.conn, &prune) {
            Ok(()) => self.touched.clear(),
            Err(e) => log_warn!(log(), "failed to prune the SQLite database", "error" => e.to_string()),
        }
    }

    pub(super) fn finish(mut self, deadline: Instant) -> bool {
        let mut ops = std::mem::take(&mut self.pending);
        ops.extend(self.queue.take_all());
        let mut clean = false;
        while Instant::now() < deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            let _ = self.db.conn.busy_timeout(left.min(store::BUSY_TIMEOUT));
            if self.write(&ops, true) {
                clean = true;
                break;
            }
            std::thread::sleep(FINAL_RETRY.min(deadline.saturating_duration_since(Instant::now())));
        }
        let path = self.db.path.display().to_string();
        if !clean {
            log_warn!(log(), "the final SQLite write did not finish in time; the next start uses a new epoch",
                "path" => path.clone(), "ops" => ops.len());
        }
        let Database { conn, lock, .. } = self.db;
        let _ = conn.busy_timeout(deadline.saturating_duration_since(Instant::now()));
        if let Err(e) = store::checkpoint(&conn) {
            log_warn!(log(), "failed to checkpoint the SQLite WAL", "path" => path.clone(), "error" => e.to_string());
        }
        if let Err((_, e)) = conn.close() {
            log_warn!(log(), "failed to close the SQLite database", "path" => path, "error" => e.to_string());
        }
        drop(lock);
        clean
    }
```

- [ ] **Step 5: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::persister 2>&1 | grep "^test result"`
Expected: `test result: ok. 10 passed; …`

- [ ] **Step 6: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite.rs crates/cabby-core/src/broker/sqlite/persister.rs
git commit -m "broker::sqlite: the persister

Batches by size (1000) and time (50 ms), keeps and retries a failed batch, logs
write failures first / every 10 s / on recovery, prunes every 30 s, and finishes with
one transaction that marks the shutdown clean only if it succeeds in time, then a
WAL checkpoint, close and unlock.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: `loader`: lock, checks, epoch, streams, sessions, startup line

Startup, in the order of §5: directory, lock (retried every 100 ms for up to 10 s), open and check (moving the file aside only on corruption, §6), epoch from `clean_shutdown`, `mark_running`, streams (the contiguous tail anchored at `max_seq`, within `history_ttl`, at most `history_limit`), sessions (unexpired, with their original expiry), then the startup line (§7).

**Files:**
- Create: `crates/cabby-core/src/broker/sqlite/loader.rs`
- Modify: `crates/cabby-core/src/broker/sqlite.rs` (`mod loader;`, `SqliteOptions`, `LOCK_TIMEOUT`, `QUEUE_CAPACITY`)

- [ ] **Step 1: Add the options**

In `crates/cabby-core/src/broker/sqlite.rs`, add `mod loader;` between `mod files;` and `mod persister;`, replace the `use` lines at the top with:

```rust
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use crate::logger::Logger;
```

and add after `NoopObserver`'s impl:

```rust
/// How long opening waits for another process to release the database (spec §5).
pub const LOCK_TIMEOUT: Duration = Duration::from_secs(10);
/// The write queue's bound (spec §3).
pub const QUEUE_CAPACITY: usize = 100_000;

/// Where the database lives and how the broker treats it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteOptions {
    pub path: PathBuf,
    pub lock_timeout: Duration,
    pub queue_capacity: usize,
}

impl SqliteOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), lock_timeout: LOCK_TIMEOUT, queue_capacity: QUEUE_CAPACITY }
    }
}
```

- [ ] **Step 2: Write the loader skeleton and its failing tests**

Create `crates/cabby-core/src/broker/sqlite/loader.rs`:

```rust
//! Startup (spec §5, §6, §7): the lock, opening and checking the file, the
//! epoch, and loading streams and sessions into the memory broker before
//! Cabby accepts connections.

use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rusqlite::{Connection, params};

use super::files::{self, LockError};
use super::persister::Database;
use super::store::{self, StoreError};
use super::{SqliteOptions, log, units};
use crate::broker::{BrokerError, Clock, Config, Entry, Memory, nanoid};
use crate::{log_info, log_warn};

/// How the previous run ended, as the loader found the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Previous {
    /// No epoch stored yet: a first start.
    NewDatabase,
    /// Shut down gracefully: the epoch is kept.
    Clean,
    /// Crashed, killed, or the final write failed: a new epoch.
    Unclean,
    /// The file was corrupt and moved to this path; a new database.
    Corrupt(PathBuf),
}

/// What the loader restored (spec §7's startup line).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadReport {
    pub messages: usize,
    pub streams: usize,
    pub sessions: usize,
    pub epoch: String,
    pub previous: Previous,
    pub took: Duration,
}

impl LoadReport {
    /// E.g. `restored 4210 messages across 37 streams and 112 sessions (epoch
    /// abcd, clean shutdown) in 18 ms`.
    pub fn summary(&self) -> String {
        todo!()
    }
}

/// The open database and what was loaded from it.
pub(super) struct Loaded {
    pub(super) db: Database,
    pub(super) report: LoadReport,
}

/// Opens `options.path` and fills `memory` from it.
pub(super) fn load(
    memory: &Memory,
    config: &Config,
    options: &SqliteOptions,
    clock: &dyn Clock,
) -> Result<Loaded, BrokerError> {
    todo!()
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    use super::*;
    use crate::broker::Broker;
    use crate::broker::sqlite::queue::WriteOp;
    use crate::broker::sqlite::testing::{TempDir, commit, history, history_at};
    use crate::broker::test_clock::{T0, TestClock};

    /// Writes `ops` and `meta` into the database at `db`, as an earlier run would have.
    fn seed(db: &Path, history_ttl: i64, ops: &[WriteOp], meta: &[(&str, &str)]) {
        files::prepare_dir(db).unwrap();
        let mut conn = store::open(db).unwrap();
        store::write_batch(&mut conn, ops, history_ttl, T0, false).unwrap();
        for (key, value) in meta {
            store::meta_set(&conn, key, value).unwrap();
        }
    }

    fn options(dir: &TempDir) -> SqliteOptions {
        let mut options = SqliteOptions::new(dir.db());
        options.lock_timeout = Duration::from_millis(200);
        options
    }

    /// Loads `dir`'s database into a new memory broker on `clock`.
    fn load_into(dir: &TempDir, config: &Config, clock: &Arc<TestClock>) -> Result<(Memory, Loaded), BrokerError> {
        let memory = Memory::new(None, None, config, clock.clone());
        let loaded = load(&memory, config, &options(dir), clock.as_ref())?;
        Ok((memory, loaded))
    }

    fn data(memory: &Memory, stream: &str) -> Vec<String> {
        memory.history_since(stream, 0).unwrap().unwrap_or_default().into_iter().map(|m| m.data).collect()
    }

    fn corrupt_files(dir: &TempDir) -> Vec<String> {
        std::fs::read_dir(dir.db().parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".corrupt-"))
            .collect()
    }

    #[test]
    fn an_empty_directory_gives_a_new_database() {
        let dir = TempDir::new();
        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        let r = &loaded.report;
        assert_eq!((0, 0, 0, Previous::NewDatabase), (r.messages, r.streams, r.sessions, r.previous.clone()));
        assert_eq!(memory.epoch(), r.epoch);
        assert_eq!(4, r.epoch.len());
        assert_eq!(Some(r.epoch.clone()), store::meta_get(&loaded.db.conn, "epoch").unwrap());
        assert_eq!(Some("0".to_owned()), store::meta_get(&loaded.db.conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn history_is_loaded_within_the_limit_and_the_ttl() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_limit = 3;
        config.history_ttl = 60;
        let mut ops: Vec<WriteOp> = (1..=5).map(|offset| history("s", offset)).collect();
        ops.extend([history_at("t", 1, T0 - 120), history_at("t", 2, T0 - 120), history_at("t", 3, T0)]);
        seed(&dir.db(), 60, &ops, &[]);

        let (memory, loaded) = load_into(&dir, &config, &TestClock::new()).unwrap();

        assert_eq!(vec!["m3", "m4", "m5"], data(&memory, "s"), "the newest history_limit entries");
        assert_eq!(vec!["m3"], data(&memory, "t"), "entries beyond the TTL are not loaded");
        assert_eq!((4, 2), (loaded.report.messages, loaded.report.streams));
        assert_eq!(6, memory.add("s", "next"));
    }

    #[test]
    fn a_gap_loads_only_the_contiguous_tail() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[history("s", 1), history("s", 2), history("s", 4), history("s", 5)], &[]);

        let (memory, _) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_eq!(vec!["m4", "m5"], data(&memory, "s"));
        assert_eq!(6, memory.add("s", "next"));
    }

    #[test]
    fn a_stream_whose_newest_row_is_missing_loads_empty_at_its_offset() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &(1..=5).map(|offset| history("s", offset)).collect::<Vec<_>>(), &[]);
        store::open(&dir.db()).unwrap().execute("UPDATE streams SET max_seq = 6", []).unwrap();

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_eq!(Some(vec![]), memory.history_since("s", 0).unwrap(), "the stream exists, without entries");
        assert_eq!((0, 1), (loaded.report.messages, loaded.report.streams));
        assert_eq!(7, memory.add("s", "next"));
    }

    #[test]
    fn a_stream_whose_entries_all_expired_keeps_its_offset() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_ttl = 60;
        // Deadline T0 - 120 + 600: still ahead.
        seed(&dir.db(), 60, &[history_at("s", 1, T0 - 120), history_at("s", 2, T0 - 120)], &[]);

        let (memory, _) = load_into(&dir, &config, &TestClock::new()).unwrap();

        assert!(data(&memory, "s").is_empty());
        assert_eq!(3, memory.add("s", "next"), "max_seq + 1");
    }

    #[test]
    fn a_stream_past_its_deadline_is_not_loaded() {
        let dir = TempDir::new();
        let mut config = Config::new();
        config.history_ttl = 60;
        // Deadline T0 - 1000 + 600: passed.
        seed(&dir.db(), 60, &[history_at("s", 4, T0 - 1000)], &[]);

        let (memory, loaded) = load_into(&dir, &config, &TestClock::new()).unwrap();

        assert_eq!(None, memory.history_since("s", 0).unwrap());
        assert_eq!(0, loaded.report.streams);
        assert_eq!(1, memory.add("s", "next"));
    }

    #[test]
    fn unexpired_sessions_are_restored_with_their_original_expiry() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        seed(&dir.db(), 300, &[commit("live", (T0 + 10) * 1000), commit("gone", T0 * 1000)], &[]);

        let (memory, loaded) = load_into(&dir, &Config::new(), &clock).unwrap();

        assert_eq!(1, loaded.report.sessions);
        assert_eq!(Some(b"live".to_vec()), memory.restore_session("live").unwrap());
        assert_eq!(None, memory.restore_session("gone").unwrap());
        clock.advance(Duration::from_millis(9_999));
        memory.expire();
        assert!(memory.restore_session("live").unwrap().is_some());
        clock.advance(Duration::from_millis(1));
        memory.expire();
        assert_eq!(None, memory.restore_session("live").unwrap(), "expired at its original deadline");
    }

    #[test]
    fn the_epoch_is_kept_after_a_clean_shutdown() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("epoch", "abcd"), ("clean_shutdown", "1")]);

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_eq!("abcd", memory.epoch());
        assert_eq!(Previous::Clean, loaded.report.previous);
        assert_eq!(Some("0".to_owned()), store::meta_get(&loaded.db.conn, "clean_shutdown").unwrap());
    }

    #[test]
    fn the_epoch_is_rotated_after_an_unclean_shutdown() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("epoch", "abcd"), ("clean_shutdown", "0")]);

        let (memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        assert_ne!("abcd", memory.epoch());
        assert_eq!(Previous::Unclean, loaded.report.previous);
        assert_eq!(Some(memory.epoch()), store::meta_get(&loaded.db.conn, "epoch").unwrap());
    }

    #[test]
    fn a_corrupt_file_is_moved_aside_and_a_new_one_started() {
        let dir = TempDir::new();
        let db = dir.db();
        files::prepare_dir(&db).unwrap();
        std::fs::write(&db, vec![0x42_u8; 8192]).unwrap();

        let (_, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();

        let Previous::Corrupt(moved) = &loaded.report.previous else {
            panic!("expected Corrupt, got {:?}", loaded.report.previous)
        };
        assert_eq!(&files::sibling(&db, &format!(".corrupt-{T0}")), moved);
        assert_eq!(vec![0x42_u8; 8192], std::fs::read(moved).unwrap(), "the corrupt file is kept for inspection");
        assert_eq!(1, store::schema_version(&loaded.db.conn).unwrap(), "a new database");
    }

    #[test]
    fn a_newer_schema_is_refused_and_the_file_kept() {
        let dir = TempDir::new();
        seed(&dir.db(), 300, &[], &[("schema_version", "99")]);

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");

        assert!(err.message.contains("schema version 99 is newer than this Cabby supports (1)"), "{}", err.message);
        assert!(dir.db().exists());
        assert!(corrupt_files(&dir).is_empty());
    }

    #[test]
    fn an_unreadable_file_is_an_error_not_corruption() {
        let dir = TempDir::new();
        let db = dir.db();
        seed(&db, 300, &[], &[]);
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o000)).unwrap();
        if std::fs::read(&db).is_ok() {
            return; // Running as root: file permissions don't apply.
        }

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");
        std::fs::set_permissions(&db, std::fs::Permissions::from_mode(0o600)).unwrap();

        assert!(err.message.contains("unable to open database file"), "{}", err.message);
        assert!(corrupt_files(&dir).is_empty());
    }

    #[test]
    fn a_database_in_use_is_refused() {
        let dir = TempDir::new();
        let db = dir.db();
        files::prepare_dir(&db).unwrap();
        let _held = files::acquire_lock(&db, Duration::ZERO).unwrap();

        let err = load_into(&dir, &Config::new(), &TestClock::new()).err().expect("refused");

        assert!(err.message.contains("in use by another Cabby process"), "{}", err.message);
        assert!(!db.exists(), "not even opened");
    }

    #[test]
    fn an_impossible_directory_is_an_error() {
        let dir = TempDir::new();
        std::fs::write(dir.file("blocker"), b"").unwrap();
        let clock = TestClock::new();
        let memory = Memory::new(None, None, &Config::new(), clock.clone());
        let options = SqliteOptions::new(dir.path().join("blocker").join("sub").join("cabby.db"));

        let err = load(&memory, &Config::new(), &options, clock.as_ref()).err().expect("refused");

        assert!(err.message.contains("cannot create its directory"), "{}", err.message);
    }

    #[test]
    fn the_database_its_wal_shm_lock_file_and_directory_are_private() {
        let dir = TempDir::new();
        let (_memory, loaded) = load_into(&dir, &Config::new(), &TestClock::new()).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

        let db = dir.db();
        assert_eq!(0o700, mode(db.parent().unwrap()));
        for path in [db.clone(), files::sibling(&db, "-wal"), files::sibling(&db, "-shm"), files::lock_path(&db)] {
            assert_eq!(0o600, mode(&path), "{}", path.display());
        }
        drop(loaded);
    }

    #[test]
    fn the_startup_line_says_what_was_restored() {
        let report = |previous| LoadReport {
            messages: 4210,
            streams: 37,
            sessions: 112,
            epoch: "abcd".into(),
            previous,
            took: Duration::from_millis(18),
        };
        let head = "restored 4210 messages across 37 streams and 112 sessions";
        assert_eq!(format!("{head} (epoch abcd, clean shutdown) in 18 ms"), report(Previous::Clean).summary());
        assert_eq!(
            format!("{head} (epoch abcd, new epoch after an unclean shutdown) in 18 ms"),
            report(Previous::Unclean).summary()
        );
        assert_eq!(format!("{head} (epoch abcd, new database) in 18 ms"), report(Previous::NewDatabase).summary());
        assert_eq!(
            format!("{head} (epoch abcd, new database, the corrupt file was moved to /d/cabby.db.corrupt-5) in 18 ms"),
            report(Previous::Corrupt("/d/cabby.db.corrupt-5".into())).summary()
        );
    }
}
```

- [ ] **Step 3: Run the tests to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::loader 2>&1 | grep "^test result"`
Expected: `test result: FAILED. 0 passed; 16 failed; …`

- [ ] **Step 4: Implement**

Replace `LoadReport::summary` and `load`, and add the helpers below `load`:

```rust
    pub fn summary(&self) -> String {
        let how = match &self.previous {
            Previous::NewDatabase => "new database".to_owned(),
            Previous::Clean => "clean shutdown".to_owned(),
            Previous::Unclean => "new epoch after an unclean shutdown".to_owned(),
            Previous::Corrupt(moved) => format!("new database, the corrupt file was moved to {}", moved.display()),
        };
        format!(
            "restored {} messages across {} streams and {} sessions (epoch {}, {how}) in {} ms",
            self.messages,
            self.streams,
            self.sessions,
            self.epoch,
            self.took.as_millis()
        )
    }
```

```rust
pub(super) fn load(
    memory: &Memory,
    config: &Config,
    options: &SqliteOptions,
    clock: &dyn Clock,
) -> Result<Loaded, BrokerError> {
    let started = Instant::now();
    let path = options.path.as_path();
    files::prepare_dir(path).map_err(|e| error(path, format!("cannot create its directory: {e}")))?;
    let lock = files::acquire_lock(path, options.lock_timeout).map_err(|e| match e {
        LockError::Held => error(
            path,
            format!(
                "in use by another Cabby process (lock file {} still held after {} s)",
                files::lock_path(path).display(),
                options.lock_timeout.as_secs()
            ),
        ),
        LockError::Io(e) => error(path, format!("cannot open the lock file {}: {e}", files::lock_path(path).display())),
    })?;

    let (mut conn, moved) = match open_file(path) {
        Ok(conn) => (conn, None),
        Err(StoreError::Corrupt(why)) => {
            let moved = files::move_aside(path, units::now_secs(clock))
                .map_err(|e| error(path, format!("is corrupt ({why}) and could not be moved aside: {e}")))?;
            log_warn!(log(), &format!("the SQLite database is corrupt ({why}); moved it to {} and starting a new one",
                moved.display()), "path" => path.display().to_string());
            (open_file(path).map_err(|e| error(path, e))?, Some(moved))
        }
        Err(e) => return Err(error(path, e)),
    };

    let saved = store::meta_get(&conn, "epoch").map_err(|e| error(path, e))?.filter(|e| !e.is_empty());
    let clean = store::meta_get(&conn, "clean_shutdown").map_err(|e| error(path, e))?.as_deref() == Some("1");
    let (epoch, previous) = match (moved, saved) {
        (Some(moved), _) => (nanoid(4), Previous::Corrupt(moved)),
        (None, None) => (nanoid(4), Previous::NewDatabase),
        (None, Some(saved)) if clean => (saved, Previous::Clean),
        (None, Some(_)) => (nanoid(4), Previous::Unclean),
    };
    store::mark_running(&mut conn, &epoch).map_err(|e| error(path, e))?;
    memory.set_epoch(&epoch);

    let (messages, streams) = load_streams(&conn, memory, config, clock).map_err(|e| error(path, e))?;
    let sessions = load_sessions(&conn, memory, clock).map_err(|e| error(path, e))?;
    let report = LoadReport { messages, streams, sessions, epoch, previous, took: started.elapsed() };
    log_info!(log(), &report.summary(), "path" => path.display().to_string());
    Ok(Loaded { db: Database { conn, lock, path: path.to_owned() }, report })
}

fn error(path: &Path, e: impl Display) -> BrokerError {
    BrokerError { message: format!("SQLite database {}: {e}", path.display()) }
}

/// Creates the file with mode 0600 if needed (SQLite gives its -wal and -shm
/// files the same mode), then opens and checks it.
fn open_file(path: &Path) -> Result<Connection, StoreError> {
    files::create_db_file(path).map_err(|e| StoreError::Other(format!("cannot create the file: {e}")))?;
    store::open(path)
}

/// Installs every stream still within its deadline with its contiguous tail;
/// returns (messages, streams).
fn load_streams(
    conn: &Connection,
    memory: &Memory,
    config: &Config,
    clock: &dyn Clock,
) -> Result<(usize, usize), StoreError> {
    let now = units::now_secs(clock);
    let cutoff = units::history_cutoff(now, config.history_ttl);
    let limit = i64::try_from(config.history_limit).unwrap_or(i64::MAX);
    let rows: Vec<(String, i64, i64)> = conn
        .prepare(store::LOAD_STREAMS)?
        .query_map([now], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<Result<_, _>>()?;
    let mut tail = conn.prepare(store::LOAD_TAIL)?;
    let (mut messages, mut streams) = (0, 0);
    for (name, max_seq, deadline) in rows {
        let Some(max_offset) = units::offset_from_sql(max_seq) else { continue };
        // Walk down from max_seq; stop at the first gap or expired entry.
        let mut expected = max_offset;
        let mut entries = Vec::new();
        let mut found = tail.query(params![name, max_seq, limit])?;
        while let Some(row) = found.next()? {
            let (seq, ts, data): (i64, i64, String) = (row.get(0)?, row.get(1)?, row.get(2)?);
            if expected == 0 || units::offset_from_sql(seq) != Some(expected) || ts < cutoff {
                break;
            }
            entries.push(Entry { offset: expected, timestamp: ts, data });
            expected -= 1;
        }
        entries.reverse();
        messages += entries.len();
        memory.restore_stream(&name, max_offset, deadline, entries).map_err(|e| StoreError::Other(e.message))?;
        streams += 1;
    }
    Ok((messages, streams))
}

/// Restores every unexpired session with its stored expiry.
fn load_sessions(conn: &Connection, memory: &Memory, clock: &dyn Clock) -> Result<usize, StoreError> {
    let mut stmt = conn.prepare(store::LOAD_SESSIONS)?;
    let mut rows = stmt.query([units::now_millis(clock)])?;
    let mut sessions = 0;
    while let Some(row) = rows.next()? {
        let sid: String = row.get(0)?;
        memory.restore_session_with_deadline(&sid, row.get(1)?, row.get(2)?);
        sessions += 1;
    }
    Ok(sessions)
}
```

- [ ] **Step 5: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::loader 2>&1 | grep "^test result"`
Expected: `test result: ok. 16 passed; …`

- [ ] **Step 6: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-core --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-core/src/broker/sqlite.rs crates/cabby-core/src/broker/sqlite/loader.rs
git commit -m "broker::sqlite: the loader

Lock, open and check (a corrupt file is moved aside, other errors stop startup),
epoch kept only after a clean shutdown, streams loaded as their contiguous tail
anchored at max_seq within TTL and limit, sessions with their original expiry, and
the startup line.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: `SqliteBroker`

Wires the pieces into `cabby_core::broker::Broker` (`broker.rs:144-172`), the way `broker::Nats` (`broker/nats.rs:393-1060`) wraps a node-local `Memory`: reads, presence and fan-out go to the memory broker; `handle_broadcast`, `commit_session` and `touch_session` also enqueue writes. The `Broker` trait is synchronous and none of this blocks: `open` loads the file once at startup, the persister writes on its own thread, and `shutdown` waits only for the final flush.

**Files:**
- Modify: `crates/cabby-core/src/broker/sqlite.rs`
- Modify: `crates/cabby-core/src/broker.rs` (the `mod sqlite;` lines added in Task 6)

- [ ] **Step 1: Write the failing tests**

Inside `pub(crate) mod testing { … }` in `sqlite.rs`, add:

```rust
    /// Session state for `commit_session`.
    pub(crate) struct Bytes(pub(crate) Vec<u8>);

    impl crate::broker::Cacheable for Bytes {
        fn to_cache_entry(&self) -> Result<Vec<u8>, crate::broker::BrokerError> {
            Ok(self.0.clone())
        }
    }
```

At the end of `sqlite.rs`, add:

```rust
#[cfg(test)]
mod tests {
    use super::testing::{Bytes, Recording, TempDir};
    use super::*;
    use crate::broker::test_clock::{T0, TestClock};
    use crate::common::StreamMessageMetadata;

    fn msg(stream: &str, data: &str, transient: bool) -> StreamMessage {
        StreamMessage {
            stream: stream.into(),
            data: data.into(),
            meta: transient.then(|| StreamMessageMetadata { transient: true, ..Default::default() }),
            ..Default::default()
        }
    }

    fn open(dir: &TempDir, clock: &Arc<TestClock>) -> SqliteBroker {
        SqliteBroker::open(None, None, &Config::new(), &SqliteOptions::new(dir.db()), clock.clone(), Arc::new(NoopObserver))
            .unwrap()
    }

    fn history(broker: &SqliteBroker, stream: &str) -> Vec<(u64, String)> {
        broker.history_since(stream, 0).unwrap().unwrap_or_default().into_iter().map(|m| (m.offset, m.data)).collect()
    }

    #[test]
    fn a_graceful_restart_restores_history_offsets_sessions_and_the_epoch() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        let epoch = broker.epoch();
        for data in ["a", "b", "c"] {
            broker.handle_broadcast(&msg("s", data, false)).unwrap();
        }
        broker.handle_broadcast(&msg("s", "transient", true)).unwrap();
        broker.commit_session("sid", &Bytes(b"state".to_vec())).unwrap();
        assert!(broker.shutdown_within(Duration::from_secs(5)), "marked clean");
        assert!(!files::sibling(&dir.db(), "-wal").exists(), "no WAL left after a graceful shutdown");
        drop(broker);

        let broker = open(&dir, &clock);
        assert_eq!(epoch, broker.epoch());
        assert_eq!(Previous::Clean, broker.report().previous);
        let expected: Vec<(u64, String)> = vec![(1, "a".into()), (2, "b".into()), (3, "c".into())];
        assert_eq!(expected, history(&broker, "s"), "transient broadcasts are not persisted");
        assert_eq!(2, broker.history_from("s", &epoch, 1).unwrap().len(), "resumable by offset");
        assert_eq!(Some(b"state".to_vec()), broker.restore_session("sid").unwrap());
        broker.start().unwrap();
        broker.handle_broadcast(&msg("s", "d", false)).unwrap();
        assert_eq!(Some(4), broker.peak("s").unwrap().map(|m| m.offset));
        assert!(broker.shutdown_within(Duration::from_secs(5)));
    }

    #[test]
    fn a_session_touch_reaches_the_file_with_its_new_expiry() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        broker.commit_session("sid", &Bytes(b"state".to_vec())).unwrap();
        clock.advance(Duration::from_secs(5));
        broker.touch_session("sid").unwrap();
        broker.touch_session("unknown").unwrap();
        assert!(broker.shutdown_within(Duration::from_secs(5)));
        drop(broker);

        let conn = store::open(&dir.db()).unwrap();
        let expiry: i64 =
            conn.query_row("SELECT expires_at_ms FROM sessions WHERE sid = 'sid'", [], |r| r.get(0)).unwrap();
        assert_eq!((T0 + 5) * 1000 + 300_000, expiry);
        let rows: i64 = conn.query_row("SELECT count(*) FROM sessions", [], |r| r.get(0)).unwrap();
        assert_eq!(1, rows, "a touch for an unknown session writes nothing");
    }

    #[test]
    fn dropping_the_broker_without_shutdown_rotates_the_epoch() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.start().unwrap();
        let epoch = broker.epoch();
        drop(broker);

        // Waits (up to LOCK_TIMEOUT) for the old writer to release the lock.
        let broker = open(&dir, &clock);
        assert_ne!(epoch, broker.epoch());
        assert_eq!(Previous::Unclean, broker.report().previous);
    }

    #[test]
    fn a_shutdown_without_start_still_writes_and_marks_it_clean() {
        let dir = TempDir::new();
        let clock = TestClock::new();
        let broker = open(&dir, &clock);
        broker.handle_broadcast(&msg("s", "a", false)).unwrap();

        assert!(broker.shutdown_within(Duration::from_secs(5)));
        assert!(!broker.shutdown_within(Duration::from_secs(5)), "a second shutdown does nothing");
        drop(broker);

        let broker = open(&dir, &clock);
        assert_eq!(Previous::Clean, broker.report().previous);
        assert_eq!(vec![(1, "a".to_owned())], history(&broker, "s"));
    }

    #[test]
    fn queue_overflow_is_reported_to_the_observer() {
        let dir = TempDir::new();
        let observer = Arc::new(Recording::default());
        let mut options = SqliteOptions::new(dir.db());
        options.queue_capacity = 2;
        let broker =
            SqliteBroker::open(None, None, &Config::new(), &options, TestClock::new(), observer.clone()).unwrap();

        // Not started: nothing drains the queue.
        broker.commit_session("a", &Bytes(b"a".to_vec())).unwrap();
        broker.handle_broadcast(&msg("s", "x", false)).unwrap();
        broker.handle_broadcast(&msg("s", "y", false)).unwrap();
        broker.touch_session("a").unwrap();

        assert_eq!(vec![OpKind::History, OpKind::History], observer.dropped());
        assert_eq!(2, broker.history_since("s", 0).unwrap().unwrap_or_default().len(), "live history is unaffected");
    }

    #[test]
    fn announce_names_the_file_and_the_settings() {
        let dir = TempDir::new();
        let broker = open(&dir, &TestClock::new());
        assert_eq!(
            format!(
                "Using SQLite broker: {} (epoch: {}, history limit: 100, history ttl: 300s, sessions ttl: 300s, presence ttl: 15s)",
                dir.db().display(),
                broker.epoch()
            ),
            broker.announce()
        );
        assert!(broker.supports_presence());
    }
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite::tests 2>&1 | grep -E "^error" | head -2`
Expected: `error[E0433]: failed to resolve: use of undeclared type \`SqliteBroker\`` (or `cannot find type \`SqliteBroker\``).

- [ ] **Step 3: Implement**

In `sqlite.rs`, replace the `use` lines at the top with:

```rust
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::Value;

use super::{Broadcaster, Broker, BrokerError, Cacheable, Clock, Config, Memory, PresenceInfoOptions, Presenter, lock};
use crate::common::{PresenceEvent, PresenceInfo, RemoteCommandMessage, StreamMessage};
use crate::log_warn;
use crate::logger::Logger;
use persister::{Persister, Timing};
use queue::{WriteOp, WriteQueue};

pub use loader::{LoadReport, Previous};
pub use queue::OpKind;
```

In `SqliteObserver` and `NoopObserver`, change `queue::OpKind` to `OpKind`. After `SqliteOptions`'s impl, add:

```rust
/// What `Broker::shutdown` gives the final write when no runner deadline
/// applies (the default `shutdown_timeout`).
pub const SHUTDOWN_BUDGET: Duration = Duration::from_secs(30);
/// How long `shutdown_within` waits past its budget for the writer to close
/// the file.
const FINISH_GRACE: Duration = Duration::from_secs(1);

/// The memory broker plus a write-behind SQLite copy (see the module docs).
pub struct SqliteBroker {
    memory: Memory,
    config: Config,
    path: PathBuf,
    queue: Arc<WriteQueue>,
    observer: Arc<dyn SqliteObserver>,
    report: LoadReport,
    writer: Mutex<Writer>,
}

enum Writer {
    /// Loaded; the thread is not started yet.
    Ready(Box<Persister>),
    /// The thread runs and reports whether its final flush marked the shutdown clean.
    Running(Receiver<bool>),
    Stopped,
}

impl SqliteBroker {
    /// Opens (creating if needed) the database at `options.path`, takes its
    /// lock and loads it into a new memory broker. Errors name the path:
    /// another process holds it, it can't be created or read, or its schema
    /// is newer. A corrupt file is moved aside instead (spec §6).
    pub fn open(
        broadcaster: Option<Arc<dyn Broadcaster>>,
        presenter: Option<Arc<dyn Presenter>>,
        config: &Config,
        options: &SqliteOptions,
        clock: Arc<dyn Clock>,
        observer: Arc<dyn SqliteObserver>,
    ) -> Result<Self, BrokerError> {
        let memory = Memory::new(broadcaster, presenter, config, clock.clone());
        let loaded = loader::load(&memory, config, options, clock.as_ref())?;
        let queue = Arc::new(WriteQueue::new(options.queue_capacity, persister::BATCH_SIZE));
        let persister = Persister::new(loaded.db, queue.clone(), config, clock, observer.clone(), Timing::standard());
        Ok(Self {
            memory,
            config: config.clone(),
            path: options.path.clone(),
            queue,
            observer,
            report: loaded.report,
            writer: Mutex::new(Writer::Ready(Box::new(persister))),
        })
    }

    /// What `open` restored.
    pub fn report(&self) -> &LoadReport {
        &self.report
    }

    pub fn epoch(&self) -> String {
        self.memory.epoch()
    }

    /// Stops the broker: the writer writes everything still queued and marks
    /// the shutdown clean if that finishes within `budget`, then checkpoints,
    /// closes the database and releases the lock. Returns whether the shutdown
    /// was marked clean (false for a second call).
    pub fn shutdown_within(&self, budget: Duration) -> bool {
        let _ = Broker::shutdown(&self.memory);
        let deadline = Instant::now() + budget;
        self.queue.close(deadline);
        match std::mem::replace(&mut *lock(&self.writer), Writer::Stopped) {
            Writer::Stopped => false,
            Writer::Ready(persister) => persister.finish(deadline),
            Writer::Running(done) => match done.recv_timeout(budget + FINISH_GRACE) {
                Ok(clean) => clean,
                Err(_) => {
                    log_warn!(log(), &format!("the SQLite writer did not finish within {} s; the next start uses a new epoch",
                        budget.as_secs()), "path" => self.path.display().to_string());
                    false
                }
            },
        }
    }

    fn enqueue(&self, op: WriteOp) {
        if let Some(kind) = self.queue.push(op) {
            self.observer.dropped(kind);
        }
    }
}

impl Drop for SqliteBroker {
    /// Without `shutdown` (a failed startup, a test), the writer stops at once,
    /// as after a crash: nothing more is written and the next start rotates
    /// the epoch.
    fn drop(&mut self) {
        self.queue.close(Instant::now());
    }
}

impl Broker for SqliteBroker {
    /// Starts the memory broker's expiration loop and the writer thread.
    fn start(&self) -> Result<(), BrokerError> {
        let mut writer = lock(&self.writer);
        let persister = match std::mem::replace(&mut *writer, Writer::Stopped) {
            Writer::Ready(persister) => persister,
            other => {
                *writer = other;
                return Ok(());
            }
        };
        Broker::start(&self.memory)?;
        let (done, finished) = mpsc::channel();
        std::thread::Builder::new()
            .name("cabby-sqlite".into())
            .spawn(move || {
                let _ = done.send(persister.run());
            })
            .map_err(|e| BrokerError { message: format!("failed to start the SQLite writer: {e}") })?;
        *writer = Writer::Running(finished);
        Ok(())
    }

    fn shutdown(&self) -> Result<(), BrokerError> {
        self.shutdown_within(SHUTDOWN_BUDGET);
        Ok(())
    }

    fn announce(&self) -> String {
        let c = &self.config;
        format!(
            "Using SQLite broker: {} (epoch: {}, history limit: {}, history ttl: {}s, sessions ttl: {}s, presence ttl: {}s)",
            self.path.display(),
            self.memory.epoch(),
            c.history_limit,
            c.history_ttl,
            c.sessions_ttl,
            c.presence_ttl,
        )
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), BrokerError> {
        if let Some(recorded) = self.memory.handle_broadcast_recorded(msg)? {
            self.enqueue(WriteOp::History {
                stream: msg.stream.clone(),
                offset: recorded.offset,
                ts_secs: recorded.timestamp,
                data: msg.data.clone(),
            });
        }
        Ok(())
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), BrokerError> {
        self.memory.handle_command(msg)
    }

    fn subscribe(&self, stream: &str) -> String {
        self.memory.subscribe(stream)
    }

    fn unsubscribe(&self, stream: &str) -> String {
        self.memory.unsubscribe(stream)
    }

    fn history_from(&self, stream: &str, epoch: &str, offset: u64) -> Result<Vec<StreamMessage>, BrokerError> {
        self.memory.history_from(stream, epoch, offset)
    }

    fn history_since(&self, stream: &str, ts: i64) -> Result<Option<Vec<StreamMessage>>, BrokerError> {
        self.memory.history_since(stream, ts)
    }

    fn commit_session(&self, sid: &str, session: &dyn Cacheable) -> Result<(), BrokerError> {
        let data = session.to_cache_entry()?;
        let expires_at_ms = self.memory.commit_session_data(sid, data.clone());
        self.enqueue(WriteOp::SessionCommit { sid: sid.to_owned(), data, expires_at_ms });
        Ok(())
    }

    fn restore_session(&self, sid: &str) -> Result<Option<Vec<u8>>, BrokerError> {
        self.memory.restore_session(sid)
    }

    fn touch_session(&self, sid: &str) -> Result<(), BrokerError> {
        if let Some(expires_at_ms) = self.memory.touch_session_deadline(sid) {
            self.enqueue(WriteOp::SessionTouch { sid: sid.to_owned(), expires_at_ms });
        }
        Ok(())
    }

    /// Presence lives in memory only: every connection drops on restart (spec §2).
    fn supports_presence(&self) -> bool {
        self.memory.supports_presence()
    }

    fn presence_add(
        &self,
        stream: &str,
        sid: &str,
        pid: &str,
        info: Value,
    ) -> Result<Option<PresenceEvent>, BrokerError> {
        self.memory.presence_add(stream, sid, pid, info)
    }

    fn presence_remove(&self, stream: &str, sid: &str) -> Result<Option<PresenceEvent>, BrokerError> {
        self.memory.presence_remove(stream, sid)
    }

    fn presence_info(&self, stream: &str, opts: Option<&PresenceInfoOptions>) -> Result<PresenceInfo, BrokerError> {
        self.memory.presence_info(stream, opts)
    }

    fn touch_presence(&self, sid: &str) -> Result<(), BrokerError> {
        self.memory.touch_presence(sid)
    }

    fn peak(&self, stream: &str) -> Result<Option<StreamMessage>, BrokerError> {
        self.memory.peak(stream)
    }
}
```

In `crates/cabby-core/src/broker.rs`, replace the lines added in Task 6 (the two comment lines, `#[allow(dead_code)]` and `mod sqlite;`) with:

```rust

mod sqlite;

pub use sqlite::{
    LOCK_TIMEOUT, LoadReport, NoopObserver, OpKind, Previous, QUEUE_CAPACITY, SHUTDOWN_BUDGET, SqliteBroker,
    SqliteObserver, SqliteOptions,
};
```


- [ ] **Step 4: Run the tests**

Run: `mise exec -- cargo test -p cabby-core --lib broker::sqlite 2>&1 | grep "^test result"`
Expected: `test result: ok. 68 passed; …` (units 4, store 17, queue 8, files 7, persister 10, loader 16, broker 6). If the number differs, list them with `mise exec -- cargo test -p cabby-core --lib broker::sqlite -- --list` and compare with the tasks.

Run the whole crate: `mise exec -- cargo test -p cabby-core --lib 2>&1 | grep "^test result"`
Expected: `test result: ok. 152 passed; …` (75 baseline + 9 memory tests + 68 sqlite tests).

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy --workspace --all-targets -- -D warnings 2>&1 | grep -E "^(error|warning)" -A3
git add crates/cabby-core/src/broker.rs crates/cabby-core/src/broker/sqlite.rs
git commit -m "broker: SqliteBroker

Memory as the live path, broadcasts, commits and touches enqueued for the persister
thread, presence in memory only; shutdown_within gives the final write a budget,
dropping without shutdown behaves like a crash.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected from the clippy command: no output, or only the known `crates/cabby-testkit/src/reference/benchi.rs:514` baseline error (Ground rules).

---

### Task 15: Configuration: `[sqlite] path` and the preset precedence

`Config` (`crates/cabby-server/src/config.rs:125-176`) composes one type per TOML table. `[sqlite]` gets its own small type, like `[metrics]`. `load_broker_preset` (`config.rs:509-534`) only sets the broker when it is still the default (`""`), so an explicit `--broker=sqlite` survives `--presets=broker`; a test pins that (§2).

**Files:**
- Modify: `crates/cabby-server/src/config.rs` (module docs lines 3-6; after the `MetricsConfig` conversions, line 117; `Config` lines 125-176; `toml_section!` lines 180-215; `to_toml` sections lines 333-347; `mod tests` lines 548-580)

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `crates/cabby-server/src/config.rs`:

```rust
    #[test]
    fn sqlite_path_comes_from_the_sqlite_section() {
        let mut c = Config::new();
        let unknown = toml_config::decode(&mut c, "[sqlite]\npath = \"/var/lib/cabby/cabby.db\"\n").unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!("/var/lib/cabby/cabby.db", c.sqlite.path);
    }

    #[test]
    fn printed_configuration_has_a_sqlite_section_that_reads_back() {
        assert!(Config::new().to_toml().contains(
            "# SQLite broker configuration (--broker=sqlite)\n[sqlite]\n\
             # Database file for --broker=sqlite (created with mode 0600 in a 0700 directory)\n\
             # path = \"/var/lib/cabby/cabby.db\"\n\n"
        ));

        let mut c = Config::new();
        c.sqlite.path = "/data/cabby.db".into();
        let mut back = Config::new();
        let unknown = toml_config::decode(&mut back, &c.to_toml()).unwrap();
        assert!(unknown.is_empty(), "{unknown:?}");
        assert_eq!("/data/cabby.db", back.sqlite.path);
    }

    /// Pins spec §2: `--presets=broker` only fills in a default broker.
    #[test]
    fn broker_preset_keeps_an_explicit_sqlite_broker() {
        let mut c = Config::new();
        c.user_presets = Some(vec!["broker".to_owned()]);
        c.broker.adapter = "sqlite".to_owned();
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("sqlite", c.broker.adapter);

        let mut d = Config::new();
        d.user_presets = Some(vec!["broker".to_owned()]);
        d.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("memory", d.broker.adapter, "the preset still picks a broker when none was chosen");
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-server --lib config::tests 2>&1 | grep -E "^error" | head -2`
Expected: `error[E0609]: no field \`sqlite\` on type \`config::Config\`` (or `…on type \`Config\``).

- [ ] **Step 3: Implement**

In the module docs (lines 3-6), replace "The only section with a type of its own here is `[metrics]`" with "The sections with a type of their own here are `[metrics]` and `[sqlite]`", keeping the rest of the sentence about the metrics tags.

After `impl From<&MetricsConfig> for crate::metrics::Config { … }` (ends at line 117), add:

```rust
/// `[sqlite]`: the SQLite broker's database (`--sqlite_path`,
/// `CABBY_SQLITE_PATH`). A Cabby-only section (deviation D-065), so it has no
/// `ANYCABLE_*` alias.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SqliteConfig {
    /// The database file; required with `broker.adapter = "sqlite"`.
    pub path: String,
}

cabby_core::toml_section!(SqliteConfig, "sqlite.Config" {
    "path" => path,
});

impl SqliteConfig {
    /// Renders the annotated TOML section.
    pub fn to_toml(&self) -> String {
        let mut out =
            String::from("# Database file for --broker=sqlite (created with mode 0600 in a 0700 directory)\n");
        if self.path.is_empty() {
            out.push_str("# path = \"/var/lib/cabby/cabby.db\"\n");
        } else {
            out.push_str(&format!("path = {}\n", toml_config::go_quote(&self.path)));
        }
        out.push('\n');
        out
    }
}
```

In `pub struct Config`, after `pub broker: BrokerConfig,`, add:

```rust
    /// `[sqlite]` (Cabby only, D-065).
    pub sqlite: SqliteConfig,
```

In `cabby_core::toml_section!(Config, "config.Config" { … })`, after `"broker" => broker,`, add:

```rust
    "sqlite" => sqlite,
```

In `to_toml`, change `let sections: [(&str, String); 13] = [` to `let sections: [(&str, String); 14] = [`, and after the `("# Broker configuration\n[broker]\n", self.broker.to_toml()),` entry add:

```rust
            ("# SQLite broker configuration (--broker=sqlite)\n[sqlite]\n", self.sqlite.to_toml()),
```

`Config::new` needs no change: `..Default::default()` fills `sqlite`.

- [ ] **Step 4: Run the tests, including the translated config round trips**

Run: `mise exec -- cargo test -p cabby-server --lib config::tests 2>&1 | grep "^test result"`
Expected: `test result: ok. 5 passed; …`

Run: `mise exec -- cargo xtask test --backend rust config_test 2>&1 | grep -E "^  tests"` and `mise exec -- cargo xtask test --backend rust test_unknown_config_keys 2>&1 | grep -E "^  tests"`
Expected: only `pass` (the uncommented `path = …` line of `[sqlite]` is a known key, so `TestToTomlRoundTrip/with_commented_out_settings_enabled` and `TestUnknownConfigKeys/printed_configuration_has_no_unknown_keys` keep passing).

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-server --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-server/src/config.rs
git commit -m "config: [sqlite] path

A Cabby-only section for the SQLite broker's database file, printed by
--print-config; pins that --presets=broker keeps an explicit --broker=sqlite.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 16: CLI: `--sqlite_path` and `CABBY_SQLITE_PATH`

Flags follow `cli/options.rs`: `with_defaults` gives each flag its help category and, unless set, the `ANYCABLE_<NAME>` variable (`options.rs:518-530`); `with_env` replaces that list. `--sqlite_path` gets `CABBY_SQLITE_PATH` only. Precedence is the usual one (`options.rs:4-13`): defaults, the file, then the environment, then the command line.

**Files:**
- Modify: `crates/cabby-server/src/cli/options.rs` (category constants, lines 25-47; `definitions`, line 561; after `broker_flags`, line 717; `mod tests`)

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `crates/cabby-server/src/cli/options.rs`:

```rust
    #[test]
    fn sqlite_path_from_the_flag_and_the_cabby_env_var() {
        let (res, _) = run(&["--broker=sqlite", "--sqlite_path=/data/a.db"], &[]);
        let c = res.unwrap().0;
        assert_eq!(("sqlite", "/data/a.db"), (c.broker.adapter.as_str(), c.sqlite.path.as_str()));

        let (res, _) = run(&[], &[("CABBY_SQLITE_PATH", "/data/b.db")]);
        assert_eq!("/data/b.db", res.unwrap().0.sqlite.path);

        let (res, _) = run(&["--sqlite_path=/data/c.db"], &[("CABBY_SQLITE_PATH", "/data/b.db")]);
        assert_eq!("/data/c.db", res.unwrap().0.sqlite.path, "the command line wins");

        let (res, _) = run(&[], &[("ANYCABLE_SQLITE_PATH", "/data/d.db")]);
        assert_eq!("", res.unwrap().0.sqlite.path, "no ANYCABLE_* alias");
    }

    #[test]
    fn sqlite_path_from_the_config_file_unless_overridden() {
        let file = std::env::temp_dir().join(format!("cabby-sqlite-options-{}.toml", std::process::id()));
        std::fs::write(&file, "[sqlite]\npath = \"/data/file.db\"\n").unwrap();
        let path = file.to_str().unwrap();
        let mut out = Vec::new();

        let (c, _) = new_config_from_cli_to(&["cabby", "--config-path", path], &EnvMap::new(), vec![], &mut out).unwrap();
        assert_eq!("/data/file.db", c.sqlite.path);

        let args = ["cabby", "--config-path", path, "--sqlite_path=/data/flag.db"];
        let (c, _) = new_config_from_cli_to(&args, &EnvMap::new(), vec![], &mut out).unwrap();
        assert_eq!("/data/flag.db", c.sqlite.path);
        std::fs::remove_file(&file).unwrap();
    }

    #[test]
    fn sqlite_path_shows_in_help_with_its_env_var() {
        let (_, out) = run(&["--help"], &[]);
        assert!(out.contains("SQLITE:"), "{out}");
        assert!(out.contains("--sqlite_path value") && out.contains("[$CABBY_SQLITE_PATH]"), "{out}");
    }

    #[test]
    fn explicit_sqlite_broker_survives_the_broker_preset() {
        let (res, _) = run(&["--presets=broker", "--broker=sqlite", "--sqlite_path=/data/cabby.db"], &[]);
        let mut c = res.unwrap().0;
        assert_eq!(Some(vec!["broker".to_owned()]), c.user_presets);
        c.load_presets(&EnvMap::new()).unwrap();
        assert_eq!("sqlite", c.broker.adapter);
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-server --lib cli::options::tests::sqlite 2>&1 | grep -E "^test result|panicked" | head -4`
Expected: `test result: FAILED. 0 passed; 3 failed; …` with `flag provided but not defined: -sqlite_path` in the panics (`explicit_sqlite_broker_survives_the_broker_preset` is filtered out by the name; it fails the same way).

- [ ] **Step 3: Implement**

After `const API: &str = "API:";` (line 47), add:

```rust
const SQLITE: &str = "SQLITE:";
```

In `definitions`, after `flags.extend(broker_flags(c));` (line 561), add:

```rust
    flags.extend(sqlite_flags(c));
```

After `fn broker_flags(c: &Config) -> Vec<Flag> { … }` (ends at line 717), add:

```rust
/// The SQLite broker (Cabby only, D-065): `CABBY_SQLITE_PATH`, no `ANYCABLE_*` alias.
fn sqlite_flags(c: &Config) -> Vec<Flag> {
    with_defaults(
        SQLITE,
        vec![with_env(
            str_flag("sqlite_path", &c.sqlite.path, "Database file for --broker=sqlite (Cabby only)", |s, v| {
                s.c.sqlite.path = v
            }),
            &["CABBY_SQLITE_PATH"],
        )],
    )
}
```

- [ ] **Step 4: Run the tests**

Run: `mise exec -- cargo test -p cabby-server --lib cli::options::tests 2>&1 | grep "^test result"`
Expected: `ok`, with 4 more tests than before.

Run: `mise exec -- cargo xtask test --backend rust options_test 2>&1 | grep -E "^  tests"` and `mise exec -- cargo xtask test --backend rust cli_test 2>&1 | grep -E "^  tests"`
Expected: only `pass`.

- [ ] **Step 5: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-server --all-targets -- -D warnings 2>&1 | tail -1
git add crates/cabby-server/src/cli/options.rs
git commit -m "cli: --sqlite_path and CABBY_SQLITE_PATH

Cabby-only flag in its own SQLITE: help category, without an ANYCABLE_* alias.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 17: Runner wiring, startup validation, metrics, shutdown budget

The runner builds the default broker in `new_broker` (`cli/runner.rs:680-716`) and pushes it into the shutdown order after `node.shutdown` and the broadcasters (`runner.rs:539-548`: ws server, `node.shutdown`, broadcasters, broker, subscriber). Clients disconnecting in `node.shutdown` touch their sessions (`node/app.rs:1173-1176`), so those writes are queued before the broker's shutdown runs; Task 20 pins that through the binary. `CoreBroker` (`cli/adapters.rs:219-245`) calls `Broker::shutdown`, which has no deadline; the SQLite broker gets its own adapter that passes what is left of the shutdown context (`adapters.rs:80-106`, `ShutdownContext`, `remaining`), so the final write is bounded by `--shutdown_timeout` (§5 step 2). **Owner decision:** the final write gets whatever is left of the shutdown context, but never less than **1 second** (`FINAL_FLUSH_FLOOR = Duration::from_secs(1)`; use `remaining.max(FINAL_FLUSH_FLOOR)`). If it still doesn't finish, `clean_shutdown` stays `'0'` and the next start rotates the epoch, which is the designed fallback. Add a unit test that a zero remaining budget still allows a 1 s flush.

**Files:**
- Create: `crates/cabby-server/src/cli/sqlite.rs`
- Modify: `crates/cabby-server/src/cli.rs` (`mod sqlite;` after `mod signals;`, line 14)
- Modify: `crates/cabby-server/src/cli/runner.rs` (`new_broker`, lines 680-716)

- [ ] **Step 1: Write the module with failing tests**

Add `mod sqlite;` after `mod signals;` in `crates/cabby-server/src/cli.rs`. Create `crates/cabby-server/src/cli/sqlite.rs`:

```rust
//! The SQLite broker in the runner (Cabby only, deviation D-065): startup
//! validation, its metrics (spec §7), and a shutdown step that gives the final
//! write what is left of the shutdown timeout.

use std::sync::Arc;
use std::time::Duration;

use cabby_core::broker::{self as core_broker, OpKind, SqliteBroker, SqliteObserver, SqliteOptions};
use cabby_core::common::{RemoteCommandMessage, StreamMessage};

use super::adapters::{ShutdownContext, remaining};
use super::{Broker, CliError, Shutdownable};
use crate::config::Config;
use crate::metrics::Instrumenter;

pub(super) const QUEUE_SIZE: &str = "sqlite_queue_size";
pub(super) const BATCH_WRITE_MS: &str = "sqlite_batch_write_ms";
pub(super) const WRITE_ERRORS: &str = "sqlite_write_errors_total";
pub(super) const DROPPED: &str = "sqlite_dropped_total";
pub(super) const DROPPED_HISTORY: &str = "sqlite_dropped_history_total";
pub(super) const DROPPED_SESSION_TOUCH: &str = "sqlite_dropped_session_touch_total";
pub(super) const DROPPED_SESSION_COMMIT: &str = "sqlite_dropped_session_commit_total";

/// The broker's options from the configuration; the path is required.
pub(super) fn sqlite_options(c: &Config) -> Result<SqliteOptions, CliError> {
    todo!()
}

/// Reports the broker's write path as metrics. The registry has no labels, so
/// drops are counted in total and per kind.
pub(super) struct MetricsObserver(Arc<dyn Instrumenter>);

impl MetricsObserver {
    pub(super) fn new(metrics: Arc<dyn Instrumenter>) -> Self {
        todo!()
    }
}

impl SqliteObserver for MetricsObserver {
    fn queue_size(&self, ops: usize) {
        todo!()
    }

    fn batch_written(&self, took: Duration) {
        todo!()
    }

    fn write_failed(&self) {
        todo!()
    }

    fn dropped(&self, kind: OpKind) {
        todo!()
    }
}

/// The runner's view of the SQLite broker: its shutdown step gives the final
/// write the time left of the shutdown context.
pub(super) struct SqliteRunnerBroker {
    pub(super) broker: Arc<SqliteBroker>,
    pub(super) ctx: ShutdownContext,
}

impl Shutdownable for SqliteRunnerBroker {
    fn shutdown(&self) -> Result<(), CliError> {
        self.broker.shutdown_within(remaining(&self.ctx.get()));
        Ok(())
    }
}

impl Broker for SqliteRunnerBroker {
    fn start(&self) -> Result<(), CliError> {
        core_broker::Broker::start(self.broker.as_ref()).map_err(|e| CliError(e.message))
    }

    fn announce(&self) -> String {
        core_broker::Broker::announce(self.broker.as_ref())
    }

    fn handle_broadcast(&self, msg: &StreamMessage) -> Result<(), CliError> {
        core_broker::Broker::handle_broadcast(self.broker.as_ref(), msg).map_err(|e| CliError(e.message))
    }

    fn handle_command(&self, msg: &RemoteCommandMessage) -> Result<(), CliError> {
        core_broker::Broker::handle_command(self.broker.as_ref(), msg).map_err(|e| CliError(e.message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;

    #[test]
    fn the_sqlite_broker_needs_a_path() {
        let mut c = Config::new();
        c.broker.adapter = "sqlite".into();
        assert_eq!(
            "--broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path",
            sqlite_options(&c).unwrap_err().0
        );
        c.sqlite.path = "  ".into();
        assert!(sqlite_options(&c).is_err(), "blank is missing");

        c.sqlite.path = "/data/cabby.db".into();
        let options = sqlite_options(&c).unwrap();
        assert_eq!(std::path::PathBuf::from("/data/cabby.db"), options.path);
        assert_eq!(core_broker::LOCK_TIMEOUT, options.lock_timeout);
    }

    #[test]
    fn metrics_observer_registers_and_reports() {
        let m = Arc::new(Metrics::new(Vec::new(), 0));
        let observer = MetricsObserver::new(m.clone());

        observer.queue_size(7);
        observer.batch_written(Duration::from_millis(12));
        observer.write_failed();
        observer.dropped(OpKind::History);
        observer.dropped(OpKind::SessionCommit);

        assert_eq!(7, m.gauge(QUEUE_SIZE).unwrap().value());
        assert_eq!(12, m.gauge(BATCH_WRITE_MS).unwrap().value());
        assert_eq!(1, m.counter(WRITE_ERRORS).unwrap().value());
        assert_eq!(2, m.counter(DROPPED).unwrap().value());
        assert_eq!(1, m.counter(DROPPED_HISTORY).unwrap().value());
        assert_eq!(0, m.counter(DROPPED_SESSION_TOUCH).unwrap().value());
        assert_eq!(1, m.counter(DROPPED_SESSION_COMMIT).unwrap().value());
        assert!(m.prometheus().contains("anycable_go_sqlite_queue_size 7"), "{}", m.prometheus());
    }
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `mise exec -- cargo test -p cabby-server --lib cli::sqlite 2>&1 | grep "^test result"`
Expected: `test result: FAILED. 0 passed; 2 failed; …` (`not yet implemented`).

- [ ] **Step 3: Implement the module**

Replace the `todo!()` bodies:

```rust
pub(super) fn sqlite_options(c: &Config) -> Result<SqliteOptions, CliError> {
    if c.sqlite.path.trim().is_empty() {
        return Err(CliError(
            "--broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path".into(),
        ));
    }
    Ok(SqliteOptions::new(&c.sqlite.path))
}
```

```rust
    pub(super) fn new(metrics: Arc<dyn Instrumenter>) -> Self {
        metrics.register_gauge(QUEUE_SIZE, "The number of SQLite broker writes waiting to be persisted");
        metrics.register_gauge(BATCH_WRITE_MS, "The duration of the last SQLite broker batch write (milliseconds)");
        metrics.register_counter(WRITE_ERRORS, "The total number of failed SQLite broker batch writes");
        metrics.register_counter(DROPPED, "The total number of SQLite broker writes dropped on queue overflow");
        metrics.register_counter(DROPPED_HISTORY, "The total number of history writes dropped on queue overflow");
        metrics
            .register_counter(DROPPED_SESSION_TOUCH, "The total number of session touches dropped on queue overflow");
        metrics
            .register_counter(DROPPED_SESSION_COMMIT, "The total number of session commits dropped on queue overflow");
        Self(metrics)
    }
```

```rust
    fn queue_size(&self, ops: usize) {
        self.0.gauge_set(QUEUE_SIZE, u64::try_from(ops).unwrap_or(u64::MAX));
    }

    fn batch_written(&self, took: Duration) {
        self.0.gauge_set(BATCH_WRITE_MS, u64::try_from(took.as_millis()).unwrap_or(u64::MAX));
    }

    fn write_failed(&self) {
        self.0.counter_increment(WRITE_ERRORS);
    }

    fn dropped(&self, kind: OpKind) {
        self.0.counter_increment(DROPPED);
        self.0.counter_increment(match kind {
            OpKind::History => DROPPED_HISTORY,
            OpKind::SessionTouch => DROPPED_SESSION_TOUCH,
            OpKind::SessionCommit => DROPPED_SESSION_COMMIT,
        });
    }
```

- [ ] **Step 4: Run the module's tests**

Run: `mise exec -- cargo test -p cabby-server --lib cli::sqlite 2>&1 | grep "^test result"`
Expected: `test result: ok. 2 passed; …`

- [ ] **Step 5: Wire it into the runner**

In `crates/cabby-server/src/cli/runner.rs`, in `fn new_broker`, at the start of the `Some(BrokerSource::Default) | None => {` arm (line 690), before `// Upstream \`WithDefaultBroker\`.`, add:

```rust
                if self.config.broker.adapter == "sqlite" {
                    return self.new_sqlite_broker(node, subscriber.broadcaster.clone());
                }
```

After `fn new_broker` (ends at line 716), add:

```rust
    /// `--broker=sqlite` (Cabby only, D-065): the memory broker with a
    /// write-behind SQLite copy, loaded before the server accepts connections.
    fn new_sqlite_broker(
        &self,
        node: &Arc<Node>,
        broadcaster: Arc<dyn core_broker::Broadcaster>,
    ) -> Result<BrokerPair, CliError> {
        let options = super::sqlite::sqlite_options(&self.config)?;
        let observer = Arc::new(super::sqlite::MetricsObserver::new(self.metrics.clone() as Arc<dyn Instrumenter>));
        let broker = Arc::new(
            core_broker::SqliteBroker::open(
                Some(broadcaster),
                Some(node.clone() as Arc<dyn core_broker::Presenter>),
                &self.config.broker,
                &options,
                Arc::new(core_broker::SystemClock),
                observer,
            )
            .map_err(|e| CliError(e.message))?,
        );
        let runner: Arc<dyn Broker> =
            Arc::new(super::sqlite::SqliteRunnerBroker { broker: broker.clone(), ctx: self.shutdown_ctx.clone() });
        let node_broker: Arc<dyn core_broker::Broker> = broker;
        Ok((runner, node_broker))
    }
```

Update the doc comment of `RunnerOption::DefaultBroker` in `crates/cabby-server/src/cli.rs` from "legacy, memory or NATS broker from the configuration" to "legacy, memory, NATS or SQLite broker from the configuration".

- [ ] **Step 6: Smoke-test the binary**

```sh
mise exec -- cargo build -p cabby-server --bin cabby
rm -rf /tmp/cabby-smoke && mkdir -p /tmp/cabby-smoke
for run in 1 2; do
  ANYCABLE_DISABLE_TELEMETRY=true target/debug/cabby --public --broadcast_adapter=http --broker=sqlite \
    --sqlite_path=/tmp/cabby-smoke/data/cabby.db --port=18080 --http_broadcast_port=18090 \
    > "/tmp/cabby-smoke/run${run}.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 100); do grep -q "Handle WebSocket connections" "/tmp/cabby-smoke/run${run}.log" && break; sleep 0.1; done
  kill -TERM "${pid}"; wait "${pid}"; echo "exit=$?"
  grep -E "restored|Using SQLite broker" "/tmp/cabby-smoke/run${run}.log"
done
ls -la /tmp/cabby-smoke/data
```
Expected: `exit=0` twice. Run 1: `INF restored 0 messages across 0 streams and 0 sessions (epoch XXXX, new database) in N ms … context=broker provider=sqlite path=/tmp/cabby-smoke/data/cabby.db` then `INF Using SQLite broker: /tmp/cabby-smoke/data/cabby.db (epoch: XXXX, history limit: 100, history ttl: 300s, sessions ttl: 300s, presence ttl: 15s)`. Run 2: the same epoch and `(epoch XXXX, clean shutdown)`. The listing shows `drwx------` for `data`, `-rw-------` for `cabby.db` and `cabby.db.lock`, and no `cabby.db-wal`.

Then the validation:
```sh
ANYCABLE_DISABLE_TELEMETRY=true target/debug/cabby --public --broker=sqlite; echo "exit=$?"
```
Expected: the startup lines, then `failed to initialize broker, cause: --broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path` and `exit=1`.

- [ ] **Step 7: Run the server tests, lint and commit**

Run: `mise exec -- cargo test -p cabby-server 2>&1 | grep "^test result"`
Expected: lib `ok. 171 passed` (162 + 3 config + 4 options + 2 sqlite), bin `ok. 1 passed`.

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy --workspace --all-targets -- -D warnings 2>&1 | grep -E "^(error|warning)" -A3
git add crates/cabby-server/src/cli.rs crates/cabby-server/src/cli/sqlite.rs crates/cabby-server/src/cli/runner.rs
git commit -m "runner: --broker=sqlite

Validates --sqlite_path at startup, registers the sqlite_* metrics, and shuts the
broker down with what is left of the shutdown timeout, after node.shutdown and the
broadcasters.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected from the clippy command: no output, or only the known `crates/cabby-testkit/src/reference/benchi.rs:514` baseline error (Ground rules).

---

### Task 18: Testkit helpers for scenarios through the binary

Both compat modules (Tasks 19 and 20) start Cabby on one database file several times, talk the extended protocol and read the file afterwards. The helpers live in `cabby-testkit` so the two test binaries share them.

**Files:**
- Modify: `crates/cabby-testkit/Cargo.toml` (`[dependencies]`)
- Modify: `crates/cabby-testkit/src/lib.rs` (module list and docs)
- Create: `crates/cabby-testkit/src/sqlite_broker.rs`

- [ ] **Step 1: Add the dependency and the module**

In `crates/cabby-testkit/Cargo.toml`, after `prost.workspace = true`, add `rusqlite.workspace = true`.

In `crates/cabby-testkit/src/lib.rs`, add to the doc list after the `sut` line:

```rust
//! * [`sqlite_broker`]: the SQLite broker through the binary (Cabby only, D-065).
```

and after `pub mod services;`:

```rust
pub mod sqlite_broker;
```

- [ ] **Step 2: Write the helpers**

Create `crates/cabby-testkit/src/sqlite_broker.rs`:

```rust
//! The SQLite broker through the binary (a Cabby addition, deviation D-065):
//! data directories, servers started and stopped on one database file,
//! extended-protocol clients, and reading the file the server left behind.
//!
//! AnyCable rejects `--broker=sqlite`, so every scenario calls [`rust_only`]
//! first; on the Go backend the runner records the case as not observable.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::{Value, json};

use crate::cable_client::{CableClient, Options};
use crate::raw_http;
use crate::sut::Sut;
use crate::{Backend, backend, workspace_root};

/// The extended Action Cable protocol (session recovery, offsets and epochs).
pub const EXT_PROTOCOL: &str = "actioncable-v1-ext-json";
/// Bound for every wait on the server, a client or the database.
pub const DEADLINE: Duration = Duration::from_secs(10);
/// From SIGTERM to exit: the default 30 s shutdown timeout plus a margin.
pub const STOP_GRACE: Duration = Duration::from_secs(40);

/// Reports the case as not observable on the Go backend.
pub fn rust_only() {
    if backend() == Backend::Go {
        unimplemented!(
            "not_observable: AnyCable rejects --broker=sqlite; the SQLite broker is a Cabby addition (D-065)"
        );
    }
}

/// A directory under `target/tmp` for one scenario's database, removed on drop.
pub struct DataDir(PathBuf);

impl DataDir {
    pub fn new(tag: &str) -> Self {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let dir = workspace_root().join(format!(
            "target/tmp/sqlite-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("creating a data dir");
        Self(dir)
    }

    /// The database path. Its `data` directory doesn't exist yet: the server creates it.
    pub fn db(&self) -> PathBuf {
        self.0.join("data").join("cabby.db")
    }
}

impl Drop for DataDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `<db><suffix>`, e.g. the `-wal` file.
pub fn sibling(db: &Path, suffix: &str) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// HTTP broadcasts and the SQLite broker on `db`, then `extra`.
pub fn server_args(db: &Path, extra: &[&str]) -> Vec<String> {
    let mut args =
        vec!["--broadcast_adapter=http".to_owned(), "--broker=sqlite".to_owned(), format!("--sqlite_path={}", db.display())];
    args.extend(extra.iter().map(|a| (*a).to_owned()));
    args
}

pub fn try_start(db: &Path, extra: &[&str]) -> anyhow::Result<Sut> {
    let args = server_args(db, extra);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    Sut::try_start(&args, &[])
}

pub fn start(db: &Path, extra: &[&str]) -> Sut {
    try_start(db, extra).unwrap_or_else(|e| panic!("{e:#}"))
}

/// SIGTERM, then waits for a clean exit; returns the server's output.
pub fn stop_gracefully(sut: &mut Sut) -> String {
    let status = sut.process.stop(STOP_GRACE).expect("stopping the server");
    let output = sut.process.output();
    assert!(status.success(), "the server did not exit cleanly ({status}):\n{output}");
    output
}

/// SIGKILL (a crash), then waits for the process to go.
pub fn kill_hard(sut: &mut Sut) {
    let pid = sut.process.pid().to_string();
    let status = std::process::Command::new("kill").args(["-KILL", &pid]).status().expect("running kill");
    assert!(status.success(), "kill -KILL {pid}");
    sut.process.wait_exit(DEADLINE).expect("the killed server exits");
}

/// Connects with the extended protocol, restoring `restore_sid` if given;
/// returns the client and its welcome.
pub fn connect(sut: &Sut, restore_sid: Option<&str>) -> (CableClient, Value) {
    let headers =
        restore_sid.map(|sid| vec![("X-ANYCABLE-RESTORE-SID".to_owned(), sid.to_owned())]).unwrap_or_default();
    let opts = Options { ignore: vec!["ping".into()], headers, protocol: EXT_PROTOCOL.into(), ..Options::default() };
    let mut client = CableClient::connect(&format!("ws://{}/cable", sut.addr()), opts).expect("connect");
    let welcome = client.receive_within(DEADLINE).expect("welcome");
    assert_eq!(Some("welcome"), welcome["type"].as_str(), "unexpected first message {welcome}");
    (client, welcome)
}

/// The `$pubsub` identifier of `stream`.
pub fn identifier(stream: &str) -> String {
    json!({"channel": "$pubsub", "stream_name": stream}).to_string()
}

/// Subscribes to `stream` (public streams) and returns the identifier.
pub fn subscribe(client: &mut CableClient, stream: &str) -> String {
    let id = identifier(stream);
    client.send(&json!({"command": "subscribe", "identifier": id}));
    let ack = client.receive_within(DEADLINE).expect("subscription reply");
    assert_eq!(Some("confirm_subscription"), ack["type"].as_str(), "unexpected reply {ack}");
    id
}

/// Broadcasts the JSON string `text` to `stream` over HTTP.
pub fn broadcast(sut: &Sut, stream: &str, text: &str) {
    let body = json!({"stream": stream, "data": json!(text).to_string()}).to_string();
    let addr = SocketAddr::from(([127, 0, 0, 1], sut.broadcast_port));
    let headers = [("Content-Type", "application/json")];
    let response =
        raw_http::request(addr, "POST", "/_broadcast", &headers, Some(body.as_bytes())).expect("broadcast request");
    assert_eq!(201, response.status, "broadcast rejected");
}

/// Waits for the broadcast whose message is `text`, skipping others; returns
/// it (with `stream_id`, `offset` and `epoch`).
pub fn receive_message(client: &mut CableClient, text: &str) -> Value {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match client.receive_within(deadline.saturating_duration_since(Instant::now())) {
            Ok(m) if m["message"] == text => return m,
            Ok(_) => {}
            Err(e) => panic!("no message {text:?} within {DEADLINE:?}: {e}"),
        }
    }
}

/// Asks for `stream`'s messages after `offset` in `epoch`; returns the replies
/// up to and including `confirm_history` or `reject_history`.
pub fn history_after(client: &mut CableClient, id: &str, stream: &str, offset: u64, epoch: &str) -> Vec<Value> {
    client.send(&json!({
        "command": "history",
        "identifier": id,
        "history": {"streams": {stream: {"offset": offset, "epoch": epoch}}},
    }));
    replies_until_verdict(client)
}

/// Asks for the messages since `since` (unix seconds).
pub fn history_since(client: &mut CableClient, id: &str, since: i64) -> Vec<Value> {
    client.send(&json!({"command": "history", "identifier": id, "history": {"since": since}}));
    replies_until_verdict(client)
}

fn replies_until_verdict(client: &mut CableClient) -> Vec<Value> {
    let mut replies = Vec::new();
    loop {
        let reply = client.receive_within(DEADLINE).expect("history reply");
        let verdict = matches!(reply["type"].as_str(), Some("confirm_history" | "reject_history"));
        replies.push(reply);
        if verdict {
            return replies;
        }
    }
}

/// The broadcasts among `replies`: (message, offset, epoch).
pub fn messages(replies: &[Value]) -> Vec<(String, u64, String)> {
    replies
        .iter()
        .filter(|m| m.get("message").is_some())
        .map(|m| {
            (
                m["message"].as_str().unwrap_or_default().to_owned(),
                m["offset"].as_u64().unwrap_or_default(),
                m["epoch"].as_str().unwrap_or_default().to_owned(),
            )
        })
        .collect()
}

pub fn unix_now() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after 1970");
    i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
}

pub fn unix_now_ms() -> i64 {
    let since = SystemTime::now().duration_since(UNIX_EPOCH).expect("clock after 1970");
    i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
}

/// Opens the database the server uses (WAL: it may be running).
pub fn open_db(db: &Path) -> Connection {
    let conn = Connection::open_with_flags(db, OpenFlags::SQLITE_OPEN_READ_WRITE).expect("opening the database");
    conn.busy_timeout(DEADLINE).expect("setting the busy timeout");
    conn
}

pub fn meta(db: &Path, key: &str) -> Option<String> {
    open_db(db).query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0)).optional().expect("meta")
}

pub fn session_expiry_ms(db: &Path, sid: &str) -> Option<i64> {
    open_db(db)
        .query_row("SELECT expires_at_ms FROM sessions WHERE sid = ?1", [sid], |r| r.get(0))
        .optional()
        .expect("sessions")
}

pub fn history_rows(db: &Path) -> i64 {
    open_db(db).query_row("SELECT count(*) FROM history", [], |r| r.get(0)).expect("history")
}

/// Waits until the server has written `rows` history rows (write-behind:
/// about 50 ms), observed in the file.
pub fn await_history_rows(db: &Path, rows: i64) {
    let deadline = Instant::now() + DEADLINE;
    while history_rows(db) < rows {
        assert!(Instant::now() < deadline, "{rows} history rows not written within {DEADLINE:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
}
```

- [ ] **Step 3: Build and lint**

Run: `mise exec -- cargo clippy -p cabby-testkit --all-targets -- -D warnings 2>&1 | grep -E "^(error|warning)" -A3`
Expected from the clippy command: no output, or only the known `crates/cabby-testkit/src/reference/benchi.rs:514` baseline error (Ground rules).

- [ ] **Step 4: Commit**

```sh
git add crates/cabby-testkit/Cargo.toml crates/cabby-testkit/src/lib.rs crates/cabby-testkit/src/sqlite_broker.rs Cargo.lock
git commit -m "testkit: helpers for SQLite broker scenarios through the binary

Data directories, graceful and hard stops, extended-protocol clients, history
requests and reads of the database file; rust_only() reports the Go arm as not
observable.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 19: D-065 conformance scenario

The pin for D-065 (§8): start with `--broker=sqlite`, restart gracefully, resume history and a session. An RPC application authenticates connections, so the restored session proves recovery without an RPC call (§9).

**Files:**
- Create: `tests/conformance/sqlite.rs`
- Modify: `tests/conformance/main.rs` (module list: `mod sqlite;` after `mod security;`)

- [ ] **Step 1: Write the scenario**

Add `mod sqlite;` after `mod security;` in `tests/conformance/main.rs`. Create `tests/conformance/sqlite.rs`:

```rust
//! Deviation D-065: the SQLite broker (`--broker=sqlite`), a Cabby addition.
//! A graceful restart keeps the epoch, the stream offsets, the history and the
//! sessions. AnyCable has no such broker, so the Go arm is not observable.

use cabby_core::protos;
use cabby_testkit::fake_rpc::{FakeRpc, Method, Reply};
use cabby_testkit::sqlite_broker::{self as sq, DataDir};
use serde_json::json;

/// An application that authenticates every connection and welcomes it with
/// its session ID, as anycable-rails does.
fn app() -> FakeRpc {
    let fake = FakeRpc::start();
    fake.on(Method::Connect).reply(Reply::with(|call| {
        let sid = call.meta("sid").or_else(|| call.meta("x-anycable-meta-sid")).unwrap_or_default().to_owned();
        protos::ConnectionResponse {
            status: protos::Status::Success as i32,
            identifiers: r#"{"user":"u1"}"#.into(),
            transmissions: vec![json!({"type": "welcome", "sid": sid}).to_string()],
            ..Default::default()
        }
        .into()
    }));
    fake
}

/// regression-for: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
/// regression-for: node/broker_integration_test.go#TestIntegrationHistory_Memory/Subscribe_+_History
/// note: Deviation D-065: with --broker=sqlite, a graceful restart (SIGTERM, then a new process on the same file) keeps the epoch and the stream offsets: a client resuming from its offset gets the messages it missed (m2, m3) with the old epoch, and the next broadcast continues the offsets.
/// note: The session restores by X-ANYCABLE-RESTORE-SID after the restart without an RPC Connect call (restored: true, its $pubsub subscription live again); the startup line reports 3 messages, 1 stream and 2 sessions after a clean shutdown.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn deviation_d065_sqlite_broker_resumes_history_and_sessions_after_a_graceful_restart() {
    sq::rust_only();
    let fake = app();
    let dir = DataDir::new("d065");
    let db = dir.db();
    let rpc = format!("--rpc_host={}", fake.http_url());
    let extra = [rpc.as_str(), "--public_streams", "--disconnect_mode=never"];

    // First run: a session subscribes and sees m1; m2 and m3 arrive after it left.
    let mut sut = sq::start(&db, &extra);
    let (mut client, welcome) = sq::connect(&sut, None);
    let sid = welcome["sid"].as_str().expect("sid in the welcome").to_owned();
    let id = sq::subscribe(&mut client, "d065");
    sq::broadcast(&sut, "d065", "m1");
    let m1 = sq::receive_message(&mut client, "m1");
    let epoch = m1["epoch"].as_str().expect("epoch").to_owned();
    let offset = m1["offset"].as_u64().expect("offset");
    let stream_id = m1["stream_id"].as_str().expect("stream_id").to_owned();
    let (mut witness, _) = sq::connect(&sut, None);
    sq::subscribe(&mut witness, "d065");
    client.close(true);
    sq::broadcast(&sut, "d065", "m2");
    sq::broadcast(&sut, "d065", "m3");
    // Observed by a witness, so both reached the broker before the stop.
    sq::receive_message(&mut witness, "m3");
    drop(witness);
    let connects = fake.call_count(Method::Connect);
    sq::stop_gracefully(&mut sut);

    // Second run on the same file.
    let mut sut = sq::start(&db, &extra);
    let (mut restored, welcome) = sq::connect(&sut, Some(&sid));
    assert_eq!(Some(true), welcome["restored"].as_bool(), "the session is restored: {welcome}");
    assert_eq!(connects, fake.call_count(Method::Connect), "restoring the session called the application");

    let replies = sq::history_after(&mut restored, &id, &stream_id, offset, &epoch);
    assert_eq!(
        vec![("m2".to_owned(), offset + 1, epoch.clone()), ("m3".to_owned(), offset + 2, epoch.clone())],
        sq::messages(&replies),
        "the missed messages, with the old epoch and offsets"
    );
    assert_eq!(Some("confirm_history"), replies.last().and_then(|r| r["type"].as_str()));

    sq::broadcast(&sut, "d065", "m4");
    let m4 = sq::receive_message(&mut restored, "m4");
    assert_eq!((Some(offset + 3), Some(epoch.as_str())), (m4["offset"].as_u64(), m4["epoch"].as_str()));

    let output = sq::stop_gracefully(&mut sut);
    let line = format!("restored 3 messages across 1 streams and 2 sessions (epoch {epoch}, clean shutdown)");
    assert!(output.contains(&line), "no startup line {line:?} in:\n{output}");
}
```

- [ ] **Step 2: Run it against Cabby**

Run (from the worktree, with the services env of Task 0 Step 4 exported):
```sh
mise exec -- cargo xtask test --backend rust deviation_d065 2>&1 | grep -E "^backend|^  tests|UNMAPPED|error"
```
Expected: `backend rust: 1 Rust tests` and `tests:  {"pass": 1}`, followed by an error that the test is not in the manifest yet (Task 21 maps it). Also run the binary directly:
```sh
mise exec -- cargo build -p cabby-server --bin cabby
CABBY_BACKEND=rust SUT_BIN=target/debug/cabby mise exec -- cargo test -p cabby-compat --test conformance sqlite:: 2>&1 | grep "^test "
```
Expected: `test sqlite::deviation_d065_sqlite_broker_resumes_history_and_sessions_after_a_graceful_restart ... ok`

- [ ] **Step 3: Check the Go arm reports not observable**

Run: `CABBY_BACKEND=go mise exec -- cargo test -p cabby-compat --test conformance sqlite:: 2>&1 | grep -E "not_observable|^test "`
Expected: `test sqlite::deviation_d065_… ... FAILED` with `not implemented: not_observable: AnyCable rejects --broker=sqlite; …` in its output (the runner classifies that as `not_observable`).

- [ ] **Step 4: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-compat --all-targets -- -D warnings 2>&1 | grep -E "^(error|warning)" -A3
git add tests/conformance/main.rs tests/conformance/sqlite.rs
git commit -m "conformance: pin D-065 (SQLite broker survives a graceful restart)

Resumes history by offset with the old epoch and restores a session without an RPC
call after SIGTERM and a restart on the same file; not observable on Go.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected from the clippy command: no output, or only the known `crates/cabby-testkit/src/reference/benchi.rs:514` baseline error (Ground rules).

---

### Task 20: End-to-end feature tests (§9)

The rest of §9's end-to-end list, through the binary: a crash rotates the epoch and clients resync; a second process on the same file exits with the lock error; a session touched while clients disconnect at shutdown is on disk (pins the shutdown order: `node.shutdown`, then the broker); a final write that fails leaves `clean_shutdown = '0'` and the next start rotates the epoch; no `-wal` file is left after a graceful shutdown; and the startup validation of the path. (§9's first two items, resuming with offsets and restoring a session without RPC, are the D-065 scenario of Task 19.)

**Files:**
- Create: `tests/features/sqlite.rs`
- Modify: `tests/features/main.rs` (`mod sqlite;` after `mod rpc_less;`)

- [ ] **Step 1: Write the scenarios**

Add `mod sqlite;` after `mod rpc_less;` in `tests/features/main.rs`. Create `tests/features/sqlite.rs`:

```rust
//! The SQLite broker through the binary (deviation D-065, Cabby only): crash
//! recovery, the lock, the shutdown order, a failed final write, the WAL file
//! and startup validation. Every case is not observable on Go.

use std::time::Duration;

use cabby_testkit::sqlite_broker::{self as sq, DataDir};
use cabby_testkit::sut::Sut;

/// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
/// note: D-065: after kill -9 the restart uses a new epoch (startup line "new epoch after an unclean shutdown"); a client resuming with the old epoch gets reject_history, then resyncs by time and gets the flushed messages with the new epoch; offsets continue.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_crash_restart_uses_a_new_epoch_and_clients_resync() {
    sq::rust_only();
    let dir = DataDir::new("crash");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public"]);
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "crash");
    sq::broadcast(&sut, "crash", "c1");
    sq::broadcast(&sut, "crash", "c2");
    let c2 = sq::receive_message(&mut client, "c2");
    let (epoch, offset) = (c2["epoch"].as_str().unwrap().to_owned(), c2["offset"].as_u64().unwrap());
    let stream_id = c2["stream_id"].as_str().unwrap().to_owned();
    sq::await_history_rows(&db, 2);
    sq::kill_hard(&mut sut);
    drop(client);

    let mut sut = sq::start(&db, &["--public"]);
    assert!(sut.process.output().contains("new epoch after an unclean shutdown"), "{}", sut.process.output());
    let (mut client, _) = sq::connect(&sut, None);
    let id = sq::subscribe(&mut client, "crash");

    let replies = sq::history_after(&mut client, &id, &stream_id, offset, &epoch);
    assert_eq!(Some("reject_history"), replies.last().and_then(|r| r["type"].as_str()), "{replies:?}");

    let replies = sq::history_since(&mut client, &id, sq::unix_now() - 60);
    assert_eq!(Some("confirm_history"), replies.last().and_then(|r| r["type"].as_str()), "{replies:?}");
    let resynced = sq::messages(&replies);
    assert_eq!(vec![("c1", 1), ("c2", 2)], resynced.iter().map(|(m, o, _)| (m.as_str(), *o)).collect::<Vec<_>>());
    let new_epoch = resynced[0].2.clone();
    assert_ne!(epoch, new_epoch);

    sq::broadcast(&sut, "crash", "c3");
    let c3 = sq::receive_message(&mut client, "c3");
    assert_eq!((Some(3), Some(new_epoch.as_str())), (c3["offset"].as_u64(), c3["epoch"].as_str()));
    sq::stop_gracefully(&mut sut);
}

/// regression-for: node/broker_integration_test.go#TestIntegrationRestore_Memory/Restore_session_connection_and_channels_state
/// note: D-065: a second Cabby on the same database waits 10 s for the lock file (<path>.lock), then exits with "in use by another Cabby process"; the first keeps serving.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_second_server_on_the_same_database_exits_with_the_lock_error() {
    sq::rust_only();
    let dir = DataDir::new("lock");
    let db = dir.db();
    let mut first = sq::start(&db, &["--public"]);

    let err = sq::try_start(&db, &["--public"]).err().expect("the second server must not start");
    let text = format!("{err:#}");
    assert!(text.contains("failed to initialize broker, cause: SQLite database"), "{text}");
    assert!(text.contains(": in use by another Cabby process (lock file "), "{text}");

    let (mut client, _) = sq::connect(&first, None);
    sq::subscribe(&mut client, "lock");
    sq::stop_gracefully(&mut first);
}

/// regression-for: cli/cli_test.go#TestRunnerDrainsBroadcastsBeforeBrokerShutdown
/// note: D-065, shutdown order: the session touch made when node.shutdown disconnects a connected extended-protocol client reaches the file (its expiry is at least SIGTERM time + sessions_ttl), so the broker is shut down after the node.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_session_touched_while_clients_disconnect_at_shutdown_is_on_disk() {
    sq::rust_only();
    let dir = DataDir::new("order");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public"]);
    let (mut client, welcome) = sq::connect(&sut, None);
    let sid = welcome["sid"].as_str().expect("sid").to_owned();
    sq::subscribe(&mut client, "order");
    // Not synchronization: the wall clock must move past the subscribe-time
    // commit, so the shutdown's touch sets a later expiry than the commit did.
    std::thread::sleep(Duration::from_millis(50));
    let stopping_at = sq::unix_now_ms();

    sq::stop_gracefully(&mut sut);
    drop(client);

    let expiry = sq::session_expiry_ms(&db, &sid).expect("the session is on disk");
    let sessions_ttl_ms = 300 * 1000;
    assert!(
        expiry >= stopping_at + sessions_ttl_ms,
        "expiry {expiry} predates the shutdown touch (>= {})",
        stopping_at + sessions_ttl_ms
    );
}

/// regression-for: broker/memory_test.go#TestMemory_FromOffset/With_unknown_epoch
/// note: D-065: when every write fails at shutdown (here another connection holds SQLite's write lock, so writes end with SQLITE_BUSY; a read-only or full disk behaves the same), clean_shutdown stays '0' and the next start rotates the epoch.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn a_failed_final_write_leaves_an_unclean_shutdown_and_a_new_epoch() {
    sq::rust_only();
    let dir = DataDir::new("flush");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public", "--shutdown_timeout=2"]);
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "flush");
    sq::broadcast(&sut, "flush", "f1");
    let epoch = sq::receive_message(&mut client, "f1")["epoch"].as_str().unwrap().to_owned();
    sq::await_history_rows(&db, 1);

    let blocker = sq::open_db(&db);
    blocker.execute_batch("BEGIN EXCLUSIVE").expect("SQLite's write lock");
    sq::stop_gracefully(&mut sut);
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    drop(client);

    assert_eq!(Some("0".to_owned()), sq::meta(&db, "clean_shutdown"));
    let mut sut = sq::start(&db, &["--public"]);
    assert!(sut.process.output().contains("new epoch after an unclean shutdown"), "{}", sut.process.output());
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "flush");
    sq::broadcast(&sut, "flush", "f2");
    let f2 = sq::receive_message(&mut client, "f2");
    assert_ne!(Some(epoch.as_str()), f2["epoch"].as_str());
    sq::stop_gracefully(&mut sut);
}

/// regression-for: cli/cli_test.go#TestRunnerDrainsBroadcastsBeforeBrokerShutdown
/// note: D-065: after a graceful shutdown the WAL is checkpointed and removed, so the .db file alone is a complete copy (backups, volume snapshots); clean_shutdown is '1'.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn no_wal_file_is_left_after_a_graceful_shutdown() {
    sq::rust_only();
    let dir = DataDir::new("wal");
    let db = dir.db();
    let mut sut = sq::start(&db, &["--public"]);
    let (mut client, _) = sq::connect(&sut, None);
    sq::subscribe(&mut client, "wal");
    sq::broadcast(&sut, "wal", "w1");
    sq::receive_message(&mut client, "w1");

    sq::stop_gracefully(&mut sut);
    drop(client);

    assert!(db.exists());
    assert!(!sq::sibling(&db, "-wal").exists(), "a -wal file is left next to {}", db.display());
    assert_eq!(1, sq::history_rows(&db));
    assert_eq!(Some("1".to_owned()), sq::meta(&db, "clean_shutdown"));
}

/// regression-for: config/presets_test.go#TestBroker
/// note: D-065: --broker=sqlite without --sqlite_path (or CABBY_SQLITE_PATH, or [sqlite] path) stops the server at startup with a message naming the three settings.
/// note: not_observable on Go: AnyCable rejects --broker=sqlite.
#[test]
fn the_sqlite_broker_needs_a_database_path() {
    sq::rust_only();
    let err = Sut::try_start(&["--public", "--broadcast_adapter=http", "--broker=sqlite"], &[])
        .err()
        .expect("the server must not start");
    let text = format!("{err:#}");
    assert!(
        text.contains("failed to initialize broker, cause: --broker=sqlite needs a database path: set --sqlite_path, CABBY_SQLITE_PATH or [sqlite] path"),
        "{text}"
    );
}
```

- [ ] **Step 2: Run them**

```sh
mise exec -- cargo build -p cabby-server --bin cabby
CABBY_BACKEND=rust SUT_BIN=target/debug/cabby mise exec -- cargo test -p cabby-compat --test features sqlite:: 2>&1 | grep -E "^test |^test result"
```
Expected: 6 tests `ok`, `test result: ok. 6 passed; …` (about 15 s: the lock scenario waits out the 10 s lock timeout).

If `a_session_touched_while_clients_disconnect_at_shutdown_is_on_disk` fails, the runner order regressed: check `node_shutdownables` in `runner.rs:457-548` (node first, broker after the broadcasters).

- [ ] **Step 3: Lint and commit**

```sh
mise exec -- cargo fmt --all && mise exec -- cargo clippy -p cabby-compat --all-targets -- -D warnings 2>&1 | grep -E "^(error|warning)" -A3
git add tests/features/main.rs tests/features/sqlite.rs
git commit -m "features: SQLite broker end to end

Crash restart with a new epoch and a clean resync, the lock error for a second
process, the shutdown order (session touch on disk), a failed final write rotating
the epoch, no WAL file after a graceful stop, and the missing-path error.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected from the clippy command: no output, or only the known `crates/cabby-testkit/src/reference/benchi.rs:514` baseline error (Ground rules).

---

### Task 21: Manifest: inventory and recorded statuses

The runner fails on unmapped tests and on cases whose recorded status changed, and CI checks that `cargo xtask inventory` leaves `compat/test-map.json` unchanged (`.github/workflows/ci.yml`, job `go-reference`). The seven new compat tests become `rust_only` entries (their annotations are `regression-for:`): `cargo xtask inventory` adds them as `unverified`/`pending`, the Go run records `not_observable`, the Rust run records `pass`. On a single feature branch the manifest is updated here (the "orchestrator records on main" note in `docs/implementing.md` is for parallel agent branches).

Toolchains: `cargo xtask inventory` runs the Go AST tool (`go run`, `tools/xtask/src/inventory.rs` `go_ast`) and needs `compat/reference`. Neither this Mac nor the Linux box has `go` on `PATH` (checked 2026-11-23; the box also lacks `target/reference-adapters`), so run it through mise, which installs Go 1.27.3 on first use without touching `mise.toml`. The Go-backend run needs no Go toolchain when `target/reference-bin/anycable-go` and `target/reference-adapters/bridge` exist (`tools/xtask/src/runner.rs` `run`); link the main checkout's builds. If installing Go locally is not an option, push the branch and take the `go-reference` job's diff from CI instead.

**Files:**
- Modify: `compat/test-map.json` (generated)

- [ ] **Step 1: Link the reference builds**

```sh
mkdir -p target
ln -s <repo>/target/reference-bin target/reference-bin
ln -s <repo>/target/reference-adapters target/reference-adapters
ls target/reference-bin/anycable-go target/reference-adapters/bridge
```
Expected: both paths listed. (`target/` is gitignored.)

- [ ] **Step 2: Inventory**

Run: `mise exec go@1.27.3 -- cargo xtask inventory 2>&1 | tail -5`
Expected: the progress report; no `references unknown upstream case` or `has no … annotation` error. Then:
```sh
python3 - <<'PY'
import json
m = json.load(open("compat/test-map.json"))
new = [r for r in m["rust_only"] if "sqlite::" in r["rust_case"]]
for r in new: print(r["rust_case"], r["reference_status"], r["rust_status"])
print(len(new))
PY
```
Expected: 7 lines `… unverified pending` (`conformance::sqlite::deviation_d065_…` and the six `features::sqlite::…`), then `7`.

- [ ] **Step 3: Record the Go reference status**

Run: `mise exec -- cargo xtask test --backend go --record sqlite:: 2>&1 | grep -E "^backend|^  tests|manifest|error"`
Expected:
```
backend go: 7 Rust tests
  tests:  {"not_observable": 7}
  manifest updated
```

- [ ] **Step 4: Record the Rust status**

Run (services env from Task 0 Step 4 exported):
```sh
mise exec -- cargo xtask test --backend rust --record sqlite:: 2>&1 | grep -E "^backend|^  tests|manifest|error"
```
Expected:
```
backend rust: 7 Rust tests
  tests:  {"pass": 7}
  manifest updated
```

- [ ] **Step 5: Whole suites**

Run: `mise exec -- cargo xtask test --backend rust 2>&1 | grep -E "^backend|^  tests|compatibility error"`
Expected on macOS: `backend rust: 1198 Rust tests`, `tests:  {"fail": 3, "pass": 1195}` and `Error: 6 compatibility error(s)`, the three Linux-only `conformance::threads` tests of Task 0 and nothing else. (CI's Linux job must be fully green.)

Run: `mise exec -- cargo xtask test --backend go 2>&1 | grep -E "^backend|^  tests|compatibility error"`
Expected: `backend go: 1198 Rust tests` and no compatibility error: every recorded reference status is unchanged, the 7 new cases are `not_observable`. (If this local Go run is not possible, CI's `go-reference` job is the check.)

- [ ] **Step 6: The CI manifest check, locally**

CI reruns the inventory and fails on any diff, so a second inventory must change nothing:
```sh
cp compat/test-map.json /tmp/test-map.recorded.json
mise exec go@1.27.3 -- cargo xtask inventory > /dev/null
cmp -s /tmp/test-map.recorded.json compat/test-map.json && echo "inventory: unchanged"
python3 compat/scripts/normalize-tests-manifest.py && git diff --exit-code --stat tests/Cargo.toml && echo "targets: unchanged"
```
Expected: `inventory: unchanged`, `35 test targets`, `targets: unchanged` (no `[[test]]` entry was added, so the normalizer has nothing to fix).

- [ ] **Step 7: Commit**

```sh
git add compat/test-map.json
git commit -m "Manifest: map the SQLite broker scenarios

Seven rust_only cases (D-065 conformance pin and six feature scenarios): reference
status not_observable, Rust status pass.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 22: Docs: deviation entry and README; final checks

**Files:**
- Modify: `compat/deviations.md` (move D-065 from *Proposed* to *Implemented in Cabby*)
- Modify: `README.md` (`## Features`, line 62)

- [ ] **Step 1: Move the deviation**

In `compat/deviations.md`, delete the `| D-065 | …` row from the *Proposed (not implemented yet)* table (added in Task 1), and add this row to the *Implemented in Cabby* table, directly after the `| D-057 |` row:

```markdown
| D-065 | SQLite broker (`--broker=sqlite`, `--sqlite_path`, `CABBY_SQLITE_PATH`, `[sqlite] path`) | No such broker: `--broker=sqlite` stops the server at startup (`Unsupported broker adapter: sqlite`) and `--sqlite_path` is not a flag | A Cabby addition, observable only when chosen, apart from the `--sqlite_path` line (category `SQLITE:`) in `--help` and the `[sqlite]` section of `--print-config`. The memory broker plus a write-behind copy in one SQLite file (WAL, mode 0600 in a 0700 directory, lock file `<path>.lock`): a graceful restart keeps the epoch, stream offsets, history (within `history_limit` and `history_ttl`) and sessions (with their original expiry); after a crash or a failed final write the epoch rotates, so clients resync. Presence is not persisted. Startup line `restored N messages across S streams and K sessions (epoch E, clean shutdown \| new epoch after an unclean shutdown \| new database) in T ms` (`context=broker provider=sqlite`); metrics `sqlite_queue_size`, `sqlite_batch_write_ms`, `sqlite_write_errors_total`, `sqlite_dropped_total` and `sqlite_dropped_{history,session_touch,session_commit}_total`; `--broker=sqlite` without a path stops at startup | durable history on one node without Redis or NATS (`docs/superpowers/specs/2026-11-23-sqlite-broker-design.md`) | `conformance::sqlite::deviation_d065_sqlite_broker_resumes_history_and_sessions_after_a_graceful_restart`; `features::sqlite::*` (crash, lock, shutdown order, failed final write, WAL file, missing path); `cabby-core` `broker::sqlite::*`; `cabby-server` `config::tests::broker_preset_keeps_an_explicit_sqlite_broker`, `cli::sqlite::tests::*` | not observable: AnyCable rejects `--broker=sqlite` |
```

Run: `grep -c "^| D-065 |" compat/deviations.md` → Expected: `1`. And `awk '/^## Proposed/,/^## Resolved/' compat/deviations.md | grep -c "D-065"` → Expected: `0`.

- [ ] **Step 2: README feature list**

In `README.md`, under `## Features`, after the line `- Reliable streams: history and session recovery after a reconnect`, add:

```markdown
- SQLite broker (`--broker=sqlite --sqlite_path=/var/lib/cabby/cabby.db`): stream history,
  offsets and sessions survive a graceful restart of a single node, with no Redis or NATS. After
  a crash, clients resync cleanly (deviation D-065)
```

- [ ] **Step 3: Final checks**

```sh
mise exec -- cargo fmt --all -- --check
mise exec -- cargo clippy --workspace --all-targets -- -D warnings 2>&1 | grep -E "^(error|warning)" -A3
mise exec -- cargo test --workspace --lib --bins 2>&1 | grep "^test result" | sort | uniq -c
```
Expected: no fmt diff; from clippy no output, or only the known `crates/cabby-testkit/src/reference/benchi.rs:514` baseline error (Ground rules); every `test result: ok` (cabby-core lib 152 passed, cabby-server lib 171 passed).

Release cross-build of the whole binary (§3: "cross-compiles with the existing `cargo zigbuild` release builds (glibc 2.28)"):
```sh
for target in x86_64-unknown-linux-gnu.2.28 aarch64-unknown-linux-gnu.2.28; do
  mise exec zig@0.15.2 cargo:cargo-zigbuild@0.24.1 -- \
    cargo zigbuild --release -p cabby-server --bin cabby --target "${target}" 2>&1 | tail -1
done
ls -l target/x86_64-unknown-linux-gnu/release/cabby target/aarch64-unknown-linux-gnu/release/cabby
```
Expected: `Finished \`release\` …` twice and both binaries listed. If the vendored mruby C build fails under zig (a pre-existing concern, not SQLite's), rerun with `--no-default-features` to isolate the SQLite part and report it.

- [ ] **Step 4: Commit**

```sh
x86=$(wc -c < target/x86_64-unknown-linux-gnu/release/cabby | tr -d ' ')
arm=$(wc -c < target/aarch64-unknown-linux-gnu/release/cabby | tr -d ' ')
git add compat/deviations.md README.md
git commit -m "Docs: D-065 implemented; README lists the SQLite broker

Release cross-build checked with cargo zigbuild for glibc 2.28: cabby is ${x86}
bytes (x86_64) and ${arm} bytes (aarch64).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 23: Benchmark on the Linux box

§9: throughput and latency with `--broker=sqlite` match `--broker=memory` within run noise, on the Linux benchmark box with the existing harness (`compat/scripts/bench-compare.sh`, `crates/cabby-bench`). The harness passes `--broker=none` by default (`crates/cabby-bench/src/server.rs` `ServerConfig::args`); two wrapper SUTs append the broker flags last, so they win (`--server-arg` relies on the same last-flag-wins rule, `crates/cabby-bench/src/lib.rs`). Both wrappers keep history, so the comparison isolates the write-behind cost.

The Linux benchmark box (`ssh <bench-box>`, the repo at `<repo>` there) is at an older `main` (`6e37634` on 2026-11-23); this task pushes the branch to it over SSH, not to GitHub. CLAUDE.md: run benchmarks only with the machine idle.

**Files:**
- Create: `bench/sqlite/cabby-memory`
- Create: `bench/sqlite/cabby-sqlite`
- Modify: `compat/scripts/bench-compare.sh` (the `BIN` map and the `unknown server` message)
- Create: `docs/benchmarks/sqlite-results/` (generated)

- [ ] **Step 1: The wrapper SUTs**

Create `bench/sqlite/cabby-memory`:

```sh
#!/bin/sh
# Cabby on the memory broker (history kept), the baseline of the SQLite broker comparison.
# exec keeps the PID the harness samples.
exec "$(dirname "$0")/../../target/release/cabby" "$@" --broker=memory
```

Create `bench/sqlite/cabby-sqlite`:

```sh
#!/bin/sh
# Cabby on the SQLite broker, on a fresh database per run, for the comparison with cabby-memory.
# exec keeps the PID the harness samples.
dir="$(mktemp -d "${TMPDIR:-/tmp}/cabby-bench-sqlite.XXXXXX")"
exec "$(dirname "$0")/../../target/release/cabby" "$@" --broker=sqlite "--sqlite_path=${dir}/cabby.db"
```

```sh
chmod +x bench/sqlite/cabby-memory bench/sqlite/cabby-sqlite
```

In `compat/scripts/bench-compare.sh`, add to the `declare -A BIN=(` map, after the `[cabby]=…` line:

```bash
  [cabby-memory]="${ROOT}/bench/sqlite/cabby-memory"
  [cabby-sqlite]="${ROOT}/bench/sqlite/cabby-sqlite"
```

and change `unknown server ${sut} (actioncable, anycable-go, cabby)` to `unknown server ${sut} (actioncable, anycable-go, cabby, cabby-memory, cabby-sqlite)`. Add one line to the usage comment at the top: `#        SUTS="cabby-memory cabby-sqlite" compares the memory and SQLite brokers (bench/sqlite).`

- [ ] **Step 2: Commit and push the branch to the box**

```sh
git add bench/sqlite compat/scripts/bench-compare.sh
git commit -m "bench: memory vs SQLite broker wrappers for bench-compare.sh

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push <bench-box>:<repo> sqlite-broker:sqlite-broker
```
Expected: `* [new branch]      sqlite-broker -> sqlite-broker`.

- [ ] **Step 3: Build on the box and check the wrappers**

```sh
ssh <bench-box> 'cd <repo> && git switch sqlite-broker && \
  mise exec -- cargo build --release -p cabby-server -p cabby-bench 2>&1 | tail -1 && \
  bench/sqlite/cabby-memory --broker=none --print-config | grep -A2 "^\[broker\]" && \
  bench/sqlite/cabby-sqlite --broker=none --print-config | grep -A3 -E "^\[(broker|sqlite)\]"'
```
Expected: `Finished \`release\` …`; the first config shows `adapter = "memory"` under `[broker]`; the second `adapter = "sqlite"` and `path = "/tmp/cabby-bench-sqlite.…/cabby.db"` under `[sqlite]`. Then `ssh <bench-box> 'rm -rf /tmp/cabby-bench-sqlite.*'`.

- [ ] **Step 4: Run the comparison (machine idle)**

```sh
ssh <bench-box> 'cd <repo> && uptime && \
  nohup env SUTS="cabby-memory cabby-sqlite" compat/scripts/bench-compare.sh docs/benchmarks/sqlite-results \
  > /tmp/bench-sqlite.log 2>&1 &'
```
Check `uptime` first: a load average near 0 means idle. The run takes about an hour (7 workloads, 2 servers, 1 warm-up + 5 rounds). Wait for the end marker:
```sh
ssh <bench-box> 'tail -1 /tmp/bench-sqlite.log'
```
Expected eventually: `results in <repo>/docs/benchmarks/sqlite-results`.

- [ ] **Step 5: Summarize and judge**

```sh
ssh <bench-box> 'cd <repo> && python3 compat/scripts/bench-summary.py docs/benchmarks/sqlite-results'
```
Expected: a Markdown table with `cabby-memory` and `cabby-sqlite` columns. Pass criterion (§9 "within run noise"): for every workload, the `cabby-sqlite` median of delivered throughput, p50 and p99 latency and CPU lies within the min-max spread of the five `cabby-memory` runs (read the per-run files `docs/benchmarks/sqlite-results/<workload>.cabby-memory.<n>.txt` for the spread). Memory may be higher by about the queue's size; note it. If a metric is outside the spread, rerun that workload once (`SUTS=… RUNS=5` with only that `run …` line) before treating it as a regression, and report it rather than tuning silently. Also note `exit=` lines or `generator_saturated=true` runs, which the summary excludes.

Clean up the per-run databases: `ssh <bench-box> 'rm -rf /tmp/cabby-bench-sqlite.*'`.

Write the verdict down for the commit, e.g. `VERDICT="cabby-sqlite within the cabby-memory run spread on every workload"`, or name each workload and metric that is not.

- [ ] **Step 6: Bring the results back and commit**

```sh
scp -r <bench-box>:<repo>/docs/benchmarks/sqlite-results docs/benchmarks/
python3 compat/scripts/bench-summary.py docs/benchmarks/sqlite-results > /tmp/sqlite-bench-summary.md
{
  printf 'Benchmarks: memory vs SQLite broker\n\n'
  cat /tmp/sqlite-bench-summary.md
  printf '\nVerdict: %s\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\n' "${VERDICT}"
} > /tmp/sqlite-bench-commit.txt
git add docs/benchmarks/sqlite-results
git commit -F /tmp/sqlite-bench-commit.txt
```

---

### Task 24: Handoff: what the cabby-site repo needs (§10)

Site docs and translations ship separately in the cabby-site repo, with the release (§10, §11). This task changes no file here; it ends with the list below passed on with the branch (PR description or the release issue). Nothing to commit.

- [ ] **Step 1: Hand over this list**

1. New guide "Keep history across restarts (SQLite)": `--broker=sqlite`, `--sqlite_path` / `CABBY_SQLITE_PATH` / `[sqlite] path`; the Kamal accessory volume (`volumes: ["cabby_data:/var/lib/cabby"]`); what survives a graceful restart (epoch, offsets, history within `history_limit`/`history_ttl`, sessions with their original expiry) versus a crash (new epoch, clients resync; up to ~50 ms of writes may be lost); presence is not persisted; Kamal accessories aren't restarted on app deploys, so the value shows on upgrades, `kamal accessory reboot` and server reboots; the file is self-contained after a graceful stop (backups); a second Cabby on the same file exits after 10 s; a corrupt file is moved to `<path>.corrupt-<ts>`; the startup line and the `sqlite_*` metrics.
2. Configuration page: `--broker=sqlite`, `--sqlite_path`, `CABBY_SQLITE_PATH`, `[sqlite] path` (no `ANYCABLE_*` alias), and that `--presets=broker` keeps an explicit `--broker=sqlite`.
3. Rails guide, Kamal section: the volume line.
4. Landing page, "How it works": one line, e.g. "No Redis needed. History survives restarts with a single SQLite file."
5. Landing page comparison table: the Cabby history row, `comparison.rows.history.cabby` in `config/locales/*.yml` (currently "Stream history and session recovery, in memory or in NATS"), must mention SQLite, e.g. "Stream history and session recovery, in memory, in NATS or in a SQLite file".
6. All of it in the site's six languages (every `config/locales/*.yml` and translated page).
7. Outside the site, for the release: add SQLite's public-domain notice to the cargo-about output (Phase A Task 18) when that lands; ship in v0.2.0 alongside Phase A with one set of release notes (§11).

---

## Spec coverage

| Spec | Requirement | Task |
|---|---|---|
| §1 | single node keeps history and sessions across restarts; crash resyncs cleanly | 13, 14, 19, 20 |
| §2 | `--broker=sqlite`, `--sqlite_path` required, `CABBY_SQLITE_PATH`, `[sqlite] path`, no `ANYCABLE_*` alias | 15, 16, 17 |
| §2 | `--presets=broker` keeps `--broker=sqlite` (pinned) | 15, 16 |
| §2 | parent dir 0700, files 0600; unwritable path exits at startup | 11, 13, 17, 20 |
| §2 | retention from `history_limit`, `history_ttl`, `sessions_ttl`; presence not persisted | 10, 13, 14 |
| §2, §10 | Kamal volume (docs) | 24 |
| §3 | `SqliteBroker` wraps `Memory`, which serves reads, offsets and fan-out | 14 |
| §3 | `handle_broadcast_recorded`, `set_epoch` (exists), `restore_stream`, `restore_session_with_deadline` | 3, 4, 5 |
| §3 | persister thread, `Mutex<VecDeque>` + `Condvar` queue, ops, 50 ms / 1,000-op batches, `streams` upsert per batch | 8, 9, 12 |
| §3 | queue bound 100,000, drop history → touch → commit, counted, never blocks | 8, 14 |
| §3 | prune every 30 s with indexes | 10, 12 |
| §3 | loader before connections | 13, 14, 17 |
| §3 | lock file via `File::try_lock` | 11, 13, 20 |
| §3 | `rusqlite` `bundled`, zigbuild glibc 2.28, ~1 MB | 2, 22 |
| §4 | pragmas, units in one module, schema, version, forward migrations, newer refused | 6, 7 |
| §4 | pruning queries | 10 |
| §5 | broadcast, commit, touch data flow; reads from memory | 14 |
| §5 | startup steps 1-6 (lock retry 100 ms / 10 s, quick_check, epoch rule, contiguous tail anchored at `max_seq`, sessions, log) | 11, 13 |
| §5 | shutdown order (ws server, node, broadcasters, broker), pinned | 17, 20 |
| §5 | final flush, `clean_shutdown='1'` in the same transaction only on success within the timeout, checkpoint, close, unlock | 9, 12, 14, 17, 20 |
| §6 | write errors kept and retried, rate-limited logs, recovery logged | 12 |
| §6 | corruption only on `SQLITE_CORRUPT`/`NOTADB`/`quick_check`; busy, permissions, newer schema exit | 7, 13 |
| §7 | startup line; four metrics | 13, 17 |
| §8 | D-065 deviation, conformance pin, Go not observable, memory and NATS unchanged | 1, 19, 21, 22 |
| §9 | unit tests (batching, overflow, recorded offsets, loader cases, deadline, epoch, prune plans, schema, corruption, modes, presets) | 3-16 |
| §9 | end-to-end tests | 19, 20 |
| §9 | benchmark | 23 |
| §10 | site docs and translations | 24 |
| §11 | v0.2.0 with Phase A; out of scope: PostgreSQL, inspector | 24 |
