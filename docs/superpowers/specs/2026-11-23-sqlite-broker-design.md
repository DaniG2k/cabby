# SQLite broker: stream history and sessions that survive restarts

Status: approved design, 2026-11-23, revised the same day after two design reviews against
`crates/cabby-core/src/broker.rs` and `cli/runner.rs`. First of the "zero Redis" brokers; the PostgreSQL broker and
pub/sub follow in their own spec.

## 1. Goal

A single Cabby node keeps stream history and session-recovery state across restarts, with no
external service. After a planned restart, reconnecting clients resume from their stream offsets
and restore their sessions as if Cabby had never stopped. After a crash, clients resync cleanly
(section 5).

Who it's for: Rails 8 / Kamal deployments that run on one server and often already use SQLite
(Solid Queue, Solid Cache, Solid Cable). They get durable history without adding Redis or NATS.

What it is not: a multi-node broker (that's the PostgreSQL work), and not a Redis-style broker.
It reuses a local file and stays out of AnyCable Pro's territory (see `docs/roadmap.md`).

Honest scope note for the docs: in Kamal, Cabby runs as an accessory, and accessories aren't
restarted on normal app deploys. The value shows when Cabby itself restarts: an upgrade, a
`kamal accessory reboot` or a server reboot. A crash is handled safely but doesn't keep offsets.

## 2. User-facing behaviour

```
cabby --broker=sqlite --sqlite_path=/var/lib/cabby/cabby.db ...
```

- `--broker=sqlite` is a new value next to `memory` and `nats`.
- `--sqlite_path` is required with `--broker=sqlite`. Environment variable `CABBY_SQLITE_PATH`;
  config file section `[sqlite]`, key `path`. These are Cabby-only settings, so they have no
  `ANYCABLE_*` alias; the names work the same before and after Phase A's renaming.
- `--presets=broker` only sets the broker when it is still the default (`config.rs`
  `load_broker_preset`), so an explicit `--broker=sqlite` is kept. A test pins this.
- Cabby creates the parent directory (mode `0700`) and the database files (mode `0600`) if missing:
  the file holds session state and private stream broadcasts. If the path can't be created or
  written, Cabby exits at startup with a clear error.
- Retention uses the existing broker settings unchanged: `history_limit`, `history_ttl`,
  `sessions_ttl`. Switching from `memory` to `sqlite` changes nothing else.
- What survives a **graceful** restart: the broker epoch, stream offsets, stream history (within
  limit and TTL) and session state (within each session's original expiry). Presence is **not**
  persisted: every connection drops on restart, so saved presence would only show members who are
  gone.
- Kamal: the Cabby accessory needs a volume, e.g. `volumes: ["cabby_data:/var/lib/cabby"]`.

## 3. Architecture

`SqliteBroker` implements `cabby_core::broker::Broker` by wrapping the existing in-memory broker
(`broker::Memory`). The memory broker stays the live path: it serves every read, assigns offsets
and keeps fan-out at full speed. Two new pieces sit beside it.

**Small additions to `broker::Memory`** (internal API, no behaviour change for `memory`):
- `handle_broadcast_recorded(&self, msg) -> Result<Option<Recorded>, BrokerError>`: does exactly
  what `handle_broadcast` does and returns `Some(Recorded { offset, timestamp })` for a stored
  message, `None` for a transient one. Calling `peak()` afterwards would race under concurrent
  broadcasts, so the offset and timestamp come from the same critical section that assigns them.
- `set_epoch(&self, epoch)`, used once by the loader before the server starts.
- `restore_stream(&self, name, max_offset, deadline, entries)`: installs a stream's next offset,
  its 10 × TTL deadline and its contiguous tail of entries.
- `restore_session_with_deadline(&self, sid, data, expires_at_ms)`: keeps the original expiry.
  Calling `commit_session` at startup would reset every session to `now + sessions_ttl` on every
  restart.

**Persister:** one background OS thread with a write queue (`Mutex<VecDeque<WriteOp>>` plus a
`Condvar`; `std::sync::mpsc::sync_channel` can't drop the oldest item).
- Ops: `History { stream, offset, ts_secs, data }`, `SessionCommit { sid, data, expires_at_ms }`,
  `SessionTouch { sid, expires_at_ms }`, `Epoch(String)`.
- Every 50 ms, or as soon as 1,000 ops are queued, it writes one batch in a single transaction.
  Each batch also upserts `streams` for every stream it touched.
- The queue is strictly bounded at 100,000 ops, so memory stays bounded in every failure mode. On
  overflow it drops the **oldest `History` op**; only if none is queued does it drop the oldest
  `SessionTouch`, then the oldest `SessionCommit`. Every drop is counted. Enqueueing never blocks
  the broadcast path.
- Pruning runs every 30 s (section 4), using indexes, never full scans.

**Loader:** runs once at startup, before Cabby accepts connections (section 5).

**File lock:** an OS advisory lock on `<path>.lock` via `std::fs::File::try_lock` (stable since
Rust 1.89; safe Rust; released by the kernel if the process dies). SQLite's WAL mode deliberately
lets several processes open the same database, and `locking_mode=EXCLUSIVE` only locks on the
first write, so it can't be the guard.

This is a write-behind design. It was chosen over write-through (a disk write on every message
costs throughput and latency, Cabby's headline numbers) and snapshot-on-shutdown (loses everything
on a crash).

Dependency: `rusqlite` with the `bundled` feature (SQLite is public domain). No system library is
needed, and it cross-compiles with the existing `cargo zigbuild` release builds (glibc 2.28). The
feature is on by default; the release binary grows by about 1 MB. Add its notice to the cargo-about
output (Phase A Task 18) when that lands.

## 4. Storage

Pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout` of a few seconds. `NORMAL` is
safe against application crashes; a power cut can lose the last few commits, which is within the
write-behind promise.

Units are fixed in one place: history timestamps are **unix seconds**, matching
`broker::Entry.timestamp` and `history_since`; session expiries are **unix milliseconds**,
matching the memory broker's session expiration queue. Stream deadlines are **unix seconds**,
like `Memstream`'s deadline and `history.ts`. All conversions live in one module.

```sql
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,   -- 'schema_version', 'epoch', 'clean_shutdown'
  value TEXT NOT NULL
) WITHOUT ROWID;

-- One row per stream ever written, so a quiet stream keeps its offsets after its entries expire
-- (the memory broker keeps a stream alive for 10 x history_ttl, broker.rs Memstream::append).
CREATE TABLE streams (
  stream      TEXT PRIMARY KEY,
  max_seq     INTEGER NOT NULL,
  deadline    INTEGER NOT NULL   -- unix seconds
) WITHOUT ROWID;

-- Rowid tables: payloads can be large, and SQLite advises against WITHOUT ROWID for wide rows.
CREATE TABLE history (
  stream TEXT    NOT NULL,
  seq    INTEGER NOT NULL,  -- the stream offset ("offset" is an SQL keyword)
  ts     INTEGER NOT NULL,  -- unix seconds
  data   TEXT    NOT NULL
);
CREATE UNIQUE INDEX history_stream_seq ON history (stream, seq);
CREATE INDEX history_ts ON history (ts);

CREATE TABLE sessions (
  sid           TEXT    PRIMARY KEY,
  data          BLOB    NOT NULL,  -- the Cacheable bytes commit_session receives
  expires_at_ms INTEGER NOT NULL
);
CREATE INDEX sessions_expires ON sessions (expires_at_ms);
```

Pruning (every 30 s, indexed):
- `DELETE FROM history WHERE ts < now - history_ttl` (uses `history_ts`).
- Per stream written since the last prune: delete rows with `seq <= max_seq - history_limit`
  (uses `history_stream_seq`).
- `DELETE FROM streams WHERE deadline < now_secs`, and `DELETE FROM sessions WHERE expires_at_ms < now_ms`.

Schema version starts at 1. A newer Cabby migrates older files forward at startup. An older Cabby
refuses to open a file whose version is newer than it knows, with a clear error; that is not
treated as corruption.

## 5. Data flow

- **Broadcast:** `SqliteBroker::handle_broadcast` calls `Memory::handle_broadcast_recorded`
  (offset assigned, fan-out as today). For `Some(Recorded)` it enqueues a `History` op; transient
  messages are never persisted.
- **Session commit and touch:** stored in memory as today, then a `SessionCommit` or
  `SessionTouch` op with the absolute expiry in milliseconds.
- **Reads:** `history_from`, `history_since`, `restore_session` and `peak` are served from memory.
- **Startup (Loader):**
  1. Take the file lock (retry every 100 ms for up to 10 s, then exit with an error naming the
     path).
  2. Open the database, run `PRAGMA quick_check`, migrate the schema if needed.
  3. **Epoch:** if `meta.clean_shutdown` is `'1'`, reuse the saved epoch. Otherwise (first start,
     or the previous run crashed) generate a new epoch. Either way, write `clean_shutdown = '0'`
     before accepting connections.
  4. **Streams:** for each `streams` row still within its deadline, walk its history with
     `ORDER BY seq DESC LIMIT history_limit`, starting from `expected = max_seq`: accept a row only
     while `seq == expected` (then `expected -= 1`) and it is within `history_ttl`; stop at the
     first mismatch. If the newest stored row isn't `max_seq` (its op was dropped or lost), the
     stream loads with no entries. Install the accepted rows and `max_seq` with `restore_stream`,
     so the memory broker's invariant holds (no entries, or the newest entry's offset equals the
     stream offset) and the next broadcast gets `max_seq + 1`. The memory broker's
     `filter_by_offset` assumes contiguous offsets, so a gap is never loaded.
  5. **Sessions:** restore unexpired rows with `restore_session_with_deadline`.
  6. Log the result and start listening.
- **Graceful shutdown:** the runner already stops in the right order (`cli/runner.rs`
  `Runner::shutdown`): the WebSocket server stops accepting connections, then `node.shutdown`
  disconnects clients (their final `commit_session` calls happen there), then broadcasters, then
  the broker. A test pins that order. The broker's shutdown then:
  1. drains the queue and writes the final batch;
  2. **only if that succeeded within the shutdown timeout**, writes `clean_shutdown = '1'` in the
     same transaction as the final batch (if the flush fails or times out, e.g. disk full, it stays
     `'0'` and the next start rotates the epoch);
  3. runs `PRAGMA wal_checkpoint(TRUNCATE)`, so the `.db` file is self-contained for backups and
     volume snapshots and no WAL frame is written after the truncate;
  4. closes the database and releases the lock.
- **Why the epoch rotates after a crash:** up to one batch (about 50 ms) may never reach disk.
  Reusing the epoch would hand those lost offsets to new messages, and a client that had seen the
  lost ones would skip the new ones silently. A new epoch makes reconnecting clients resync
  instead, exactly as after a restart with the memory broker. `history_since` still serves the
  loaded entries by time.

## 6. Failure handling

The rule: live delivery never stops because of the disk.

- **Write errors or disk full:** keep the batch and retry on the next tick. Log the first failure,
  then at most once every 10 s while failures continue, and log recovery once. Queue overflow
  drops the oldest history ops (section 3) and counts them.
- **Corrupt database at startup:** only on SQLite's `SQLITE_CORRUPT` or `SQLITE_NOTADB`, or a
  failed `quick_check`: rename the file to `<path>.corrupt-<unix timestamp>` (with its `-wal` and
  `-shm` files), log a warning and start fresh with a new epoch. Never on `SQLITE_BUSY`,
  permission errors or a newer schema version: those exit with a clear message instead.
- **Unwritable path, or lock still held after 10 s:** exit at startup with a clear message.

## 7. Visibility

- Startup log line, e.g. `restored 4210 messages across 37 streams and 112 sessions (epoch abcd,
  clean shutdown) in 18 ms`, or `… (new epoch after an unclean shutdown) …`.
- Metrics, registered next to the existing broker metrics:
  - `sqlite_queue_size` (gauge)
  - `sqlite_batch_write_ms` (gauge: duration of the last batch write)
  - `sqlite_write_errors_total` (counter)
  - `sqlite_dropped_total` (counter: ops dropped on queue overflow, labelled by kind)

## 8. Compatibility record

AnyCable rejects `--broker=sqlite`, so the broker is recorded in `compat/deviations.md` as
**D-065** (Phase A reserves D-058 to D-064): a Cabby-only addition, observable only when the new
value is used. Pinning test: a conformance scenario through the binary that starts with
`--broker=sqlite`, restarts gracefully, and resumes history and a session. The Go arm records it as
not observable. Behaviour with `memory` and `nats` is unchanged; the translated suite must pass as
before.

## 9. Testing

- **Unit tests:**
  - batching by size and by time; queue overflow drops the oldest history op first, then session
    touches, then session commits, never exceeds its bound, and counts every drop;
  - `handle_broadcast_recorded` returns the assigned offset and timestamp, and `None` for
    transient messages; concurrent broadcasts each get their own offset;
  - loader: empty file; history within and beyond limit and TTL; a gap in the middle loads only
    the contiguous tail; a stream whose newest row isn't `max_seq` loads empty with offset
    `max_seq`; expired sessions skipped; restored sessions keep their original expiry;
  - a stream whose entries all expired, restarted within 10 × `history_ttl`, gives the next
    broadcast `max_seq + 1`;
  - epoch kept after a clean shutdown and rotated after an unclean one;
  - pruning by TTL, by limit and by deadline uses the indexes (check with `EXPLAIN QUERY PLAN`);
  - schema: fresh file, migration, newer version refused without renaming;
  - corrupt file moved aside; `SQLITE_BUSY` and permission errors are not;
  - file and directory modes are `0600` and `0700`;
  - `--presets=broker --broker=sqlite` keeps `sqlite`.
- **End-to-end through the binary** (`tests/features` style, temp directory):
  - broadcast, stop Cabby gracefully, restart; a client resuming with its offsets gets every
    missed message, and the epoch is unchanged;
  - its session restores without an RPC call;
  - after `kill -9`, the restart uses a new epoch and the client resyncs cleanly;
  - a second process on the same file exits with the lock error;
  - a session committed while clients disconnect during shutdown is on disk after the restart
    (pins the runner's shutdown order);
  - a final flush that fails (read-only file at shutdown) leaves `clean_shutdown = '0'`, and the
    next start rotates the epoch;
  - after a graceful shutdown there is no `-wal` file left behind.
- **Benchmark:** throughput and latency with `--broker=sqlite` match `--broker=memory` within run
  noise (Linux benchmark box, existing harness).

## 10. Docs and site (shipped with the release, not before)

- New guide "Keep history across restarts (SQLite)": flags, the Kamal volume, what survives a
  graceful restart versus a crash.
- Configuration page: `--broker=sqlite`, `--sqlite_path`, `[sqlite]`.
- Rails guide, Kamal section: the volume line.
- Landing page: one line in "How it works", e.g. "No Redis needed. History survives restarts with a
  single SQLite file."
- All of it in the site's six languages.

## 11. Delivery

- Built on its own branch of the public repo, after the history swap.
- Ships in **v0.2.0** alongside Phase A, since both touch configuration: one docs update and one set
  of release notes.
- Out of scope: the PostgreSQL broker and pub/sub (next spec) and the live inspector (the spec
  after that).
