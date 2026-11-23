# SQLite broker: stream history and sessions that survive restarts

Status: approved design, 2026-11-23. First of the "zero Redis" brokers; the PostgreSQL broker and
pub/sub follow in their own spec.

## 1. Goal

A single Cabby node keeps stream history and session-recovery state across restarts, with no
external service. Reconnecting clients resume from their stream offsets and restore their sessions
as if Cabby had never stopped.

Who it's for: Rails 8 / Kamal deployments that run on one server and often already use SQLite
(Solid Queue, Solid Cache, Solid Cable). They get durable history without adding Redis or NATS.

What it is not: a multi-node broker (that's the PostgreSQL work), and not a Redis-style broker.
It reuses a local file and stays out of AnyCable Pro's territory (see `docs/roadmap.md`).

Honest scope note for the docs: in Kamal, Cabby runs as an accessory, and accessories aren't
restarted on normal app deploys. The value shows when Cabby itself restarts: an upgrade, a
`kamal accessory reboot`, a server reboot or a crash.

## 2. User-facing behaviour

```
cabby --broker=sqlite --sqlite_path=/var/lib/cabby/cabby.db ...
```

- `--broker=sqlite` is a new value next to `memory` and `nats`.
- `--sqlite_path` is required with `--broker=sqlite`. Environment variable `CABBY_SQLITE_PATH`;
  config file section `[sqlite]`, key `path`. These are Cabby-only settings, so they have no
  `ANYCABLE_*` alias; the names work the same before and after Phase A's renaming.
- Cabby creates the file and its parent directory if missing. If the path can't be created or
  written, Cabby exits at startup with a clear error.
- Retention uses the existing broker settings unchanged: `history_limit`, `history_ttl`,
  `sessions_ttl`. Switching from `memory` to `sqlite` changes nothing else.
- What survives a restart: the broker epoch, stream history (within limit and TTL) and session
  state (within `sessions_ttl`). Presence is **not** persisted: every connection drops on restart,
  so saved presence would only show members who are gone.
- Kamal: the Cabby accessory needs a volume, e.g. `volumes: ["cabby_data:/var/lib/cabby"]`.

## 3. Architecture

`SqliteBroker` implements `cabby_core::broker::Broker` by wrapping the existing in-memory broker
(`broker::Memory`). The memory broker stays the live path: it serves every read, assigns offsets
and keeps fan-out at full speed. Two new pieces sit beside it.

- **Persister:** one background thread fed by a bounded, in-order queue.
  - Every 50 ms, or as soon as 1,000 changes are queued, it writes one batch in a single SQLite
    transaction: new history entries, session commits and touches, and the epoch.
  - On a slower timer (every 30 s) it prunes rows past `history_ttl`, beyond `history_limit` per
    stream, and sessions past their expiry.
- **Loader:** runs once at startup, before Cabby accepts connections.
  - Reads the saved epoch and installs it in the memory broker, so clients' offsets stay valid.
  - Rebuilds each stream's history within `history_limit` and `history_ttl`.
  - Restores sessions that haven't expired.
  - A new, empty file starts with a fresh epoch, exactly like `memory`.

This is a write-behind design. It was chosen over write-through (a disk write on every message
costs throughput and latency, Cabby's headline numbers) and snapshot-on-shutdown (loses everything
on a crash).

The broker trait is synchronous, so the persister runs on its own OS thread with a blocking SQLite
connection. Nothing blocks the Tokio runtime or the broadcast path; enqueueing is a non-blocking
channel send.

Dependency: `rusqlite` with the `bundled` feature (SQLite is public domain). No system library is
needed, and it cross-compiles with the existing `cargo zigbuild` release builds (glibc 2.28). The
feature is on by default; the release binary grows by about 1 MB. Add its notice to the cargo-about
output (Phase A Task 18) when that lands.

## 4. Storage

Pragmas: `journal_mode=WAL`, `synchronous=NORMAL`, `busy_timeout` of a few seconds. `NORMAL` is
safe against application crashes; a power cut can lose the last few commits, which is within the
write-behind promise.

```sql
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,   -- 'schema_version', 'epoch'
  value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE history (
  stream TEXT    NOT NULL,
  seq    INTEGER NOT NULL,  -- the stream offset ("offset" is an SQL keyword)
  ts     INTEGER NOT NULL,  -- unix milliseconds
  data   TEXT    NOT NULL,
  PRIMARY KEY (stream, seq)
) WITHOUT ROWID;

CREATE TABLE sessions (
  sid        TEXT    PRIMARY KEY,
  data       BLOB    NOT NULL,  -- the Cacheable bytes commit_session receives
  expires_at INTEGER NOT NULL   -- unix milliseconds
) WITHOUT ROWID;
```

Schema version starts at 1. A newer Cabby migrates older files forward at startup. An older Cabby
refuses to open a file whose version is newer than it knows, with a clear error, instead of
corrupting it.

## 5. Data flow

- **Broadcast:** `handle_broadcast` goes to the memory broker as today (offset assigned, fan-out),
  then enqueues `(stream, offset, ts, data)`, stored as `(stream, seq, ts, data)`.
- **Session commit:** `commit_session` stores in memory as today and enqueues an upsert with
  `expires_at = now + sessions_ttl`. `touch_session` enqueues an expiry update.
- **Reads:** `history_from`, `history_since`, `restore_session` and `peak` are served from memory.
  After startup the memory broker holds everything the file holds, within limits.
- **Startup:** Loader (section 3), then the server starts listening. A client reconnecting with its
  stream offsets and session ID gets missed messages and its restored subscriptions.
- **Graceful shutdown:** stop accepting connections, drain the queue, write the final batch, close
  the database, all within the existing shutdown timeout.
- **Crash:** at most the last unflushed batch (about 50 ms of history) is lost. The next start
  resumes from disk with the same epoch. Clients that already saw the lost messages carry on;
  clients that missed them can't get them from history, the same limitation as messages beyond
  the memory broker's limits.
- **One process per file:** Cabby takes an exclusive lock on the database at startup. If another
  process holds it, Cabby retries for up to 10 seconds (covering a restart overlap), then exits
  with an error naming the path.

## 6. Failure handling

The rule: live delivery never stops because of the disk.

- **Write errors or disk full:** log the error, keep the batch, retry on the next tick. The queue
  is bounded at 100,000 changes; when full, the oldest unsaved history entries are dropped and
  counted. Messages keep flowing to clients.
- **Corrupt database at startup:** rename the file to `<path>.corrupt-<unix timestamp>` (and its
  `-wal`/`-shm` files), log a warning, and start fresh with a new epoch. Clients resync, as after a
  restart with the memory broker. Availability beats history for a real-time server.
- **Unwritable path, or lock still held after 10 s:** exit at startup with a clear message.

## 7. Visibility

- Startup log line, e.g. `restored 4210 messages across 37 streams and 112 sessions (epoch abcd) in 18 ms`.
- Metrics, registered next to the existing broker metrics:
  - `sqlite_queue_size` (gauge)
  - `sqlite_batch_write_ms` (gauge: duration of the last batch write)
  - `sqlite_write_errors_total` (counter)
  - `sqlite_dropped_total` (counter)

## 8. Compatibility record

AnyCable rejects `--broker=sqlite`, so the broker is recorded in `compat/deviations.md` as
**D-065** (Phase A reserves D-058 to D-064): a Cabby-only addition, observable only when the new
value is used. Pinning test: a conformance scenario through the binary that starts with
`--broker=sqlite`, restarts, and resumes history and a session. The Go arm records it as not
observable. Behaviour with `memory` and `nats` is unchanged; the translated suite must pass as
before.

## 9. Testing

- **Unit tests:** batching by size and by time; the loader (empty file, history within and beyond
  limits and TTL, expired sessions skipped); pruning; schema version (fresh file, migration, newer
  version refused); corrupt file moved aside; queue overflow drops and counts.
- **End-to-end through the binary** (`tests/features` style, temp directory):
  - broadcast, stop Cabby gracefully, restart; a client resuming with its offsets gets every
    missed message;
  - its session restores without an RPC call;
  - the epoch is the same before and after;
  - after `kill -9`, at most the last batch window is missing and the epoch is unchanged;
  - a second process on the same file exits with the lock error.
- **Benchmark:** throughput and latency with `--broker=sqlite` match `--broker=memory` within run
  noise (Linux benchmark box, existing harness).

## 10. Docs and site (shipped with the release, not before)

- New guide "Keep history across restarts (SQLite)": flags, the Kamal volume, what survives a
  restart versus a crash.
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
