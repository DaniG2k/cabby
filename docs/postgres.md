# PostgreSQL adapters

Cabby can use PostgreSQL for everything several nodes need to share, so an app that already runs
Postgres (Rails 8, Laravel, Django, ...) can run more than one Cabby node without Redis or NATS:

| Flag | What Postgres does |
|---|---|
| `--pubsub=postgres` | Cross-node pub/sub over `LISTEN`/`NOTIFY`: every node delivers every broadcast to its own clients |
| `--broadcast_adapter=postgres` | Broadcasts from SQL: the app inserts into an outbox table, inside its own transaction if it likes |
| `--broker=postgres` | Stream history, offsets and session recovery shared by every node |

The three are independent: use one, two or all three, and combine them with the other adapters
(for example `--broadcast_adapter=http,postgres` with `--broker=memory`). These flags exist only
in Cabby (deviations D-070 and D-071 in [`compat/deviations.md`](../compat/deviations.md)).

## Requirements

- PostgreSQL 12 or newer.
- **A direct or session-mode connection.** `LISTEN` belongs to a database session, so Cabby
  keeps one long-lived connection for it. PgBouncer in **transaction** (or statement) pooling
  mode hands each transaction to a different server connection and loses the `LISTEN`; point
  Cabby at Postgres directly, or at a PgBouncer pool in **session** mode. (The advisory locks
  the broadcaster and the broker take are transaction-level, so they alone would work with
  transaction pooling.)
- A role that can create tables in the schema, or the tables created beforehand (see
  [Creating the tables yourself](#creating-the-tables-yourself)). With
  `--postgres_auto_migrate` (the default) Cabby creates all six tables whichever Postgres
  adapters are on, and creates the schema first if it doesn't exist, which needs the `CREATE`
  privilege on the database. Since PostgreSQL 15 only the owner of the `public` schema (by
  default the database owner) may create tables in it: a role that doesn't own it needs
  `GRANT CREATE ON SCHEMA public TO <role>`, or a schema of its own
  (`--postgres_schema=cabby` with `CREATE SCHEMA cabby AUTHORIZATION <role>`).

Each node opens up to 10 connections with all three adapters: one `LISTEN` connection and two
command connections for pub/sub, one `LISTEN` connection and one claim connection for the
broadcaster, four command connections and one maintenance connection for the broker (plus one
for a moment at startup, for the migration). Size
`max_connections` (or the PgBouncer pool) for your node count. Each connection sets
`application_name`, so `pg_stat_activity` shows what it is for: `cabby pubsub <channel>`,
`cabby pubsub-listen <channel>`, `cabby broadcast <channel>`, `cabby broadcast-listen <channel>`,
`cabby broker <channel>`, `cabby broker-maintenance <channel>` and `cabby migrate` (with
`<channel>` the `--postgres_channel` value; at most 63 bytes are kept).

## Quick start

```sh
cabby --pubsub=postgres --broadcast_adapter=http,postgres --broker=postgres \
  --postgres_url="postgres://cabby:secret@db.internal:5432/app?sslmode=verify-full" \
  --rpc_host=http://localhost:3000/_anycable --secret=…
```

At startup each adapter logs a line without credentials:

```text
INF Using Postgres broker: db.internal:5432/app (history limit: 100, history ttl: 300s, sessions ttl: 300s)
INF Starting Postgres pub/sub: db.internal:5432/app (channel: cabby_internal, table: public.cabby_pubsub_overflow) context=pubsub provider=postgres
INF Postgres broker is ready (schema: public, table prefix: cabby_) context=broker provider=postgres epoch=Xm7H
INF Starting Postgres broadcaster: db.internal:5432/app (table: public.cabby_broadcasts, channel: cabby_internal_in) context=broadcast provider=postgres
```

With `--postgres_auto_migrate` (the default) Cabby creates its tables on the first start. Every
node runs the same migration inside an advisory lock, so nodes starting together don't race.

Once the tables and indexes exist, a start locks none of them exclusively: it neither waits for
sessions writing to them nor holds the writes of running nodes back. A start that does need a
lock another session holds (to create an index that was dropped, for example) waits for it at
most 5 seconds, and at most 15 seconds for another node's migration. It then stops with an error
naming the statement:

```text
failed to start Postgres broker, cause: timed out after 5s waiting for a lock to run CREATE INDEX IF NOT EXISTS "cabby_history_ts_sec" ON "public"."cabby_history": another session holds a conflicting lock (see pg_locks and pg_stat_activity)
```

While a node starts, `SELECT pid, pg_blocking_pids(pid), query FROM pg_stat_activity WHERE
application_name = 'cabby migrate'` shows which session it waits for. A statement may take 30
seconds (create a missing index of a very large table yourself, with `CREATE INDEX
CONCURRENTLY`), and preparing the tables 60 seconds in all: a server that stops answering ends
the start with `timed out after 60s: no answer from db.internal:5432/app`. With
`--postgres_auto_migrate=false` the same limits apply to the checks.

## Settings

Cabby-only settings: they have no `ANYCABLE_*` names.

| Flag | Environment | `[postgres]` key | Default | Purpose |
|---|---|---|---|---|
| `--postgres_url` | `CABBY_POSTGRES_URL` | `url` | (empty) | Connection URI (`postgres://user:password@host:5432/db?sslmode=...`) or libpq key/value string. Required by every Postgres adapter: a start without it stops with `--pubsub=postgres needs a connection URL: ...` |
| `--postgres_schema` | `CABBY_POSTGRES_SCHEMA` | `schema` | `public` | Schema for Cabby's tables |
| `--postgres_table_prefix` | `CABBY_POSTGRES_TABLE_PREFIX` | `table_prefix` | `cabby_` | Prefix of Cabby's table names (may be empty; at most 37 bytes) |
| `--postgres_channel` | `CABBY_POSTGRES_CHANNEL` | `channel` | `cabby_internal` | Base `LISTEN`/`NOTIFY` channel name (at most 59 bytes) |
| `--postgres_auto_migrate` | `CABBY_POSTGRES_AUTO_MIGRATE` | `auto_migrate` | `true` | Create the tables at startup; `false` for roles that may not create tables |
| `--postgres_keepalive_interval` | `CABBY_POSTGRES_KEEPALIVE_INTERVAL` | `keepalive_interval` | `30` | Seconds between `SELECT 1` heartbeats on the `LISTEN` connections (0: none) |
| `--postgres_tls_verify` | `CABBY_POSTGRES_TLS_VERIFY` | `tls_verify` | `true` | Verify the server certificate and host name with `sslmode=prefer` or `require` |
| `--postgres_tls_ca_cert_path` | `CABBY_POSTGRES_TLS_CA_CERT_PATH` | `tls_ca_cert_path` | (empty) | PEM file with the CA certificates to trust (default: the system roots; `sslrootcert=` in the URL also works, and `sslrootcert=system` means the system roots) |
| `--postgres_unlogged` | `CABBY_POSTGRES_UNLOGGED` | `unlogged` | `true` | Create every table but the outbox as `UNLOGGED` |

Schema, prefix and channel are SQL identifiers: they must match `^[A-Za-z_][A-Za-z0-9_]*$` (the
prefix may also be empty), and Cabby refuses to start otherwise. They are used as written, quoted,
so `App` and `app` are different schemas.

```toml
[postgres]
url = "postgres://cabby:secret@db.internal:5432/app?sslmode=verify-full"
schema = "cabby"
```

### TLS

Cabby uses rustls (no OpenSSL) and reads `sslmode` from the URL:

| `sslmode` | TLS | Certificate check |
|---|---|---|
| `disable` | no | none |
| `prefer` (default) | if the server offers it | chain and host name, unless `--postgres_tls_verify=false` |
| `require` | yes | chain and host name, unless `--postgres_tls_verify=false` |
| `verify-ca` | yes | chain only |
| `verify-full` | yes | chain and host name |

Unlike libpq, `prefer` and `require` verify the certificate by default. A certificate that
doesn't verify is a connection error: Cabby never falls back to a plaintext connection. So with
the default `prefer`, a server whose certificate isn't signed by a CA in the system roots is
rejected; managed Postgres services often use their own CA. Download it and set
`--postgres_tls_ca_cert_path` (or `sslrootcert=` in the URL), or, on a network you trust, set
`--postgres_tls_verify=false`. When verification is on and there is no CA certificate at all
(no file given and none in the system roots), Cabby stops at startup and names these settings;
`sslmode=disable` reads no CA.

## Pub/sub (`--pubsub=postgres`)

Each node publishes with `NOTIFY` and listens with `LISTEN` on a dedicated connection. Streams
are hashed into 16 channels (`cabby_internal_0` to `cabby_internal_15`); a node listens to a
channel while one of its clients is subscribed to a stream hashed into it, and delivers only the
streams it has subscribers for. Subscribing doesn't wait for the server: the `LISTEN` for a new
channel is sent right away, and a message published before it takes effect (usually a few
milliseconds) isn't delivered live to that node; clients that request history get it from
there, as with Redis pub/sub. Remote commands (disconnects through the API, for example) use
`cabby_internal_cmd`, which every node listens to.

`NOTIFY` payloads must stay under 8000 bytes. A message larger than 7500 bytes (after its
envelope) is stored in `cabby_pubsub_overflow` for 60 seconds and the notification carries the
row's ID; receivers read it from there. A node deletes expired rows every 30 seconds, so a node
that falls more than 60 seconds behind skips such a message (with a warning).

The task reading the `LISTEN` connection hands each notification on at once, also while a
`LISTEN`, `UNLISTEN` or heartbeat on that connection is waiting for its answer. Each node keeps
up to 8192 received messages waiting for delivery to its clients; a node that can't keep up
drops the messages that find the queue full, counts them (`postgres_dropped_total`) and logs a
warning, and reading the connection never stops on that account.

PostgreSQL keeps notifications that a listening session hasn't read yet in one queue of 8 GB
for the whole server. If a listener stops reading, the queue fills, and once it is full every
commit that sends a notification fails, including inserts into `cabby_broadcasts` (through the
trigger) and Cabby's own publications. `SELECT pg_notification_queue_usage()` shows how full it
is (0 to 1).

The `LISTEN` connection is checked with `SELECT 1` every `--postgres_keepalive_interval`, so a
connection that died without closing is noticed. After any failure Cabby reconnects with backoff
(250 ms, then about 2, 4, 8, ... seconds, at most 30 s; the delays start over once a
connection has stayed up for 30 s) and listens again to every channel. Messages published
while a node is disconnected don't reach it; with `--broker=postgres` (or any broker with
history) clients catch up from history when they resubscribe or reconnect.

Publications wait in a queue of 1024 per node; when Postgres is slower than the broadcasts, the
broadcasting caller waits for room, so memory stays bounded.

## Broadcasting from SQL (`--broadcast_adapter=postgres`)

Insert the usual broadcast JSON into `cabby_broadcasts`:

```sql
INSERT INTO cabby_broadcasts (payload)
VALUES ('{"stream":"chat_42","data":"{\"text\":\"hello\"}"}');
```

`payload` is the same JSON as an HTTP broadcast: one message (`stream`, `data` as a JSON string,
optional `meta`) or an array of them. The table's statement-level `AFTER INSERT` trigger
notifies `cabby_internal_in` once per insert statement; one node claims the rows
(`SELECT ... FOR UPDATE SKIP LOCKED`, 100 at a time, in one transaction), hands each one to the
broker, which stores it (with `--broker=postgres`) and fans it out through pub/sub, and deletes
the rows it handled when the transaction commits. Nodes also poll every 5 seconds, so a row
inserted while a node was reconnecting is not left behind.

- **At least once.** A row is deleted in the same transaction that handed it to the broker.
  When the broker fails (it can't reach Postgres, for example), the rows handled before it are
  deleted, the failed row and the rows after it stay, and the node tries that row again with a
  backoff of at most 5 seconds, for as long as it takes: no row is dropped because of a broker
  error. Errors are logged at the 1st, 2nd, 4th, 8th, ... failure in a row and counted in
  `postgres_broadcast_retries_total`. A row that is not JSON is deleted and logged without
  reaching the broker. A row that is JSON but not a broadcast (for example `data` as an object
  instead of a string) is logged as `failed to parse pubsub message ... error="invalid
  publication format"` and deleted too; nothing is retried for it.
- **Duplicates** can happen: rows already handed to the broker are handed again after a node
  crashes mid-batch, after its claim connection is lost or its commit fails (the transaction
  rolls back), when the broker's query timed out but Postgres ran it afterwards (see
  [Broker](#broker---brokerpostgres)), and when a row holding an array of messages fails partway
  (the messages before the failed one are broadcast again on the retry).
- **In order.** One node drains the table at a time (a transaction advisory lock; the server
  ends a claim transaction left idle for 60 s), so committed rows are broadcast in order of
  their `id`, and offsets follow that order. A batch hands rows to the broker for at most
  10 seconds, then commits the rows it handled and goes on in a new transaction, so a slow
  broker never makes a batch roll back and start over. A node that finds the lock taken tries again a
  second later.
- **Transactional.** The row and its notification only exist once the transaction commits. A
  transaction that rolls back broadcasts nothing.

When Cabby's role owns the tables and the app connects as another role, the app's role needs to
be allowed to insert into the outbox (the trigger and `pg_notify` need nothing more):

```sql
GRANT USAGE ON SCHEMA public TO app;
GRANT INSERT ON public.cabby_broadcasts TO app;
GRANT USAGE ON SEQUENCE public.cabby_broadcasts_id_seq TO app;
```

### From Rails (ActiveRecord)

```ruby
class CabbyBroadcast < ApplicationRecord
  self.table_name = "cabby_broadcasts"   # "<schema>.<prefix>broadcasts" if you changed them

  def self.to(stream, data)
    create!(payload: { stream: stream, data: data.to_json }.to_json)
  end
end

ApplicationRecord.transaction do
  message = room.messages.create!(body: params[:body], user: current_user)
  CabbyBroadcast.to("chat_#{room.id}", { id: message.id, body: message.body })
end
# Nothing is broadcast if the transaction rolls back.
```

For Turbo Streams, broadcast the rendered stream action to the signed stream name, as
`Turbo::StreamsChannel.broadcast_*` would. The `anycable-rails` gem can keep broadcasting over
HTTP alongside: `--broadcast_adapter=http,postgres` takes both.

### Direct `NOTIFY`

```sql
SELECT pg_notify('cabby_internal_in', '{"stream":"chat_42","data":"{\"text\":\"typing\"}"}');
```

A JSON payload sent straight to the channel (no table) reaches every node, and each node delivers
it to its own clients: nothing is stored in history and there is no offset, like the legacy
`redis` broadcaster. The payload must be under 8000 bytes. Use the table when the message should
be in history or part of a transaction.

## Broker (`--broker=postgres`)

The broker keeps stream history and sessions in Postgres, so a client that reconnects to any
node resumes its session and catches up on what it missed.

- **Offsets.** Each broadcast takes the stream's next offset and is stored in one statement;
  offsets are gap-free and increase across nodes.
- **History** follows `--history_limit` and `--history_ttl`, with the same answers as the memory
  broker (unknown epoch, missing stream, an offset older than the oldest kept one, an offset past
  the latest). A stream keeps its offsets for 10 times `--history_ttl` after its last message.
- **Sessions** are kept for `--sessions_ttl` after the last commit or touch.
- **Time** is the database's clock, so nodes with skewed clocks agree on what has expired.
- **Epoch.** The first node to start stores a random epoch in `cabby_meta`; every node uses it.
- **Pruning.** Every 30 seconds one node (holding an advisory lock) deletes expired history,
  streams and sessions and trims history beyond `--history_limit`, 10000 rows per transaction,
  so a large backlog is cleared over a few passes.
- **Errors.** When Postgres is unreachable, broadcasts fail (logged and counted; they are not
  delivered without an offset) and session recovery falls back to a new session, as with the NATS
  broker. An HTTP broadcast then answers `501`, the status the HTTP broadcaster gives for any
  broker failure.
- **Timeouts.** A query has 10 seconds. A broadcast that timed out has failed for Cabby, but
  Postgres may still run its statement afterwards (when the lock it waited for is released, for
  example): the message then has an offset and is in history, but isn't delivered live; clients
  get it when they catch up. If that broadcast is retried (an outbox row, a `redisx` entry, an
  HTTP request the app sends again), it is stored twice, with two offsets.
- **Presence** is not supported with `--broker=postgres` (as with `--broker=nats`).

### `UNLOGGED` tables

By default every table but the outbox is `UNLOGGED`: writes skip the write-ahead log, which makes
them much cheaper, and PostgreSQL empties these tables after a crash (not after a clean restart).
The epoch goes with them, so after a database crash Cabby creates a new epoch and clients resync
cleanly instead of resuming from offsets that no longer exist. `UNLOGGED` tables are not copied to
streaming replicas. With `--postgres_unlogged=false` Cabby creates regular tables; to switch
existing ones, run `ALTER TABLE ... SET LOGGED` (or `SET UNLOGGED`).

## Creating the tables yourself

With `--postgres_auto_migrate=false` Cabby only checks that the tables it needs exist and stops at
startup with a message naming the first one missing:

```text
failed to start subscriber, cause: failed to prepare the Postgres schema, cause: table public.cabby_pubsub_overflow is missing: create it with the DDL in docs/postgres.md or set --postgres_auto_migrate=true
```

This is the DDL Cabby runs for the defaults (schema `public`, prefix `cabby_`, channel
`cabby_internal`, `--postgres_unlogged=true`). Change the names if you changed those settings,
drop `UNLOGGED` for logged tables, and keep the channel in the trigger function in step with
`--postgres_channel`:

<!-- ddl:start -->
```sql
CREATE UNLOGGED TABLE IF NOT EXISTS "public"."cabby_meta" (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS "public"."cabby_pubsub_overflow" (
  id         BIGSERIAL PRIMARY KEY,
  payload    TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS "cabby_pubsub_overflow_expires_at" ON "public"."cabby_pubsub_overflow" (expires_at);

CREATE TABLE IF NOT EXISTS "public"."cabby_broadcasts" (
  id         BIGSERIAL PRIMARY KEY,
  payload    TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION "public"."cabby_broadcasts_notify"() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('cabby_internal_in', '');
  RETURN NULL;
END
$$;

CREATE UNLOGGED TABLE IF NOT EXISTS "public"."cabby_streams" (
  stream       TEXT PRIMARY KEY,
  max_seq      BIGINT NOT NULL,
  deadline_sec BIGINT NOT NULL,
  last_ts_sec  BIGINT NOT NULL
);

CREATE UNLOGGED TABLE IF NOT EXISTS "public"."cabby_history" (
  stream TEXT NOT NULL,
  seq    BIGINT NOT NULL,
  ts_sec BIGINT NOT NULL,
  data   TEXT NOT NULL,
  PRIMARY KEY (stream, seq)
);

CREATE INDEX IF NOT EXISTS "cabby_history_ts_sec" ON "public"."cabby_history" (ts_sec);

CREATE UNLOGGED TABLE IF NOT EXISTS "public"."cabby_sessions" (
  sid           TEXT PRIMARY KEY,
  data          BYTEA NOT NULL,
  expires_at_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS "cabby_sessions_expires_at_ms" ON "public"."cabby_sessions" (expires_at_ms);

CREATE TRIGGER "cabby_broadcasts_notify" AFTER INSERT ON "public"."cabby_broadcasts" FOR EACH STATEMENT EXECUTE FUNCTION "public"."cabby_broadcasts_notify"();

INSERT INTO "public"."cabby_meta" (key, value) VALUES ('schema_version', '1') ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
```
<!-- ddl:end -->

Without the trigger the broadcaster still works, by polling every 5 seconds, and logs a warning at
startup. The pub/sub adapter needs `cabby_pubsub_overflow`, the broadcaster `cabby_broadcasts`, and
the broker `cabby_meta`, `cabby_streams`, `cabby_history` and `cabby_sessions`. The role Cabby
connects as needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on them and `USAGE` on the sequences.

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `postgres_errors_total` | counter | Failed Postgres operations (queries, publications, lost `LISTEN` connections) |
| `postgres_query_ms` | gauge | Duration of the last operation, in milliseconds |
| `postgres_spillover_total` | counter | Pub/sub messages sent through `cabby_pubsub_overflow` |
| `postgres_reconnects_total` | counter | `LISTEN` connections re-established |
| `postgres_claimed_broadcasts_total` | counter | Rows from `cabby_broadcasts` handed to the broker |
| `postgres_broadcast_retries_total` | counter | Outbox rows retried after the broker failed to take them |
| `postgres_dropped_total` | counter | Received messages dropped because the node was not keeping up |

With Prometheus they have the usual `anycable_go_` prefix.

## Deploying

[`deploy-kamal.md`](deploy-kamal.md) has a Kamal example with two Cabby nodes on the app's
Postgres.
