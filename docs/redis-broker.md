# Redis broker (`--broker=redis`)

The Redis broker keeps stream history and session state in Redis, so several Cabby nodes share
them. A client that reconnects to any node catches up on the messages it missed and gets its
session back. Deployments that already run Redis get this without NATS JetStream. The Redis
broker is a Cabby addition (deviation D-069 in [`compat/deviations.md`](../compat/deviations.md)):
the open-source AnyCable server has no such broker and stops at startup with
`Unsupported broker adapter: redis`.

## Quick start

```sh
cabby --broker=redis --redis_url=redis://redis.internal:6379/0 --broadcast_adapter=http,redisx
```

Run every node with the same Redis URL and the same key prefix. Without `--pubsub`, the Redis
broker also selects Redis pub/sub, so a broadcast received by one node reaches the clients of
every node. To use NATS for pub/sub instead, set `--pubsub=nats`.

The history and session settings are the usual broker ones: `--history_limit` (messages per
stream, default 100), `--history_ttl` (seconds, default 300) and `--sessions_ttl` (seconds,
default 300).

## Which broadcasts are stored

Only broadcasts that go through the broker are stored in history: those received over HTTP
(`http`) and from Redis Streams (`redisx`). The legacy `redis` (PUBLISH) and `nats` broadcast
adapters deliver messages to the node directly, as in the AnyCable server: clients get them
live, but they are not stored and can't be caught up on. Cabby logs a warning at startup when you
set the broadcast adapters to include them with a history broker (with the default list or a
preset's, the line is logged at debug level):

```text
WRN broadcasts through the redis adapter are delivered live and not stored in history; use http or redisx to keep history
```

The broker doesn't read broadcasts from Redis itself. With HTTP-only broadcasts, Cabby logs
`WRN broadcasts are HTTP only; the Redis broker does not read broadcasts from Redis (set
CABBY_BROADCAST_ADAPTER to add redisx)`, in case your app broadcasts through Redis.

## Settings

| Flag | Variable | `[redis_broker]` key | Default | Meaning |
|---|---|---|---|---|
| `--redis_broker_key_prefix` | `CABBY_REDIS_BROKER_KEY_PREFIX` | `key_prefix` | `__cabby__` | Prefix of every key the broker writes |
| `--redis_broker_timeout` | `CABBY_REDIS_BROKER_TIMEOUT` | `timeout` | `5` | How long one Redis operation may take (seconds) |

Both are Cabby-only settings: they have no `ANYCABLE_*` variable. The connection itself comes
from the `[redis]` settings that pub/sub and broadcasting use too: `--redis_url` (a single
instance, or a comma-separated list for Redis Cluster), `--redis_sentinels`, and the
`--redis_tls_*` flags.

```toml
[redis]
url = "rediss://redis.internal:6380/0"

[redis_broker]
key_prefix = "myapp"
timeout = 5
```

Give each application its own prefix to share one Redis between applications. A prefix must
not be empty or contain `{` or `}`.

### Redis ACL

All keys start with `<prefix>:`, so a user can be limited to them. The broker needs these
commands (add `+select` for a database other than 0):

```text
ACL SETUSER cabby on >secret ~myapp:* resetchannels +eval +evalsha +script|load +info +time +hmget +hset +exists +xinfo|stream +xadd +xrevrange +expire +get +set
```

Redis pub/sub and the Redis broadcast adapters need their own commands and channels on top
(`SUBSCRIBE`, `PUBLISH`, the stream commands of `redisx`).

### Memory policy

Use `maxmemory-policy noeviction` on the Redis that holds the broker's keys. Every broker key
has a TTL, so the `volatile-*` policies evict them as readily as the `allkeys-*` ones. When
Redis is full under `noeviction`, broadcasts fail with an error instead of losing history
silently.

## What it stores

| Key | Type | Expires after |
|---|---|---|
| `<prefix>:{s:<stream>}:meta` | hash: the stream's last offset, its epoch, the Redis instance that wrote it | 10 x `history_ttl` after the last message |
| `<prefix>:{s:<stream>}:hist` | stream, one entry per message (ID `<offset>-0`, with its epoch) | `history_ttl` after the last message |
| `<prefix>:sess:<sid>` | string, a session's state | `sessions_ttl` after the last commit or touch |

The `{s:<stream>}` hash tag puts a stream's keys in one Redis Cluster slot: a broadcast is one
Lua script on one node, in every topology, and different streams spread over the cluster.

The offset and epoch outlive the history: a stream that is quiet for longer than `history_ttl`
loses its messages but keeps counting from where it was, so offsets never repeat for a client
that is still around. Timestamps come from the Redis server's clock, so all nodes agree on what
has expired.

Redis may keep more than `history_limit` entries per stream (`MAXLEN ~` trims whole internal
nodes). Reads return exactly the newest `history_limit` messages that are younger than
`history_ttl`, as the memory broker does. With `--history_limit=0` nothing is stored; broadcasts
still get offsets.

## Epochs

An epoch tells a client whether its position in a stream still means something. With the Redis
broker every stream has its own epoch, which all nodes share. A stream gets a new epoch
whenever its offsets could repeat:

- when it is created;
- when its state (offset and epoch) was lost while its history stayed (an eviction): its
  offsets continue after the newest one Redis generated for it;
- when it was lost entirely (a restart without persistence): it starts again at offset 1;
- when the Redis instance that holds it changed: a restart (a new `run_id`, for instance after
  loading a snapshot that brings the counter back to an older value) or a failover (a new
  `master_replid`, after which the newest writes may be missing). The first broadcast after the
  change gives the stream its new epoch; every node uses it from then on.

`master_replid` also changes in cases that lose nothing: when a master frees its replication
backlog (no replicas for `repl-backlog-ttl`), and when a Redis Cluster slot moves to another
node. The streams concerned get a new epoch at their next broadcast, and their clients resync
once.

Every history entry records its epoch. A client asking for history from a position
(`history_from`) gets only entries of the stream's current epoch; with an older epoch it gets
`unknown epoch` and resyncs, so a reused offset is never taken for one it has already seen.
Between an instance change and the stream's next broadcast, every position in the stream gets
`unknown epoch`, even one at or below the offset the snapshot brought back: the messages after
it may be lost, and the client resyncs instead of confirming an incomplete history. History
since a time (`history_since`) returns every kept message, each with the epoch it was written
in. `redis_broker_epoch_rotations_total`
counts the streams that got a new epoch because their state was lost while their history stayed
or because their Redis instance changed; Cabby logs a warning about it at most once a minute. A
stream lost entirely looks like a new one.

If only a stream's history is lost (evicted), the stream keeps its epoch and offset: a client
behind the remaining history gets `requested offset couldn't be found` and resyncs.

Streams are checked one by one: at their next broadcast, which gives them the new epoch, and at
every positional read. On a connection to a single node (standalone or Sentinel), Cabby reads
the instance (`INFO server`, `INFO replication`) when it connects and at most once a second
while it is in use, and passes it to the scripts; on a Redis Cluster connection, the scripts
read it on the node that holds the stream. If Redis reports no `run_id` or `master_replid`,
Cabby logs a warning at startup: restart and failover detection is then off.

## Presence

The Redis broker has no presence yet, like the NATS broker. Presence commands get an error,
Pusher `presence-` channels can't be subscribed to, and the users endpoints of the API and the
Pusher REST API answer `501`. Use the memory broker on a single node if you need presence.

## When Redis fails

Each operation has a timeout (`--redis_broker_timeout`). When Redis is unreachable or doesn't
answer in time:

- a broadcast fails with an error and is not delivered; the error is logged. A broadcast that
  timed out may still have been stored, if Redis ran it after the timeout. A `redisx`
  broadcast that fails is not acknowledged, so it is delivered again later: it may then be
  stored twice, with two offsets. Each broadcast carries a token, so a resend of the same
  broadcast by the Redis Cluster client (after a dropped connection) is stored once;
- history reads fail, so a reconnecting client resyncs;
- session commits, restores and touches fail; a client whose session can't be restored is
  authenticated again, as a new session;
- after a connection error, reads and session calls are tried once more on a new connection;
  broadcasts are not, since their offset may already have been assigned. A node that can't
  take writes (`READONLY`, `LOADING`, `MASTERDOWN`, also in Redis 6.2's `ERR Error running
  script ... -READONLY ...` form: a demoted master, a replica that is loading or cut off from
  its master) has run nothing, so every operation, broadcasts included, is tried once more on a
  new connection; if that one answers the same, the back-off below starts;
- operations wait for one connection attempt at a time. After a timeout or a failed attempt,
  they fail at once for about a second (`Redis is unavailable after a failure; retrying after a
  short back-off`) instead of each waiting the full timeout again. These failures are logged at
  most once a second, with the number not logged since; the metrics count each one.

At startup, Cabby connects and loads the scripts. If that fails within the timeout, the server
stops with `failed to start broker, cause: failed to start the Redis broker, cause: ...`.

## Logs and metrics

At startup:

```text
INF Using Redis broker: redis.internal:6379 (key prefix: myapp, history limit: 100, history ttl: 300s, sessions ttl: 300s)
INF Redis broker is ready (Redis 7.4.12, epochs per stream) context=broker provider=redis
```

Failed operations are logged at error level with `context=broker provider=redis`.

| Metric | Type | Meaning |
|---|---|---|
| `redis_broker_errors_total` | counter | Failed operations, timeouts included |
| `redis_broker_timeouts_total` | counter | Operations that ran out of time |
| `redis_broker_reconnects_total` | counter | Connections opened again after a lost or failed one |
| `redis_broker_epoch_rotations_total` | counter | Streams that got a new epoch because their state was lost or their Redis instance changed |
| `redis_broker_operation_ms` | gauge | Duration of the most recent operation (milliseconds), like `sqlite_batch_write_ms` |

## Redis Cluster and Sentinel

The broker uses the same connection code as Redis pub/sub and broadcasting:

- for Sentinel, set `--redis_sentinels` and give the master name in `--redis_url`;
- for Cluster, list the nodes in `--redis_url`, or give one node: Cabby detects a cluster
  node.

Each stream lives on one cluster node, and its epoch follows that node's instance, so a
restart or failover of one node gives new epochs to the streams it holds and leaves the others
alone.
