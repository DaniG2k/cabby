# Redis broker (`--broker=redis`)

The Redis broker keeps stream history and session state in Redis, so several Cabby nodes share
them. A client that reconnects to any node catches up on the messages it missed and gets its
session back. Deployments that already run Redis get this without NATS JetStream. The Redis
broker is a Cabby addition (deviation D-069 in [`compat/deviations.md`](../compat/deviations.md)):
the AnyCable server has no such broker and stops at startup with
`Unsupported broker adapter: redis`.

## Quick start

```sh
cabby --broker=redis --redis_url=redis://redis.internal:6379/0
```

Run every node with the same Redis URL and the same key prefix. Without `--pubsub`, the Redis
broker also selects Redis pub/sub, so a broadcast received by one node reaches the clients of
every node. To use NATS for pub/sub instead, set `--pubsub=nats`.

The history and session settings are the usual broker ones: `--history_limit` (messages per
stream, default 100), `--history_ttl` (seconds, default 300) and `--sessions_ttl` (seconds,
default 300).

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
not be empty or contain `{` or `}`. All keys start with `<prefix>:`, so a Redis ACL rule such as
`~myapp:*` can limit a node to its own keys. The broker also needs the scripting commands
(`EVAL`, `EVALSHA`, `SCRIPT LOAD`) and `TIME`.

## What it stores

| Key | Type | Expires after |
|---|---|---|
| `<prefix>:epoch` | string | never |
| `<prefix>:{s:<stream>}:hist` | stream, one entry per message (ID `<offset>-0`) | `history_ttl` after the last message |
| `<prefix>:{s:<stream>}:seq` | integer, the stream's last offset | 10 x `history_ttl` after the last message |
| `<prefix>:sess:<sid>` | string, a session's state | `sessions_ttl` after the last commit or touch |

The `{s:<stream>}` hash tag puts a stream's keys in one Redis Cluster slot, so the script that
stores a message runs on a single node, and different streams spread over the cluster.

The offset counter outlives the history: a stream that is quiet for longer than `history_ttl`
loses its messages but keeps counting from where it was, so offsets never repeat for a client
that is still around. Timestamps come from the Redis server's clock, so all nodes agree on what
has expired.

Redis may keep more than `history_limit` entries per stream (`MAXLEN ~` trims whole internal
nodes). Reads return exactly the newest `history_limit` messages that are younger than
`history_ttl`, as the memory broker does.

## The epoch

The epoch tells clients whether their offsets still mean something. The first node to start
creates it (`SET NX` semantics), and the others read the same value. Each broadcast and each
history read checks the epoch in the same round trip. If the key is gone, because Redis
restarted without persistence or evicted it, that operation creates a new epoch. Every node then
uses the new one, and clients that ask for history with the old epoch get `unknown epoch` and
resync. `redis_broker_epoch_rotations_total` counts these new epochs.

Use persistence (RDB or AOF) if history should survive a Redis restart. Without it, a restart
only rotates the epoch, and clients resync.

If Redis evicts keys (`maxmemory` with an `allkeys-*` policy), a stream can lose its history or
its counter. A lost history makes old offsets unavailable, and clients resync. A lost counter
continues after the newest stored message. The epoch key has no TTL, so `volatile-*` policies
never evict it.

## Presence

The Redis broker has no presence yet, like the NATS broker. Presence commands get an error,
Pusher `presence-` channels can't be subscribed to, and the users endpoints of the API and the
Pusher REST API answer `501`. Use the memory broker on a single node if you need presence.

## When Redis fails

Each operation has a timeout (`--redis_broker_timeout`). When Redis is unreachable or doesn't
answer in time:

- a broadcast fails with an error and is not delivered; the error is logged;
- history reads fail, so a reconnecting client resyncs;
- session commits, restores and touches fail; a client whose session can't be restored is
  authenticated again, as a new session;
- reads and session calls are tried once more on a new connection; broadcasts are not, since
  their offset may already have been assigned.

At startup, Cabby connects, loads the scripts and reads (or creates) the epoch. If that fails
within the timeout, the server stops with `failed to start broker, cause: failed to start the
Redis broker, cause: ...`.

## Logs and metrics

At startup:

```text
INF Using Redis broker: redis.internal:6379 (key prefix: myapp, history limit: 100, history ttl: 300s, sessions ttl: 300s)
INF Redis broker is ready (epoch 57lD7q, new) context=broker provider=redis
```

`new` means this node created the epoch; the other nodes log `existing` with the same epoch.
Failed operations are logged at error level with `context=broker provider=redis`.

| Metric | Type | Meaning |
|---|---|---|
| `redis_broker_errors_total` | counter | Failed operations, timeouts included |
| `redis_broker_timeouts_total` | counter | Operations that ran out of time |
| `redis_broker_reconnects_total` | counter | Connections opened again after a failure |
| `redis_broker_epoch_rotations_total` | counter | Epochs created because the epoch key was missing |
| `redis_broker_operation_ms` | gauge | Duration of the last operation (milliseconds) |

## Redis Cluster and Sentinel

The broker uses the same connection code as Redis pub/sub and broadcasting:

- for Sentinel, set `--redis_sentinels` and give the master name in `--redis_url`;
- for Cluster, list the nodes in `--redis_url`, or give one node: Cabby detects a cluster
  node.

Each message takes one round trip on a single node, or two in a cluster, where the epoch key
and the stream's keys live on different nodes.
