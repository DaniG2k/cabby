# Configuring Cabby

Cabby takes the AnyCable server's settings, under its own names. Every setting can come from a
command-line flag, an environment variable or a TOML configuration file. What each setting means
is described in the [AnyCable server documentation](https://docs.anycable.io/anycable-go/configuration);
`cabby --help` lists the flags.

## Precedence

From lowest to highest:

1. built-in defaults;
2. the configuration file;
3. environment variables;
4. command-line flags.

Presets (`--presets`, or detected on Fly.io and Heroku) fill in settings that are still at their
defaults after that.

## Configuration file

`--config-path <file>` names the file. Without it, Cabby loads the first of these that exists:

1. `./cabby.toml`
2. `/etc/cabby/cabby.toml`
3. `./anycable.toml`
4. `/etc/anycable/anycable.toml`

A Cabby-named file always wins over an AnyCable-named one, wherever each is. Only one file is
loaded; the others are ignored, and with `--debug` Cabby logs each one:
`DBG configuration file ignored path=./anycable.toml used=/etc/cabby/cabby.toml context=config`.
`--ignore-config-path` skips the search. The format and keys are the same for every name;
`cabby --print-config` prints the current configuration in that format.

The printed file loads back to the same settings. String values are TOML basic strings: `"` and
`\` are escaped, and so are control characters (`\n`, `\t`, `\u0007`) and other characters
that Go does not print as they are (` `). A metrics tag key that is not a bare TOML key is
quoted, so `tags."a.b"` stays one key. A value with nothing to escape prints exactly as AnyCable
prints it (D-102). Some settings print commented out, or as an example, until another setting is
set: for example `cluster_name` without `cluster_addr`, and the `statsd.*` settings without
`statsd.host`.

## Environment variables

Every flag `--<name>` reads `CABBY_<NAME>`, then `ANYCABLE_<NAME>` (the name upper-cased, `-`
becoming `_`). `--port` also reads `PORT`, and `--redis_url` also reads `REDIS_URL`, after both
prefixed names. The first variable that is set wins, and a flag on the command line beats all of
them. An empty value counts as set and hides the names after it: `CABBY_SECRET=` means an empty
secret whatever `ANYCABLE_SECRET` says, an empty number keeps the default, and an empty boolean
means `false`.

| Flag | Variables, in order |
|---|---|
| `--secret` | `CABBY_SECRET`, `ANYCABLE_SECRET` |
| `--port` | `CABBY_PORT`, `ANYCABLE_PORT`, `PORT` |
| `--redis_url` | `CABBY_REDIS_URL`, `ANYCABLE_REDIS_URL`, `REDIS_URL` |
| `--strict-config` | `CABBY_STRICT_CONFIG`, `ANYCABLE_STRICT_CONFIG` |
| `--sqlite_path` | `CABBY_SQLITE_PATH` only |
| `--redis_broker_key_prefix`, `--redis_broker_timeout` | `CABBY_REDIS_BROKER_KEY_PREFIX`, `CABBY_REDIS_BROKER_TIMEOUT` only |
| `--redis_sentinel_tls` | `CABBY_REDIS_SENTINEL_TLS` only |
| `--postgres_url`, `--postgres_*` | `CABBY_POSTGRES_URL`, `CABBY_POSTGRES_*` only |

Two settings aren't flags and follow the same order: `CABBY_FILTER_LOGS` / `ANYCABLE_FILTER_LOGS`
(set to `false` to log sensitive values unmasked) and `CABBY_FLY_RPC_APP_NAME` /
`ANYCABLE_FLY_RPC_APP_NAME` (the Fly.io preset's RPC application).

### Cabby-only settings

Settings the AnyCable server doesn't have read only their `CABBY_*` name. Today those are
`--sqlite_path` (`CABBY_SQLITE_PATH`, the SQLite broker, deviation D-065): `ANYCABLE_SQLITE_PATH`
does nothing. The same goes for the Redis broker's `--redis_broker_key_prefix` and
`--redis_broker_timeout` (`CABBY_REDIS_BROKER_KEY_PREFIX`, `CABBY_REDIS_BROKER_TIMEOUT`,
`[redis_broker]`, deviation D-069; see [`redis-broker.md`](redis-broker.md)), and for
`--redis_sentinel_tls` (`CABBY_REDIS_SENTINEL_TLS`, `sentinel_tls` in `[redis]`, deviation D-115;
see [Sentinel and TLS](redis-broker.md#sentinel-and-tls)).
So do the `--postgres_*` settings of the PostgreSQL adapters (`CABBY_POSTGRES_URL` and
the others, the `[postgres]` section, deviations D-070 and D-071; see
[`postgres.md`](postgres.md)). In a configuration file, their keys work under any of the four file names.

### When both names are set

If `CABBY_X` and `ANYCABLE_X` are both set to different values, Cabby uses `CABBY_X` and logs one
warning per setting, naming the variables but never their values:

```text
WRN both CABBY_SECRET and ANYCABLE_SECRET are set; using CABBY_SECRET context=config
```

Equal values, unprefixed aliases such as `PORT`, and settings given as a flag on the command line
(which overrides both variables) never warn. A warning usually means a
half-finished migration: remove the `ANYCABLE_*` variable.

`--help` lists only the `CABBY_*` names and the unprefixed aliases, and ends with a reminder that
every `CABBY_*` variable can also be set as `ANYCABLE_*`, except the Cabby-only ones.

## Secrets and `none`

`none` turns off a single secret, even when `--secret` is set: `--http_rpc_secret=none`,
`--jwt_secret=none`, `--streams_secret=none`, `--broadcast_key=none` and `--pusher_secret=none`
(or their variables and configuration keys), as in AnyCable. The value is compared exactly:
`None` or `NONE` is a secret like any other.

The application secret itself (`--secret`, `CABBY_SECRET`, `ANYCABLE_SECRET`, `secret` in the
configuration file) set to `none` is treated as not set: Cabby runs as without an application
secret, and no other secret is derived from it. `cabby --print-config` writes `secret = "none"`
when no secret is set, so a printed configuration loads this way. Cabby logs one warning at
startup:

```text
WRN the application secret is "none": treated as not set, and no other secret is derived from it. Remove the setting to silence this warning context=config
```

Remove the setting to run without an application secret and without the warning. A
`SecretBase = "none"` key under `[rpc]`, `[http_broadcast]` or `[api]` in a configuration file is
treated as not set too, with a similar warning. AnyCable accepts `secret = none` (deviation D-086).

## Reserved names

`CABBY_NATS_SERVER_BIN` (or its older spelling `CABBY_NATS_SERVER`) names the `nats-server`
executable for embedded NATS; it is the only server variable that isn't a flag. Cabby's test
harness uses `CABBY_TEST_*`, `CABBY_BENCH_*`, `CABBY_BACKEND` and `CABBY_GO_*`; no flag reads
those names.

## The Docker image's defaults

The image (`ghcr.io/danig2k/cabby`) sets two defaults with the `ANYCABLE_*` names:
`ANYCABLE_HOST=0.0.0.0` (so it can be reached from outside the container) and
`ANYCABLE_BROADCAST_ADAPTER=http` (so it runs without Redis). Override either with `-e` and
either name, `CABBY_*` or `ANYCABLE_*`, or with the flag. A `CABBY_*` override also logs the
conflict warning above, which is expected here. If a Redis URL is set but no broadcast adapter
reads from Redis, Cabby logs `WRN Redis is configured but broadcasts are HTTP only; set
CABBY_BROADCAST_ADAPTER to add redis or redisx`. With the Redis broker (`--broker=redis`), which
stores history in Redis but doesn't read broadcasts from it, the warning is `WRN broadcasts are
HTTP only; the Redis broker does not read broadcasts from Redis (set CABBY_BROADCAST_ADAPTER to
add redisx)`.

With a broker that keeps history (`memory`, `sqlite`, `nats`, `redis`, `postgres`), broadcasts through the
`redis` (PUBLISH) and `nats` adapters are delivered live but not stored in history, as in the
AnyCable server; when you set the broadcast adapters (flag, variable or configuration file) to
include them, Cabby logs `WRN broadcasts through the redis adapter are delivered live and not
stored in history; use http or redisx to keep history` (naming the adapters in use). With the
default list or a preset's, the same line is logged at debug level.

## Migrating from AnyCable

Nothing has to change: an `anycable.toml` and `ANYCABLE_*` variables keep working. When
convenient, rename the file to `cabby.toml` and the variables to `CABBY_*`. Rename all of a
deployment's variables at once, or remove the old ones, so that no stale `ANYCABLE_*` value is
left behind the warning above.
