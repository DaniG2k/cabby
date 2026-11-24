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

## Environment variables

Every flag `--<name>` reads `CABBY_<NAME>`, then `ANYCABLE_<NAME>` (the name upper-cased, `-`
becoming `_`). `--port` also reads `PORT`, and `--redis_url` also reads `REDIS_URL`, after both
prefixed names. The first variable that is set wins, and a flag on the command line beats all of
them.

| Flag | Variables, in order |
|---|---|
| `--secret` | `CABBY_SECRET`, `ANYCABLE_SECRET` |
| `--port` | `CABBY_PORT`, `ANYCABLE_PORT`, `PORT` |
| `--redis_url` | `CABBY_REDIS_URL`, `ANYCABLE_REDIS_URL`, `REDIS_URL` |
| `--strict-config` | `CABBY_STRICT_CONFIG`, `ANYCABLE_STRICT_CONFIG` |
| `--sqlite_path` | `CABBY_SQLITE_PATH` only |

Two settings aren't flags and follow the same order: `CABBY_FILTER_LOGS` / `ANYCABLE_FILTER_LOGS`
(set to `false` to log sensitive values unmasked) and `CABBY_FLY_RPC_APP_NAME` /
`ANYCABLE_FLY_RPC_APP_NAME` (the Fly.io preset's RPC application).

### Cabby-only settings

Settings the AnyCable server doesn't have read only their `CABBY_*` name. Today that is
`--sqlite_path` (`CABBY_SQLITE_PATH`, the SQLite broker, deviation D-065): `ANYCABLE_SQLITE_PATH`
does nothing. In a configuration file, their keys work under any of the four file names.

### When both names are set

If `CABBY_X` and `ANYCABLE_X` are both set to different values, Cabby uses `CABBY_X` and logs one
warning per setting, naming the variables but never their values:

```text
WRN both CABBY_SECRET and ANYCABLE_SECRET are set; using CABBY_SECRET context=config
```

Equal values, and unprefixed aliases such as `PORT`, never warn. A warning usually means a
half-finished migration: remove the `ANYCABLE_*` variable.

`--help` lists only the `CABBY_*` names and the unprefixed aliases, and ends with a reminder that
every `CABBY_*` variable can also be set as `ANYCABLE_*`, except the Cabby-only ones.

## Reserved names

`CABBY_NATS_SERVER_BIN` (or its older spelling `CABBY_NATS_SERVER`) names the `nats-server`
executable for embedded NATS; it is the only server variable that isn't a flag. Cabby's test
harness uses `CABBY_TEST_*`, `CABBY_BENCH_*`, `CABBY_BACKEND` and `CABBY_GO_*`; no flag reads
those names.

## Migrating from AnyCable

Nothing has to change: an `anycable.toml` and `ANYCABLE_*` variables keep working. When
convenient, rename the file to `cabby.toml` and the variables to `CABBY_*`. Rename all of a
deployment's variables at once, or remove the old ones, so that no stale `ANYCABLE_*` value is
left behind the warning above.
