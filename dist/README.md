# Cabby @VERSION@

Cabby is a real-time WebSocket server written in Rust. It holds the WebSocket connections for
Rails (Action Cable), Laravel, Node.js and Django apps, so your app doesn't have to. It is
compatible with AnyCable clients and with the AnyCable server's configuration.

Documentation: https://docs.cabbyhq.com/install

## Quick start

### Binary

Builds are available for Linux (x86_64 and arm64, glibc 2.28 or newer) and macOS (Apple
silicon). Download a tarball and `SHA256SUMS` from
https://github.com/DaniG2k/cabby-releases/releases, then:

```sh
shasum -a 256 -c SHA256SUMS --ignore-missing
tar -xzf cabby-@VERSION@-x86_64-unknown-linux-gnu.tar.gz
./cabby --version
./cabby --norpc --public --broadcast_adapter=http
```

The default broadcast adapters are `http,redis`, so a standalone binary needs
`--broadcast_adapter=http` unless Redis is running on `localhost:6379`.

### Docker

```sh
docker run --rm -p 8080:8080 -p 8090:8090 ghcr.io/danig2k/cabby:@VERSION@ --norpc --public
```

The image listens on `0.0.0.0` and takes broadcasts over HTTP only, so it runs without Redis. It
runs as a non-root user and has a health check. Configure it with flags or `CABBY_*` environment
variables (`-e`; the `ANYCABLE_*` names work too). To broadcast through Redis, set
`CABBY_BROADCAST_ADAPTER` or `ANYCABLE_BROADCAST_ADAPTER` to include `redis` or `redisx`.

### Try it

`--public` accepts every connection and subscription without authentication, so use it only
locally. Clients connect to `ws://localhost:8080/cable`. Broadcast to a stream with:

```sh
curl -X POST localhost:8090/_broadcast -H 'Content-Type: application/json' \
  -d '{"stream":"demo","data":"{\"text\":\"hello\"}"}'
```

## Configuration

`cabby --help` lists every flag and its `CABBY_*` environment variable. Cabby reads `cabby.toml`
from the working directory or `/etc/cabby/`. An AnyCable deployment keeps working as it is:
`anycable.toml` and the `ANYCABLE_*` variables are read too, after the Cabby names. See
https://github.com/DaniG2k/cabby/blob/main/docs/configuration.md.

## License

Cabby is released under the MIT License. `LICENSE` also includes AnyCable's MIT notice, because
Cabby is derived from AnyCable. The licenses of the Rust crates in the binary, and SQLite's
public-domain notice, are in `THIRD_PARTY_LICENSES.md`. `CHANGELOG.md` lists the changes in each
release.

## Source code

https://github.com/DaniG2k/cabby
