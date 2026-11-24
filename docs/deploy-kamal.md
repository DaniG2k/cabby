# Deploying Cabby with Kamal

Cabby runs as a [Kamal](https://kamal-deploy.org) accessory next to your Rails app. This guide
assumes HTTP RPC (the app mounts `/_anycable`, as in the README's quick start) and a Kamal
service called `myapp`.

## config/deploy.yml

```yaml
accessories:
  cabby:
    image: ghcr.io/danig2k/cabby:0.2.0
    host: 203.0.113.10                 # a host of your web role, or a dedicated one
    port: "8080:8080"                  # WebSocket /cable; terminate TLS in front of it
    env:
      clear:
        CABBY_RPC_HOST: "https://app.example.com/_anycable"   # your app's public URL
        CABBY_ALLOWED_ORIGINS: "app.example.com"              # restrict WebSocket origins
        CABBY_BROADCAST_ADAPTER: http
        CABBY_HTTP_BROADCAST_PORT: 8090  # broadcasts on 8090, kept inside the Docker network
      secret:
        - CABBY_SECRET
```

`CABBY_HTTP_BROADCAST_PORT` matters: with a secret set, Cabby serves HTTP broadcasts on the main
port (8080, the published one) unless a broadcast port is given. Setting it to 8090 keeps
broadcasts off the public port, where the app below expects them.

## .kamal/secrets

```sh
CABBY_SECRET=$CABBY_SECRET
ANYCABLE_SECRET=$CABBY_SECRET          # the anycable-rails gem reads ANYCABLE_*
```

## config/anycable.yml (Rails, production)

```yaml
production:
  broadcast_adapter: http
  http_broadcast_url: "http://myapp-cabby:8090/_broadcast"   # accessory container: <service>-<accessory>
  websocket_url: "wss://ws.example.com/cable"
```

Recent `anycable-rails` versions (1.5 and later) derive the broadcast key from the shared secret,
as Cabby does, so broadcasts are authorized without extra settings.

## Boot

```sh
kamal accessory boot cabby
kamal accessory logs cabby
```

The log has a line like `Accept broadcast requests at http://…:8090/_broadcast (authorization required)`.

## Notes

- Kamal connects accessories to its Docker network, so the app reaches Cabby at
  `myapp-cabby:8090`, and port 8090 is never published.
- RPC goes through the app's public URL, because Kamal app containers have no fixed name. If your
  Kamal version gives the web role a network alias, `http://<alias>/_anycable` avoids the round
  trip.
- TLS for `wss://`: put a TLS proxy in front of port 8080. Recent Kamal versions can route an
  accessory through `kamal-proxy` with a `proxy:` block; check yours.
- The image has a Docker health check (`cabby-healthcheck`, `GET /health` on the main port). It
  reads the host and the port from `CABBY_HOST` and `CABBY_PORT`, so change them with those
  variables rather than `--host` and `--port`.
- The image defaults to `ANYCABLE_HOST=0.0.0.0` and `ANYCABLE_BROADCAST_ADAPTER=http`; the
  snippet's `CABBY_BROADCAST_ADAPTER: http` repeats the second, so nothing is logged. To
  broadcast through Redis instead, set it to `http,redisx,redis` and add `CABBY_REDIS_URL`
  (Cabby then logs that both are set with different values, which is expected).
- The image is public: no `docker login` is needed on your hosts.
- `compat/scripts/release.sh` checks this set-up against every release image: with
  `CABBY_SECRET`, `CABBY_HTTP_BROADCAST_PORT=8090` and a broadcast key, an authorized
  `POST /_broadcast` answers 201 on 8090 and 404 on 8080.

## Several nodes on the app's Postgres

With the Postgres adapters ([`postgres.md`](postgres.md)), Cabby nodes share broadcasts, stream
history and sessions through the database the app already has, with no Redis or NATS. This
example runs Cabby on two hosts and the app broadcasts either over HTTP or by inserting rows
into Cabby's outbox table.

```yaml
accessories:
  db:
    image: postgres:18.6-alpine
    host: 203.0.113.20
    env:
      clear:
        POSTGRES_USER: myapp
        POSTGRES_DB: myapp_production
      secret:
        - POSTGRES_PASSWORD
    directories:
      - data:/var/lib/postgresql/data

  cabby:
    image: ghcr.io/danig2k/cabby:0.2.0
    hosts:
      - 203.0.113.10
      - 203.0.113.11
    port: "8080:8080"
    env:
      clear:
        CABBY_RPC_HOST: "https://app.example.com/_anycable"
        CABBY_ALLOWED_ORIGINS: "app.example.com"
        CABBY_BROADCAST_ADAPTER: http,postgres   # HTTP broadcasts and the outbox table
        CABBY_HTTP_BROADCAST_PORT: 8090
        CABBY_PUBSUB: postgres                   # nodes share broadcasts
        CABBY_BROKER: postgres                   # shared history and session recovery
      secret:
        - CABBY_SECRET
        - CABBY_POSTGRES_URL
```

`.kamal/secrets`:

```sh
POSTGRES_PASSWORD=$POSTGRES_PASSWORD
CABBY_SECRET=$CABBY_SECRET
ANYCABLE_SECRET=$CABBY_SECRET
CABBY_POSTGRES_URL=postgres://myapp:$POSTGRES_PASSWORD@myapp-db:5432/myapp_production?sslmode=disable
```

- `myapp-db` is the accessory's container on Kamal's Docker network, reachable from every host
  only if they share that network (one host, or an overlay network between hosts). Across hosts,
  use the database host's private address and `sslmode=verify-full` (or `require`) instead.
- Point Cabby at Postgres directly or through a PgBouncer pool in **session** mode: `LISTEN`
  does not survive transaction pooling.
- On the first boot Cabby creates its tables (`cabby_*` in `public`). To keep them apart from
  the app's tables, add `CABBY_POSTGRES_SCHEMA: cabby`. If the app's database role may not create
  tables, create them with the DDL in [`postgres.md`](postgres.md) and set
  `CABBY_POSTGRES_AUTO_MIGRATE: "false"`.
- Put a load balancer in front of the two hosts' port 8080. A client that reconnects to the other
  node resumes its session there.
- The app can keep broadcasting over HTTP to `http://myapp-cabby:8090/_broadcast` (the node on
  its own host) and, inside its transactions, insert into `cabby_broadcasts`; either way every
  node delivers the message.
