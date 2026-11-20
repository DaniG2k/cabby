# Pusher compatibility: operator notes

Cabby serves the Pusher protocol like the Go reference when `--pusher_app_id`,
`--pusher_app_key` and `--pusher_secret` (or `--secret`) are set: Pusher clients connect
at `/app/<app_key>` and the REST API is at `/apps/<app_id>/`. See upstream's
`docs/pusher.md` for setup. These notes cover security-relevant behavior
(security review 2026-11, finding F2).

## `$pusher` is reserved for the Pusher endpoint (D-055)

The Pusher encoder turns `pusher:subscribe` into an AnyCable subscription with the
identifier `{"channel":"$pusher","stream":"<channel>"}`. The signature of `private-` and
`presence-` channels is checked by the Pusher endpoint's executor, before the
subscription reaches the `$pusher` controller.

In the Go reference the `$pusher` controller is reachable from every endpoint, so a client
of `/cable` (or SSE, or long polling) can subscribe to
`{"channel":"$pusher","stream":"private-orders"}` without a signature, read the private
channel, join presence channels as any user and whisper forged events to Pusher clients.

Cabby rejects `$pusher` subscriptions from sessions that do not come through the Pusher
endpoint (`reject_subscription`; a debug record `the Pusher channel is only available at
the Pusher endpoint`). AnyCable's own clients never use `$pusher`, so nothing legitimate is
refused. Without Pusher compatibility, `$pusher` is an ordinary channel name passed to
RPC, as in Go.

## Public Pusher channels share the stream namespace

A Pusher channel name is the AnyCable stream name. A public channel (no `private-` or
`presence-` prefix) needs no signature, which is how Pusher public channels work, so a
Pusher client can subscribe to **any** stream whose name has no such prefix, including
streams your application broadcasts to Action Cable clients after authorizing them over
RPC (`stream_from "notifications:42"`), signed streams and Turbo streams. It receives the
broadcasts whose payload has an `event` key (the Pusher encoder's format); others are not
delivered to it.

This is the same in Go and Cabby. When you enable Pusher compatibility:

- Name streams that carry private data with a `private-` or `presence-` prefix, or keep
  them off the server where Pusher is enabled.
- Treat any stream without such a prefix as public: anyone who knows (or guesses) its name
  can read it through `/app/<app_key>`.
- Broadcast private Pusher data only to `private-`/`presence-` channels, whose
  subscriptions need the HMAC signature your application issues.
