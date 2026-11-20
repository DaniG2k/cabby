#!/usr/bin/env python3
"""Deep-JSON stack probe: feed every client command path a message nested near
Go's 10000-level limit and check that the server stays up (a stack overflow on
the 64 MiB deep-decode thread would abort the whole process) and that concurrent
deep decodes do not exhaust memory/threads.

usage: python3 03_deep_json.py <cabby binary> [connections]

Run against BOTH target/debug/cabby (biggest frames) and target/release/cabby.
"""
import json, sys, time
from seclib import Server, WS, binary_arg, verdict

conns = int(sys.argv[2]) if len(sys.argv) > 2 else 150
srv = Server(binary_arg(), ["--public", "--norpc", "--presets=none", "--broker=memory", "--debug"], name="deep")
IDENT = json.dumps({"channel": "$pubsub", "stream_name": "deep"})


def nested(depth, obj):
    return ('{"a":' * depth + "1" + "}" * depth) if obj else ("[" * depth + "]" * depth)


def probe(command, field, depth, obj, extra=""):
    value = nested(depth, obj)
    payload = '{"command":"%s","identifier":%s,%s"%s":%s}' % (command, json.dumps(IDENT), extra, field, value)
    w = WS(srv.port, "/cable")
    w.try_json(2)
    w.send_json({"command": "subscribe", "identifier": IDENT})
    w.try_json(2)
    w.send(payload)
    reply = w.try_json(1.5)
    w.close()
    return reply


try:
    base = (srv.status("Threads"), srv.status("VmRSS"))
    for command, field, extra in [("message", "data", ""), ("whisper", "data", ""),
                                   ("history", "data", '"history":{"since":1},'), ("presence", "data", ""),
                                   ("nope", "data", ""), ("ping", "zzz", "")]:
        for depth in (129, 5000, 10000, 10001):
            for obj in (False, True):
                try:
                    r = probe(command, field, depth, obj, extra)
                except Exception as e:  # noqa: BLE001
                    r = f"<{type(e).__name__}: {e}>"
                print(f"{command}/{field} depth={depth} {'obj' if obj else 'arr'}: {str(r)[:80]} | alive={srv.alive()}")
                if not srv.alive():
                    verdict("deep-JSON decode crashed the process", True, f"{command} depth={depth}")
                    raise SystemExit(1)

    # concurrency: many simultaneous 10000-level decodes (one 64 MiB thread each on Cabby)
    workers = []
    doc = '{"command":"message","identifier":%s,"data":%s}' % (json.dumps(IDENT), nested(10000, False))
    for _ in range(conns):
        try:
            w = WS(srv.port, "/cable")
            w.try_json(1)
            w.send_json({"command": "subscribe", "identifier": IDENT})
            w.try_json(1)
            w.send(doc)
            workers.append(w)
        except Exception:  # noqa: BLE001
            break
    time.sleep(2)
    print(f"{len(workers)} concurrent deep decodes | threads {base[0]} -> {srv.status('Threads')} | "
          f"RSS {base[1]} -> {srv.status('VmRSS')} kB | alive={srv.alive()}")
    verdict("process survived all deep-JSON paths", srv.alive() and isinstance(WS(srv.port, '/cable').try_json(), dict))
    print("note: compare peak RSS and thread count against the Go reference; a per-connection 64 MiB")
    print("      reservation that commits under load would be a memory-amplification DoS. log:", srv.log_path)
    for w in workers:
        w.close()
finally:
    srv.stop()
