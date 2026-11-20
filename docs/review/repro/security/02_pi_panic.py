#!/usr/bin/env python3
"""Finding: `?pi=<huge>` (client ping interval) overflows Duration arithmetic in
Session::start_timers and panics on a command-pool thread.

usage: python3 02_pi_panic.py <cabby | anycable-go binary> [count]

Expected on Cabby: no welcome, the connection is dropped, one
"overflow when multiplying duration by scalar" panic per attempt on stderr; the
server keeps serving. Checks that a normal client still gets a welcome and that
threads/RSS do not grow with repeated attempts.
"""
import sys
from seclib import Server, WS, binary_arg, verdict

count = int(sys.argv[2]) if len(sys.argv) > 2 else 200
srv = Server(binary_arg(), ["--noauth", "--norpc", "--presets=none"], name="pi")
try:
    ok = WS(srv.port, "/cable")
    print("normal client:", ok.try_json())
    base = (srv.status("Threads"), srv.status("VmRSS"))

    results = {}
    for pi in ("9223372036854775807", "6148914691236517206", "6148914691236517205", "3000000000000000000", "-1", "0"):
        try:
            w = WS(srv.port, f"/cable?pi={pi}")
            results[pi] = w.try_json(2)
            w.close()
        except Exception as e:  # noqa: BLE001
            results[pi] = f"<{type(e).__name__}: {e}>"
        print(f"pi={pi}: {results[pi]}")

    for _ in range(count):
        try:
            w = WS(srv.port, "/cable?pi=9223372036854775807")
            w.try_json(0.3)
            w.close()
        except Exception:  # noqa: BLE001
            pass

    after = WS(srv.port, "/cable")
    welcome = after.try_json()
    out = srv.output()
    panics = out.count("panicked")
    print(f"panics on stderr: {panics}; threads {base[0]} -> {srv.status('Threads')}; "
          f"RSS {base[1]} -> {srv.status('VmRSS')} kB; alive: {srv.alive()}; health: {srv.health()}")
    print("client after the attempts:", welcome)
    verdict("contained panic reachable by an unauthenticated client (?pi=9223372036854775807)", panics > 0,
            f"{panics} panics")
    verdict("process survives", not srv.alive() or not isinstance(welcome, dict), "server died or stopped serving")
    print("log:", srv.log_path)
finally:
    srv.stop()
