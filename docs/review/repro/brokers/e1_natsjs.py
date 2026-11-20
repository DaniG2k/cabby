import sys, time, json
from srv import Server, free_port
from lib import WS
# A NATS server without JetStream: Go reference with embedded NATS (no JS unless broker=nats)
nport = free_port()
host = Server("go", ["--public", "--embed_nats", f"--enats_addr=nats://127.0.0.1:{nport}", "--pubsub=nats"], name="natshost")
print("nats host up, port", nport, "| log tail:", host.logs().splitlines()[-3:])
res = {}
try:
    servers = {k: Server(k, ["--public", "--broker=nats", "--pubsub=nats", f"--nats_servers=nats://127.0.0.1:{nport}", "--broadcast_adapter=http"], name="jsless-" + k) for k in ("go", "rust")}
    t0 = time.time()
    exited = {}
    n = 0
    while time.time() - t0 < 100:
        for k, s in servers.items():
            if k not in exited and not s.alive():
                exited[k] = (round(time.time() - t0, 1), s.p.returncode)
                print(k, "EXITED after", exited[k])
            elif k not in exited:
                # keep broadcasting non-transient messages
                try:
                    for _ in range(200):
                        s.broadcast({"stream": "a", "data": "x" * 2000}); n += 1
                except Exception as e:
                    pass
        if int(time.time() - t0) % 10 == 0:
            print(int(time.time() - t0), {k: (s.alive(), s.rss_kb()) for k, s in servers.items()}, "broadcasts", n, flush=True)
            time.sleep(1)
    for k, s in servers.items():
        print("=====", k, "alive:", s.alive(), "rss_kb:", s.rss_kb())
        print("\n".join(l[:300] for l in s.logs().splitlines()[-25:]))
        s.stop()
finally:
    host.stop()
