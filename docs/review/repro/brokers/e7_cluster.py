import time, json, os, threading
from srv import Server, free_port
from fakecluster import Fake
p = free_port(); fake = Fake(p, delay=0.25)
srv = Server("rust", ["--public", "--pubsub=redis", "--broker=memory", "--broadcast_adapter=http", f"--redis_url=redis://127.0.0.1:{p},redis://localhost:{p}", "--log_level=debug"], name="cluster-rust")
try:
    time.sleep(1.5)
    print("startup log:", [l[24:140] for l in srv.logs().splitlines() if "Redis" in l or "redis" in l][:6])
    N = 30
    def send(i): srv.broadcast({"stream": f"room-{i}", "data": json.dumps(i)})
    ts = [threading.Thread(target=send, args=(i,)) for i in range(N)]
    for t in ts: t.start(); time.sleep(0.02)
    for t in ts: t.join()
    time.sleep(8)
    print(f"broadcasts accepted over HTTP: {N}; PUBLISH commands that reached Redis: {len(fake.published)} -> {sorted(fake.published, key=lambda s: int(s.split('-')[1]))}")
    errs = [l[24:330] for l in srv.logs().splitlines() if " ERR " in l or " WRN " in l]
    print("error/warn log lines:", len(errs))
    for e in errs[:4]: print("   ", e)
    # same stream for all: should batch fine
    fake.published.clear()
    ts = [threading.Thread(target=lambda: srv.broadcast({"stream": "one-room", "data": "1"})) for i in range(N)]
    for t in ts: t.start(); time.sleep(0.02)
    for t in ts: t.join()
    time.sleep(6)
    print(f"same-stream control: accepted {N}; reached Redis: {len(fake.published)}")
    print("distinct commands seen by fake:", sorted(set(c.split(b' ')[0] for c in fake.commands)))
finally:
    srv.stop(); print("log:", srv.log)
