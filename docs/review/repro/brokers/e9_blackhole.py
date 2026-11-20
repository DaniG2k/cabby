import sys, time, json, os, socket, threading
from srv import Server, free_port
from lib import WS
RPORT = 32768
frozen = threading.Event()
def pump(a, b):
    try:
        while True:
            while frozen.is_set(): time.sleep(0.05)
            d = a.recv(65536)
            if not d: break
            while frozen.is_set(): time.sleep(0.05)
            b.sendall(d)
    except Exception: pass
    for s in (a, b):
        try: s.close()
        except Exception: pass
def proxy(port):
    ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); ls.bind(("127.0.0.1", port)); ls.listen(128)
    def loop():
        while True:
            c, _ = ls.accept()
            def start(c=c):
                while frozen.is_set(): time.sleep(0.05)
                u = socket.create_connection(("127.0.0.1", RPORT))
                threading.Thread(target=pump, args=(c, u), daemon=True).start(); threading.Thread(target=pump, args=(u, c), daemon=True).start()
            threading.Thread(target=start, daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()
def ident(stream): return json.dumps({"channel": "$pubsub", "stream_name": stream})
kind = sys.argv[1]; FREEZE = 25
tag = "rvwbrk" + os.urandom(3).hex(); pp = free_port(); proxy(pp)
cfg = f"{os.path.dirname(os.path.abspath(__file__))}/cfg-bh-{tag}.toml"
open(cfg, "w").write(f'[redis_pubsub]\nchannel = "{tag}_internal"\n')
srv = Server(kind, ["--public", "--pubsub=redis", "--broker=memory", "--broadcast_adapter=http", f"--redis_url=redis://127.0.0.1:{pp}", f"--config-path={cfg}", "--history_limit=1"], name="bh-" + kind)
try:
    time.sleep(1); X = tag + "-x"
    ws = WS(srv.port); ws.recv(); ws.send({"command": "subscribe", "identifier": ident(X)}); ws.recv_all(0.4)
    srv.broadcast({"stream": X, "data": "\"warm\""}); print("warm delivered:", [m.get("message") for m in ws.recv_all(0.5)], "rss_kb", srv.rss_kb())
    frozen.set(); t0 = time.time(); sent = [0]; slow = [0]; maxlat = [0.0]; stop = [False]
    pad = "x" * 4000
    def worker(k):
        i = 0
        while not stop[0]:
            t = time.time()
            try: srv.broadcast({"stream": X, "data": json.dumps({"k": k, "i": i, "t": t, "pad": pad})})
            except Exception: slow[0] += 1; continue
            lat = time.time() - t; maxlat[0] = max(maxlat[0], lat); sent[0] += 1; i += 1
    ths = [threading.Thread(target=worker, args=(k,), daemon=True) for k in range(8)]
    for t in ths: t.start()
    while time.time() - t0 < FREEZE:
        time.sleep(5); print(f"  frozen {time.time()-t0:4.1f}s: accepted={sent[0]} http_errors/timeouts={slow[0]} max_http_latency={maxlat[0]:.2f}s rss_kb={srv.rss_kb()} alive={srv.alive()}", flush=True)
    stop[0] = True; time.sleep(0.3); total = sent[0]
    frozen.clear(); tu = time.time(); got = 0; oldest = 0.0; last = tu
    while time.time() - tu < 40:
        ms = ws.recv_all(1.0)
        for m in ms:
            if isinstance(m.get("message"), dict):
                got += 1; oldest = max(oldest, time.time() - m["message"]["t"]); last = time.time()
        if time.time() - last > 6: break
    print(f"after unfreeze: stale messages delivered={got} of {total} accepted during outage; oldest age at delivery={oldest:.1f}s; flood lasted {last-tu:.1f}s; rss_kb={srv.rss_kb()} alive={srv.alive()}")
    lg = srv.logs().splitlines(); c = {}
    for l in lg:
        if " ERR " in l or " WRN " in l:
            k = l[24:100]; c[k] = c.get(k, 0) + 1
    for k, v in sorted(c.items(), key=lambda x: -x[1])[:6]: print(f"  LOG x{v}: {k}")
finally:
    srv.stop()
