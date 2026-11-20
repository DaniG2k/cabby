import sys, time, json, os, signal
from srv import Server
from lib import WS, Redis
RPORT = 32768
def ident(stream): return json.dumps({"channel": "$pubsub", "stream_name": stream})
tag = "rvwbrk" + os.urandom(3).hex()
cfg = f"{os.path.dirname(os.path.abspath(__file__))}/cfg-mixed-{tag}.toml"
open(cfg, "w").write(f'[redis_pubsub]\nchannel = "{tag}_internal"\n[redis_stream_broadcast]\nstream = "{tag}_stream"\ngroup = "{tag}_bx"\nstream_read_block_milliseconds = 500\n')
args = ["--public", "--pubsub=redis", "--broker=memory", "--broadcast_adapter=http,redisx", f"--redis_url=redis://127.0.0.1:{RPORT}", f"--config-path={cfg}", "--log_level=debug"]
G = Server("go", args, name="mixr-go"); R = Server("rust", args, name="mixr-rust")
r = Redis(RPORT); X = tag + "-x"
try:
    time.sleep(1.2)
    g = WS(G.port); g.recv(); rr = WS(R.port); rr.recv()
    for w in (g, rr): w.send({"command": "subscribe", "identifier": ident(X)}); w.recv_all(0.4)
    G.broadcast({"stream": X, "data": "\"via-go-http\""}); R.broadcast({"stream": X, "data": "\"via-rust-http\""})
    strip = lambda ms: [(m.get("message"), m.get("epoch"), m.get("offset")) for m in ms]
    print("go client  :", strip(g.recv_all(0.8))); print("rust client:", strip(rr.recv_all(0.8)))
    N = 60
    for i in range(N): r.cmd("XADD", tag + "_stream", "*", "payload", json.dumps({"stream": X, "data": str(i)}))
    time.sleep(2.0)
    a = [m.get("message") for m in g.recv_all(0.8)]; b = [m.get("message") for m in rr.recv_all(0.8)]
    print(f"XADD {N}: go client got {len(a)} (uniq {len(set(a))}), rust client got {len(b)} (uniq {len(set(b))})")
    cons = r.cmd("XINFO", "CONSUMERS", tag + "_stream", tag + "_bx"); print("consumers:", [(c[1], "pending", c[3]) for c in cons], "xlen", r.cmd("XLEN", tag + "_stream"))
    # poison entry stays pending on whoever reads it; then kill -9 the rust node and see who owns it
    for _ in range(6): r.cmd("XADD", tag + "_stream", "*", "nopayload", "1")
    time.sleep(1.5)
    print("pending per consumer before kill:", r.cmd("XPENDING", tag + "_stream", tag + "_bx")[3])
    R.p.send_signal(signal.SIGKILL); time.sleep(4)
    print("pending per consumer 4s after kill -9 of cabby:", r.cmd("XPENDING", tag + "_stream", tag + "_bx")[3])
    for i in range(10): r.cmd("XADD", tag + "_stream", "*", "payload", json.dumps({"stream": X, "data": str(1000 + i)}))
    time.sleep(1.5)
    print("after cabby death, go client got:", len([m for m in g.recv_all(0.8)]), "of 10")
    print("consumers left:", [(c[1], "pending", c[3]) for c in r.cmd("XINFO", "CONSUMERS", tag + "_stream", tag + "_bx")])
finally:
    G.stop(); R.stop(); r.cmd("DEL", tag + "_stream")
    print("go ERR/WRN:", len([l for l in G.logs().splitlines() if " ERR " in l and "handler failed" not in l]) if False else "")
