import sys, time, json, os, re
from srv import Server
from lib import WS, Redis
RPORT = 32768
def ident(stream): return json.dumps({"channel": "$pubsub", "stream_name": stream})
kind = sys.argv[1]
tag = "rvwbrk" + os.urandom(3).hex()
cfg = f"{os.path.dirname(os.path.abspath(__file__))}/cfg-{kind}-{tag}.toml"
open(cfg, "w").write(f'[redis_pubsub]\nchannel = "{tag}_internal"\n')
srv = Server(kind, ["--public", "--pubsub=redis", "--broker=memory", "--broadcast_adapter=http", f"--redis_url=redis://127.0.0.1:{RPORT}?client_name={tag}", f"--config-path={cfg}", "--log_level=debug"], name="d003-" + kind)
r = Redis(RPORT)
def clients():
    out = []
    for l in r.cmd("CLIENT", "LIST").decode().splitlines():
        if f"name={tag}" in l:
            d = dict(kv.split("=", 1) for kv in l.split(" ") if "=" in kv)
            out.append((d["id"], d["sub"], d.get("resp"), d.get("cmd")))
    return out
def chans(): return len(r.cmd("PUBSUB", "CHANNELS", tag + "*"))
try:
    time.sleep(1)
    print("clients(id,sub,resp,lastcmd):", clients())
    N = 700
    ws = WS(srv.port); ws.recv()
    t0 = time.time()
    for i in range(N):
        ws.send({"command": "subscribe", "identifier": ident(f"{tag}-s{i}")})
    got = 0
    end = time.time() + 15
    while got < N and time.time() < end:
        got += len([m for m in ws.recv_all(0.2) if m.get("type") == "confirm_subscription"])
    print(f"subscribed {got}/{N} in {time.time()-t0:.2f}s; redis channels={chans()} (want {N+1})")
    time.sleep(0.5); print("channels after settle:", chans(), "clients:", clients())
    # deliver through redis to a few
    for i in (0, 255, 256, 257, 511, 512, 699):
        r.cmd("PUBLISH", f"{tag}-s{i}", json.dumps({"stream": f"{tag}-s{i}", "data": str(i)}))
    print("delivered:", sorted(m.get("message") for m in ws.recv_all(0.6)))
    # kill the subscriber connection
    sub = [c for c in clients() if int(c[1]) > 0]
    print("subscriber conns:", sub)
    tk = time.time()
    for c in sub: print("kill:", r.cmd("CLIENT", "KILL", "ID", c[0]))
    seq = 0; recovered = None; recv = []
    while time.time() - tk < 20:
        seq += 1
        r.cmd("PUBLISH", f"{tag}-s5", json.dumps({"stream": f"{tag}-s5", "data": str(seq)}))
        for m in ws.recv_all(0.1):
            recv.append(m.get("message"))
            if recovered is None: recovered = time.time() - tk
        if recovered is not None and time.time() - tk > recovered + 3: break
    print(f"after kill: first delivery after {recovered and round(recovered,2)}s; sent={seq} received={len(recv)} dups={len(recv)-len(set(recv))} first_received={recv[:1]}; channels={chans()} clients={clients()}")
    # unsubscribe half, check release
    for i in range(0, N, 2):
        ws.send({"command": "unsubscribe", "identifier": ident(f"{tag}-s{i}")})
    time.sleep(2.0); ws.recv_all(0.2)
    print("after unsubscribing half: channels=", chans(), "(want", N // 2 + 1, ")")
    # rapid churn on one stream
    for _ in range(200):
        ws.send({"command": "unsubscribe", "identifier": ident(f"{tag}-s1")})
        ws.send({"command": "subscribe", "identifier": ident(f"{tag}-s1")})
    time.sleep(2.0); ws.recv_all(0.3)
    r.cmd("PUBLISH", f"{tag}-s1", json.dumps({"stream": f"{tag}-s1", "data": "\"churn\""}))
    print("after churn: numsub s1=", r.cmd("PUBSUB", "NUMSUB", f"{tag}-s1"), "delivered:", [m.get("message") for m in ws.recv_all(0.5)])
    ws.close(); time.sleep(2.0)
    print("after disconnect: channels=", chans(), "(want 1)")
    time.sleep(11)
    print("after reconcile interval: channels=", chans(), "clients:", clients())
finally:
    logs = srv.logs(); srv.stop()
    lines = logs.splitlines()
    c = {}
    for l in lines:
        m = re.search(r" (INF|WRN|ERR) (.*?) nodeid", l)
        if m and ("context=pubsub" in l or "context=redis" in l):
            key = m.group(1) + " " + re.sub(r"\d+(\.\d+)?(ms|s)", "<d>", m.group(2))[:110]
            c[key] = c.get(key, 0) + 1
    for k, v in sorted(c.items(), key=lambda x: -x[1])[:25]: print("  LOG x%d: %s" % (v, k))
    print("log:", srv.log)
