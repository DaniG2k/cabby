import sys, time, json, urllib.request
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); bport = int(sys.argv[2])
EXT = {"Sec-WebSocket-Protocol": "actioncable-v1-ext-json"}
ID = json.dumps({"channel": "$pubsub", "stream_name": "s"})
def post(body): urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{bport}/_broadcast", data=json.dumps(body).encode(), method="POST")).read()
def rx(w, label, n=1, t=0.8):
    for _ in range(n):
        try: m = w.recv_json(t)
        except Exception as e: m = f"<{type(e).__name__}>"
        if isinstance(m, dict):
            for k in ("sid", "epoch"):
                if k in m: m[k] = f"<{k}>"
        print(label, json.dumps(m, sort_keys=True))
a = WS("127.0.0.1", port, headers=EXT); w = a.recv_json(2); sid = w["sid"]
a.send_json({"command": "subscribe", "identifier": ID}); rx(a, "a.sub")
a.send_json({"command": "join", "identifier": ID, "presence": {"id": "u1", "info": "x"}}); rx(a, "a.join")
t0 = int(time.time()) - 2
post({"stream": "s", "data": "\"m1\""}); rx(a, "a.m1")
b = WS("127.0.0.1", port, headers=EXT); b.recv_json(2); b.send_json({"command": "subscribe", "identifier": ID}); rx(b, "b.sub")
b.send_json({"command": "history", "identifier": ID, "history": {"since": t0}}); rx(b, "hist.fresh", 2)
b.send_json({"command": "presence", "identifier": ID}); rx(b, "presence.before")
a.close()
time.sleep(1.0)
b.send_json({"command": "presence", "identifier": ID}); rx(b, "presence.1s-after-close", 2)
time.sleep(4.5)
b.send_json({"command": "presence", "identifier": ID}); rx(b, "presence.5.5s-after-close", 3)
b.send_json({"command": "history", "identifier": ID, "history": {"since": t0}}); rx(b, "hist.after-ttl", 2)
b.send_json({"command": "history", "identifier": ID, "history": {"streams": {"s": {"offset": 0, "epoch": w and ""}}}}); rx(b, "hist.off0.after-ttl", 2)
a2 = WS("127.0.0.1", port, path="/cable?sid=" + sid, headers=EXT); rx(a2, "restore.after-ttl")
