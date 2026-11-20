import sys, time, json, struct, urllib.request
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); bport = int(sys.argv[2])
def conn(label, path="/cable", headers=None, n=3):
    try: w = WS("127.0.0.1", port, path=path, headers=headers)
    except Exception as e: print(label, "handshake", str(e)[:60]); return None
    for _ in range(n):
        try: m = w.recv_json(1.0)
        except Exception as e: m = f"<{type(e).__name__}>"
        if isinstance(m, dict) and "sid" in m: m["sid"] = "<SID>"
        print(label, json.dumps(m, sort_keys=True))
        if isinstance(m, str) or (isinstance(m, dict) and "__close__" in m): break
    return w
def tx(w, label, obj, n=2, t=0.8):
    w.send_json(obj)
    for _ in range(n):
        try: m = w.recv_json(t)
        except Exception as e: m = f"<{type(e).__name__}>"
        print(label, json.dumps(m, sort_keys=True))
        if isinstance(m, str) or (isinstance(m, dict) and "__close__" in m): break
conn("reject", "/cable?mode=reject"); conn("error", "/cable?mode=error"); conn("500", "/cable?mode=500"); conn("garbage", "/cable?mode=garbage"); conn("401", "/cable?mode=401")
H = {"Cookie": "a=1; session=abc; b=2", "X-Forwarded-For": "1.2.3.4, 5.6.7.8", "Origin": "http://x.test", "Authorization": "Bearer zzz", "X-Request-ID": "req-1", "User-Agent": "probe"}
w = conn("ok", "/cable?token=secret&x=%20y", headers=H, n=1)
ch = lambda c, **kw: json.dumps(dict(channel=c, **kw))
tx(w, "sub.reject", {"command": "subscribe", "identifier": ch("Reject")})
tx(w, "sub.error", {"command": "subscribe", "identifier": ch("Error")}, n=1)
tx(w, "sub.state", {"command": "subscribe", "identifier": ch("State")})
tx(w, "sub.state.again", {"command": "subscribe", "identifier": ch("State")}, n=1)
tx(w, "sub.notx", {"command": "subscribe", "identifier": ch("NoTx")}, n=1)
tx(w, "sub.bcast", {"command": "subscribe", "identifier": ch("Bcast")}, n=3)
tx(w, "msg.echo", {"command": "message", "identifier": ch("State"), "data": json.dumps({"action": "speak", "t": "<&>"})})
tx(w, "msg.nonstr", {"command": "message", "identifier": ch("State"), "data": {"action": "speak"}}, n=1)
tx(w, "msg.err", {"command": "message", "identifier": ch("State"), "data": "err"}, n=1)
tx(w, "msg.fail", {"command": "message", "identifier": ch("State"), "data": "fail"}, n=1)
def post(body):
    urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{bport}/_broadcast", data=json.dumps(body).encode(), method="POST")).read()
post({"stream": "b", "data": "\"to-b-1\""}); tx(w, "bcast.b", {"command": "ping"}, n=4, t=0.5)
tx(w, "msg.stop_b", {"command": "message", "identifier": ch("State"), "data": "stop_b"}, n=1)
post({"stream": "b", "data": "\"to-b-2\""}); post({"stream": "a", "data": "\"to-a-2\""}); tx(w, "after.stop_b", {"command": "ping"}, n=6, t=0.5)
tx(w, "msg.stop_all", {"command": "message", "identifier": ch("State"), "data": "stop_all"}, n=1)
post({"stream": "a", "data": "\"to-a-3\""}); tx(w, "after.stop_all", {"command": "ping"}, n=5, t=0.5)
tx(w, "unsub", {"command": "unsubscribe", "identifier": ch("State")}, n=1)
tx(w, "unsub.unknown", {"command": "unsubscribe", "identifier": ch("State")}, n=1)
tx(w, "msg.disc", {"command": "message", "identifier": ch("NoTx"), "data": "disc"}, n=3)
w2 = conn("ok2", n=1); tx(w2, "sub.disc", {"command": "subscribe", "identifier": ch("Disc")}, n=3)
w3 = conn("ok3", n=1); tx(w3, "sub3", {"command": "subscribe", "identifier": ch("Plain")}, n=1); w3.close(); time.sleep(1.0)
