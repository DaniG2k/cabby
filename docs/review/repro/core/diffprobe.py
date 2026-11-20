import sys, time, json, socket, re, urllib.request, urllib.error
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); bport = int(sys.argv[2])
EXT = {"Sec-WebSocket-Protocol": "actioncable-v1-ext-json"}
sids = {}; epochs = {}
def norm(m):
    s = json.dumps(m, sort_keys=True)
    for k, v in sids.items(): s = s.replace(v, k)
    for k, v in epochs.items(): s = s.replace(v, k)
    return s
out = []
def P(label, m): out.append(f"{label}: {norm(m) if not isinstance(m, str) else m}")
def rx(w, label, t=1.0, n=1):
    res = []
    for _ in range(n):
        try: m = w.recv_json(t)
        except Exception as e: m = f"<{type(e).__name__}>"
        P(label, m); res.append(m)
    return res
def post(body, headers=None, label="post", path="/_broadcast", method="POST"):
    req = urllib.request.Request(f"http://127.0.0.1:{bport}{path}", data=body if isinstance(body, bytes) else body.encode(), method=method, headers=headers or {})
    try:
        r = urllib.request.urlopen(req, timeout=5); code = r.status; b = r.read()
    except urllib.error.HTTPError as e: code = e.code; b = e.read()
    except Exception as e: code = repr(e); b = b""
    P(label, f"{code} {b[:100]!r}")
ID = json.dumps({"channel": "$pubsub", "stream_name": "s"})
a = WS("127.0.0.1", port, headers=EXT)
P("a.proto", [l for l in a.handshake.split("\r\n") if l.lower().startswith("sec-websocket-protocol")])
w = rx(a, "a.welcome")[0]; sids["<SID_A>"] = w["sid"]
a.send_json({"command": "subscribe", "identifier": ID}); rx(a, "a.sub")
t_before = int(time.time()) - 5
for i in range(3):
    post(json.dumps({"stream": "s", "data": json.dumps({"n": i})}))
ms = rx(a, "a.msg", n=3)
try: epochs["<EPOCH>"] = ms[0]["epoch"]
except Exception: pass
b = WS("127.0.0.1", port, headers=EXT); w = rx(b, "b.welcome")[0]; sids["<SID_B>"] = w["sid"]
b.send_json({"command": "subscribe", "identifier": ID, "history": {"since": t_before}}); rx(b, "b.sub+hist", n=5)
E = epochs.get("<EPOCH>", "x")
def hist(label, h, n=4):
    b.send_json({"command": "history", "identifier": ID, "history": h}); rx(b, label, t=0.7, n=n)
hist("hist.off1", {"streams": {"s": {"offset": 1, "epoch": E}}})
hist("hist.off0", {"streams": {"s": {"offset": 0, "epoch": E}}}, n=5)
hist("hist.off3", {"streams": {"s": {"offset": 3, "epoch": E}}}, n=2)
hist("hist.off99", {"streams": {"s": {"offset": 99, "epoch": E}}}, n=2)
hist("hist.badepoch", {"streams": {"s": {"offset": 1, "epoch": "nope"}}}, n=2)
hist("hist.otherstream", {"streams": {"zzz": {"offset": 1, "epoch": E}}}, n=2)
hist("hist.since_future", {"since": int(time.time()) + 1000}, n=2)
hist("hist.since_neg", {"since": -5}, n=2)
hist("hist.empty", {}, n=1)
hist("hist.offstr", {"streams": {"s": {"offset": "1", "epoch": E}}}, n=2)
# presence
a.send_json({"command": "join", "identifier": ID, "presence": {"id": "u1", "info": {"name": "<A&B>"}}}); rx(a, "a.join", 0.7, 2); rx(b, "b.sees.join", 0.7, 2)
b.send_json({"command": "join", "identifier": ID, "presence": {"id": "u2", "info": None}}); rx(a, "a.sees.join2", 0.7, 2); rx(b, "b.join2", 0.7, 2)
b.send_json({"command": "presence", "identifier": ID}); rx(b, "b.presence", 0.7, 2)
b.send_json({"command": "presence", "identifier": ID, "data": {"return_records": False}}); rx(b, "b.presence.norec", 0.7, 2)
b.send_json({"command": "join", "identifier": ID}); rx(b, "b.join.nodata", 0.7, 1)
b.send_json({"command": "join", "identifier": ID, "presence": {"id": "u2", "info": {"again": 1}}}); rx(a, "a.sees.rejoin", 0.7, 1); rx(b, "b.rejoin", 0.7, 1)
b.send_json({"command": "leave", "identifier": ID}); rx(a, "a.sees.leave", 0.7, 2); rx(b, "b.leave", 0.7, 1)
b.send_json({"command": "leave", "identifier": ID}); rx(a, "a.sees.leave2", 0.7, 1)
# whisper
b.send_json({"command": "whisper", "identifier": ID, "data": {"typing": True}}); rx(a, "a.whisper", 0.7, 1); rx(b, "b.whisper.self", 0.5, 1)
# broadcast variants
post(json.dumps({"stream": "s", "data": "{\"x\":1}", "meta": {"exclude_socket": sids["<SID_A>"]}}), label="post.exclude"); rx(a, "a.excluded", 0.5); rx(b, "b.notexcluded", 0.5)
post(json.dumps([{"stream": "s", "data": "\"b1\""}, {"stream": "s", "data": "\"b2\""}]), label="post.batch"); rx(a, "a.batch", 0.5, 3)
post(json.dumps({"stream": "s", "data": "not json <b>&"}), label="post.rawstr"); rx(a, "a.rawstr", 0.5, 1)
post(json.dumps({"stream": "s", "data": "{\"big\":12345678901234567890123, \"f\":1.0, \"e\":1e3, \"u\":\"\\u00e9\\ud83d\\ude00\", \"h\":\"<\\/a>\"}"}), label="post.nums"); 
try:
    op, p = a.recv(1); P("a.nums.raw", p.decode())
except Exception as e: P("a.nums.raw", repr(e))
rx(b, "b.drain", 0.3, 6)
post(json.dumps({"stream": "s", "data": {"obj": 1}}), label="post.dataobj"); rx(a, "a.dataobj", 0.5, 1)
post(json.dumps({"stream": "s"}), label="post.nodata"); rx(a, "a.nodata", 0.5, 1)
post(json.dumps({"data": "x"}), label="post.nostream"); rx(a, "a.nostream", 0.5, 1)
post("{bad json", label="post.badjson")
post("", label="post.empty")
post("null", label="post.null")
post("[]", label="post.emptyarr")
post("[1]", label="post.arrint")
post(json.dumps({"stream": "s", "data": "\"t\"", "meta": {"transient": True}}), label="post.transient"); rx(a, "a.transient", 0.5, 1)
post(json.dumps({"stream": "s", "data": "\"m\"", "meta": {"broadcast_type": "custom", "transient": "yes"}}), label="post.metabad"); rx(a, "a.metabad", 0.5, 1)
post(json.dumps({"stream": "s", "data": "\"x\""}), label="get", method="GET")
post(json.dumps({"stream": "s", "data": "\"x\""}), label="put", method="PUT")
post(b"", label="options", method="OPTIONS")
post(json.dumps({"command": "bogus", "payload": {"a": 1}}), label="post.cmd.bogus")
post(json.dumps({"command": "disconnect", "payload": "str"}), label="post.cmd.badpayload")
# session restore
a.close(); time.sleep(0.3)
post(json.dumps({"stream": "s", "data": "\"while-away\""}), label="post.away"); rx(b, "b.away", 0.5, 1)
a2 = WS("127.0.0.1", port, path="/cable?sid=" + sids["<SID_A>"], headers=EXT); w = rx(a2, "a2.welcome")[0]
if isinstance(w, dict): sids["<SID_A2>"] = w.get("sid", "?"); out[-1] = "a2.welcome: " + norm(w)
a2.send_json({"command": "history", "identifier": ID, "history": {"since": t_before}}); rx(a2, "a2.hist", 0.7, 3)
post(json.dumps({"stream": "s", "data": "\"after-restore\""}), label="post.after"); rx(a2, "a2.after", 0.5, 1)
a3 = WS("127.0.0.1", port, path="/cable?sid=doesnotexist", headers=EXT); rx(a3, "a3.welcome")
# remote disconnect of b (identifiers for public mode?)
a2.send_json({"command": "unsubscribe", "identifier": ID}); rx(a2, "a2.unsub", 0.5, 1)
a2.send_json({"command": "unsubscribe", "identifier": ID}); rx(a2, "a2.unsub2", 0.5, 1)
for l in out: print(l)
