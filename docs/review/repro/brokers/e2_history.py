import sys, json, time
from lib import WS
from srv import Server

def ident(stream): return json.dumps({"channel": "$pubsub", "stream_name": stream})

def norm(m, epoch):
    s = json.dumps(m, sort_keys=True)
    if epoch: s = s.replace(epoch, "<EPOCH>")
    return s

def scenario(kind):
    out = []
    srv = Server(kind, ["--public", "--broker=memory", "--broadcast_adapter=http", "--history_limit=3", "--history_ttl=4", "--sessions_ttl=4", "--log_level=debug"])
    srv2 = None
    try:
        bport = srv.port
        ws = WS(srv.port)
        w = ws.recv()
        sid = w.get("sid") if isinstance(w, dict) else None
        out.append("welcome keys: " + ",".join(sorted(w.keys())))
        ws.send({"command": "subscribe", "identifier": ident("s1")})
        out.append("sub: " + json.dumps(ws.recv_all(0.5)))
        for i in range(5):
            st = srv.broadcast({"stream": "s1", "data": json.dumps({"n": i + 1})})
        out.append("bstatus %s" % st)
        msgs = ws.recv_all(0.7)
        epoch = msgs[0].get("epoch") if msgs else None
        out.append("live: " + " | ".join(norm(m, epoch) for m in msgs))

        def hist(label, h, stream="s1"):
            ws.send({"command": "history", "identifier": ident(stream), "history": h})
            out.append(label + ": " + " | ".join(norm(m, epoch) for m in ws.recv_all(0.5)))
        for off in (0, 1, 2, 3, 4, 5, 6, 100):
            hist(f"from offset {off}", {"streams": {"s1": {"offset": off, "epoch": epoch}}})
        hist("bad epoch", {"streams": {"s1": {"offset": 4, "epoch": "zzzz"}}})
        hist("empty epoch", {"streams": {"s1": {"offset": 4, "epoch": ""}}})
        hist("no epoch", {"streams": {"s1": {"offset": 4}}})
        hist("unknown stream key", {"streams": {"nope": {"offset": 1, "epoch": epoch}}})
        now = int(time.time())
        hist("since now-60", {"since": now - 60})
        hist("since now+60", {"since": now + 60})
        hist("since 0", {"since": 0})
        hist("since + streams(off 4)", {"since": now - 60, "streams": {"s1": {"offset": 4, "epoch": epoch}}})
        hist("empty history", {})
        # subscribe with history on a fresh connection
        ws2 = WS(srv.port); ws2.recv()
        ws2.send({"command": "subscribe", "identifier": ident("s1"), "history": {"streams": {"s1": {"offset": 3, "epoch": epoch}}}})
        out.append("subscribe+history(3): " + " | ".join(norm(m, epoch) for m in ws2.recv_all(0.5)))
        ws3 = WS(srv.port); ws3.recv()
        ws3.send({"command": "subscribe", "identifier": ident("s1"), "history": {"streams": {"s1": {"offset": 1, "epoch": epoch}}}})
        out.append("subscribe+history(1, too old): " + " | ".join(norm(m, epoch) for m in ws3.recv_all(0.5)))
        ws3.send({"command": "subscribe", "identifier": ident("never"), "history": {"since": now - 60}})
        out.append("subscribe never + since: " + " | ".join(norm(m, epoch) for m in ws3.recv_all(0.5)))
        ws3.send({"command": "history", "identifier": ident("never"), "history": {"streams": {"never": {"offset": 0, "epoch": epoch}}}})
        out.append("history never offset0: " + " | ".join(norm(m, epoch) for m in ws3.recv_all(0.5)))
        # session restore
        ws.close()
        time.sleep(0.3)
        wr = WS(srv.port, path=f"/cable?sid={sid}")
        w2 = wr.recv()
        w2s = json.dumps(w2, sort_keys=True).replace(sid, "<SID>")
        if isinstance(w2, dict) and w2.get("sid"): w2s = w2s.replace(w2["sid"], "<NEWSID>")
        out.append("restore welcome: " + w2s)
        srv.broadcast({"stream": "s1", "data": "\"after-restore\""})
        out.append("restored live: " + " | ".join(norm(m, epoch) for m in wr.recv_all(0.5)))
        wr.close()
        # ttl expiry
        time.sleep(6.5)
        hist2 = WS(srv.port); hist2.recv()
        def h2(label, h, stream="s1"):
            hist2.send({"command": "subscribe", "identifier": ident(stream)}); hist2.recv_all(0.3)
            hist2.send({"command": "history", "identifier": ident(stream), "history": h})
            out.append(label + ": " + " | ".join(norm(m, epoch) for m in hist2.recv_all(0.5)))
        h2("after ttl: from 6", {"streams": {"s1": {"offset": 6, "epoch": epoch}}})
        h2("after ttl: from 5", {"streams": {"s1": {"offset": 5, "epoch": epoch}}})
        h2("after ttl: since 0", {"since": 0})
        srv.broadcast({"stream": "s1", "data": "\"post-expiry\""})
        out.append("post-expiry live: " + " | ".join(norm(m, epoch) for m in hist2.recv_all(0.5)))
        h2("post-expiry from 6", {"streams": {"s1": {"offset": 6, "epoch": epoch}}})
        h2("post-expiry from 5", {"streams": {"s1": {"offset": 5, "epoch": epoch}}})
        wr2 = WS(srv.port, path=f"/cable?sid={sid}")
        w3 = wr2.recv()
        out.append("restore after sessions_ttl: restored=%s" % (w3.get("restored") if isinstance(w3, dict) else w3))
    finally:
        srv.stop()
    return out

a = scenario("go"); b = scenario("rust")
for i in range(max(len(a), len(b))):
    x = a[i] if i < len(a) else "<missing>"; y = b[i] if i < len(b) else "<missing>"
    print(("SAME " if x == y else "DIFF ") + x)
    if x != y: print("     RUST: " + y)
