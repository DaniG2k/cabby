import sys, time, json
from srv import Server, free_port
from lib import WS
from natsc import Nats
def ident(stream): return json.dumps({"channel": "$pubsub", "stream_name": stream})
first, second = sys.argv[1], sys.argv[2]   # which kind hosts NATS (always go) / order of stream creation
nport = free_port()
G = Server("go", ["--public", "--embed_nats", f"--enats_addr=nats://127.0.0.1:{nport}", "--broker=nats", "--pubsub=nats", "--broadcast_adapter=http", "--log_level=debug"], name="mixed-go")
time.sleep(2.5)
R = Server("rust", ["--public", "--broker=nats", "--pubsub=nats", f"--nats_servers=nats://127.0.0.1:{nport}", "--broadcast_adapter=http", "--log_level=debug"], name="mixed-rust")
time.sleep(2.5)
S = {"go": G, "rust": R}
try:
    n = Nats(nport)
    def cfg(name):
        r = n.js("STREAM.INFO." + name)
        return r.get("config") if isinstance(r, dict) else r
    print("KV _anycable_ cfg:", json.dumps(cfg("KV__anycable_"), sort_keys=True))
    print("KV epoch cfg:", json.dumps(cfg("KV__anycable_epoch_"), sort_keys=True))
    # stream created first by `first`
    wa = WS(S[first].port); a_w = wa.recv()
    wa.send({"command": "subscribe", "identifier": ident(f"mx-{first}")}); print(first, "sub:", wa.recv_all(0.8))
    print(f"stream cfg created by {first}:", json.dumps(cfg(f"_ac_mx-{first}"), sort_keys=True))
    wb = WS(S[second].port); b_w = wb.recv()
    wb.send({"command": "subscribe", "identifier": ident(f"mx-{first}")}); print(second, "sub:", wb.recv_all(0.8))
    wb.send({"command": "subscribe", "identifier": ident(f"mx-{second}")}); print(second, "sub2:", wb.recv_all(0.8))
    print(f"stream cfg created by {second}:", json.dumps(cfg(f"_ac_mx-{second}"), sort_keys=True))
    cons = n.js("CONSUMER.LIST._ac_mx-" + first)
    print("consumers:", json.dumps([{k: c.get(k) for k in ("name", "config")} for c in (cons.get("consumers") or [])], sort_keys=True)[:900])
    st = S[first].broadcast({"stream": f"mx-{first}", "data": "\"from-first\""}); print("b1", st)
    st = S[second].broadcast({"stream": f"mx-{first}", "data": "\"from-second\""}); print("b2", st)
    print(first, "live:", wa.recv_all(1.0))
    print(second, "live:", wb.recv_all(1.0))
    ep = None
    for kind, w in ((first, wa), (second, wb)):
        for off in (0, 1, 2, 3):
            w.send({"command": "history", "identifier": ident(f"mx-{first}"), "history": {"streams": {f"mx-{first}": {"offset": off, "epoch": "?"}}}})
        pass
    print(first, "hist bad-epoch:", [m.get("type") for m in wa.recv_all(1.0)])
    print(second, "hist bad-epoch:", [m.get("type") for m in wb.recv_all(1.0)])
    # sessions across nodes
    sid_a = a_w["sid"]; sid_b = b_w["sid"]
    wa.close(); wb.close(); time.sleep(0.7)
    for label, srv, sid in ((f"{first}-session restored on {second}", S[second], sid_a), (f"{second}-session restored on {first}", S[first], sid_b)):
        w = WS(srv.port, path=f"/cable?sid={sid}"); m = w.recv()
        print(label, "->", json.dumps(m)[:300]); w.close()
    keys = n.js("STREAM.INFO.KV__anycable_", {"subjects_filter": ">"})
    print("KV subjects:", (keys.get("state") or {}).get("subjects"))
    print("epoch kv:", n.js("DIRECT.GET.KV__anycable_epoch_", {"last_by_subj": "$KV._anycable_epoch_._epoch_"}))
finally:
    for k, s in S.items():
        s.stop()
    print("=== rust log (warn/err) ===")
    print("\n".join(l[:300] for l in R.logs().splitlines() if " WRN " in l or " ERR " in l))
    print("=== go log (warn/err) ===")
    print("\n".join(l[:300] for l in G.logs().splitlines() if " WRN " in l or " ERR " in l))
