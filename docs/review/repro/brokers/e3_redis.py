import sys, time, json, os
from srv import Server
from lib import WS, Redis
RPORT = 32768
def ident(stream): return json.dumps({"channel": "$pubsub", "stream_name": stream})

def run(kind):
    tag = "rvwbrk" + os.urandom(3).hex()
    cfg = f"{os.path.dirname(os.path.abspath(__file__))}/cfg-{kind}-{tag}.toml"
    open(cfg, "w").write(f'''
[redis_pubsub]
channel = "{tag}_internal"
[redis_stream_broadcast]
stream = "{tag}_stream"
group = "{tag}_bx"
stream_read_block_milliseconds = 500
[redis_pubsub_broadcast]
channel = "{tag}_legacy"
''')
    srv = Server(kind, ["--public", "--pubsub=redis", "--broker=memory", "--broadcast_adapter=http,redisx,redis", f"--redis_url=redis://127.0.0.1:{RPORT}?client_name={tag}", f"--config-path={cfg}", "--log_level=debug"], name="redis-" + kind)
    r = Redis(RPORT)
    out = []
    try:
        time.sleep(1.0)
        X = tag + "-x"
        ws = WS(srv.port); w = ws.recv(); sid = w["sid"]
        ws.send({"command": "subscribe", "identifier": ident(X)}); ws.recv_all(0.5)
        out.append("numsub stream/internal/legacy: %r" % r.cmd("PUBSUB", "NUMSUB", X, tag + "_internal", tag + "_legacy"))
        marker = [0]
        def probe(label):
            marker[0] += 1
            r.cmd("PUBLISH", X, json.dumps({"stream": X, "data": json.dumps({"probe": marker[0]})}))
            got = ws.recv_all(0.35)
            alive = srv.alive()
            s = json.dumps(got, sort_keys=True).replace(tag, "<T>")
            ok = any(isinstance(m, dict) and m.get("message") == {"probe": marker[0]} for m in got)
            out.append(f"{label}: alive={alive} probe_ok={ok} got={s[:400]}")
        bad = [b"", b"null", b"[]", b"[1,2]", b"[null]", b"{}", b'"str"', b"123", b'{"stream":123}', b'{"stream":"%s","data":123}' % X.encode(),
               b'{"stream":"%s","data":{"a":1}}' % X.encode(), b'{"stream":"%s"}' % X.encode(), b'{"stream":"%s","data":null}' % X.encode(),
               b'{"command":"disconnect","payload":123}', b'{"command":"disconnect","payload":{"identifier":123}}', b'{"command":"disconnect","payload":null}',
               b'{"command":"disconnect"}', b'{"command":null}', b'{"command":"unknown","payload":{}}', b"[" * 20000, b'{"a":' * 5000 + b"1" + b"}" * 5000,
               b"\xff\xfe\xfd", b'{"stream":"%s","data":"\xff\xfe"}' % X.encode(),
               b'{"stream":"%s","data":"x","offset":18446744073709551616}' % X.encode(), b'{"stream":"%s","data":"x","offset":-1}' % X.encode(),
               b'{"stream":"%s","data":"x","offset":1.5}' % X.encode(), b'{"stream":"%s","data":"x","offset":"7"}' % X.encode(),
               b'{"stream":"%s","data":"x","meta":"bad"}' % X.encode(), b'{"stream":"%s","data":"x","meta":{"transient":"yes"}}' % X.encode(),
               b'{"stream":"%s","data":"x","meta":{"exclude_socket":5}}' % X.encode(), b'{"stream":"%s","data":"x","epoch":5}' % X.encode(),
               b'[{"stream":"%s","data":"\\"b1\\""}, 5]' % X.encode(), b'[{"stream":"%s","data":"\\"b1\\""},{"stream":"%s","data":"\\"b2\\""}]' % (X.encode(), X.encode()),
               b'{"stream":"%s","data":"{\\"big\\":1e400}"}' % X.encode(), b'{"stream":"%s","data":"not json at all"}' % X.encode(),
               b'{"stream":"","data":"x"}', b'{"stream":"%s","data":"\\"dup\\"","stream":"other"}' % X.encode(),
               b'{"stream":"%s","data":"\\"t\\"","meta":{"transient":true},"offset":5,"epoch":"zz"}' % X.encode(),
               ]
        for target, chan in (("stream-chan", X), ("internal-chan", tag + "_internal"), ("legacy-chan", tag + "_legacy")):
            for i, p in enumerate(bad):
                r.cmd("PUBLISH", chan, p)
                probe(f"{target} #{i} {p[:60]!r}")
                if not srv.alive(): return out, srv
        for i, p in enumerate(bad):
            r.cmd("XADD", tag + "_stream", "*", "payload", p)
            probe(f"xadd payload #{i} {p[:60]!r}")
        r.cmd("XADD", tag + "_stream", "*", "nopayload", "x"); probe("xadd missing payload field")
        r.cmd("XADD", tag + "_stream", "*", "payload", json.dumps({"stream": X, "data": "\"viax\""}), "extra", "1"); probe("xadd valid")
        time.sleep(3)
        out.append("xpending summary: %r" % (r.cmd("XPENDING", tag + "_stream", tag + "_bx"),))
        out.append("xlen: %r" % r.cmd("XLEN", tag + "_stream"))
        # remote disconnect via internal channel
        r.cmd("PUBLISH", tag + "_internal", json.dumps({"command": "disconnect", "payload": {"identifier": "nobody", "reconnect": True}}))
        probe("after disconnect of unknown identifier")
        ws.close()
    finally:
        logs = srv.logs()
        srv.stop()
        time.sleep(0.5)
        out.append("after shutdown: consumers=%r numsub=%r" % (r.cmd("XINFO", "CONSUMERS", tag + "_stream", tag + "_bx"), r.cmd("PUBSUB", "NUMSUB", tag + "_internal", tag + "_legacy")))
        r.cmd("DEL", tag + "_stream")
        panics = [l[:300] for l in logs.splitlines() if "panic" in l.lower()]
        out.append("panic lines: %d %r" % (len(panics), panics[:5]))
    return out, srv

a, sa = run("go"); b, sb = run("rust")
import re
for i in range(max(len(a), len(b))):
    x = a[i] if i < len(a) else "<missing>"; y = b[i] if i < len(b) else "<missing>"
    x2 = re.sub(r"rvwbrk[0-9a-f]{6}", "<T>", x); y2 = re.sub(r"rvwbrk[0-9a-f]{6}", "<T>", y)
    if x2 != y2:
        print("DIFF GO  : " + x2[:700]); print("     RUST: " + y2[:700])
print("total lines", len(a), len(b), "logs:", sa.log, sb.log)
