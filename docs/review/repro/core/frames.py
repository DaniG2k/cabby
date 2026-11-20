import sys, time, json, socket, struct, os
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1])
ID = json.dumps({"channel": "$pubsub", "stream_name": "s"})
def frame(op, data, fin=True, mask=True, rsv=0):
    hdr = bytes([(0x80 if fin else 0) | rsv | op]); n = len(data); m = 0x80 if mask else 0
    if n < 126: hdr += bytes([m | n])
    elif n < 65536: hdr += bytes([m | 126]) + struct.pack(">H", n)
    else: hdr += bytes([m | 127]) + struct.pack(">Q", n)
    if mask:
        k = os.urandom(4); return hdr + k + bytes(b ^ k[i % 4] for i, b in enumerate(data))
    return hdr + data
def case(label, raw_frames, n=2, path="/cable", headers=None):
    try:
        w = WS("127.0.0.1", port, path=path, headers=headers)
    except Exception as e:
        print(label, "handshake:", str(e).split("\r\n")[0][:80]); return
    res = []
    try:
        m = w.recv_json(2); res.append(m.get("type"))
        for f in raw_frames: w.s.sendall(f)
        for _ in range(n):
            op, p = w.recv(1.5)
            if op == 8: res.append(("close", struct.unpack(">H", p[:2])[0] if len(p) >= 2 else None, p[2:].decode(errors="replace")))
            elif op == 1:
                m = json.loads(p)
                if m.get("type") == "ping": continue
                res.append(m)
            else: res.append((op, p[:30]))
    except Exception as e: res.append(f"<{type(e).__name__}>")
    print(label, res); w.close()
sub = json.dumps({"command": "subscribe", "identifier": ID}).encode()
case("text", [frame(1, sub)])
case("binary", [frame(2, sub)])
case("fragmented", [frame(1, sub[:10], fin=False), frame(0, sub[10:])])
case("ping", [frame(9, b"hi")])
case("pong-unsolicited", [frame(10, b""), frame(1, sub)])
case("close1000", [frame(8, struct.pack(">H", 1000) + b"bye")])
case("close4000", [frame(8, struct.pack(">H", 4000) + b"custom")])
case("close-empty", [frame(8, b"")])
case("close1005-invalid", [frame(8, struct.pack(">H", 1005))])
case("too-big", [frame(1, b'{"command":"ping","x":"' + b"a" * 70000 + b'"}')])
case("just-under", [frame(1, b'{"command":"ping","x":"' + b"a" * 65000 + b'"}')])
case("unmasked", [frame(1, sub, mask=False)])
case("rsv1", [frame(1, sub, rsv=0x40)])
case("bad-opcode", [frame(3, b"x")])
case("invalid-utf8", [frame(1, b'{"command":"ping","x":"\xff\xfe"}')])
case("big-ping", [frame(9, b"p" * 200)])
case("cont-without-start", [frame(0, b"x")])
case("empty-text", [frame(1, b""), frame(1, sub)])
case("json-null", [frame(1, b"null"), frame(1, sub)])
case("json-array", [frame(1, b"[]"), frame(1, sub)])
case("json-number-cmd", [frame(1, b'{"command":1}'), frame(1, sub)])
case("ident-nonstring", [frame(1, b'{"command":"subscribe","identifier":{"channel":"$pubsub","stream_name":"s"}}'), frame(1, sub)])
case("dup-keys", [frame(1, b'{"command":"bogus","command":"subscribe","identifier":' + json.dumps(ID).encode() + b'}')])
case("case-keys", [frame(1, b'{"Command":"subscribe","IDENTIFIER":' + json.dumps(ID).encode() + b'}')])
case("extra-trailing", [frame(1, sub + b" garbage"), frame(1, sub)])
case("deep", [frame(1, b'{"command":"message","identifier":"x","data":' + b"[" * 3000 + b"]" * 3000 + b'}'), frame(1, sub)])
case("two-subs", [frame(1, sub), frame(1, sub)], n=2)
case("pusher-path-unknown", [], path="/app/zzz")
case("path-unknown", [], path="/nope")
case("origin-any", [frame(1, sub)], headers={"Origin": "http://evil.example"})
case("subproto-unknown", [frame(1, sub)], headers={"Sec-WebSocket-Protocol": "foo"})
case("subproto-both", [frame(1, sub)], headers={"Sec-WebSocket-Protocol": "actioncable-v1-ext-json, actioncable-v1-json"})
case("query-pi", [], n=2, path="/cable?pi=1&ptp=ms")
