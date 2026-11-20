import sys, time, json, socket
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); n = int(sys.argv[2])
conns = []
for i in range(n):
    w = WS("127.0.0.1", port); m = w.recv_json(10); assert m.get("type") == "welcome", m; conns.append(w)
t0 = time.time()
for i, w in enumerate(conns):
    w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "SlowChannel", "i": i})})
print(f"{n} idle clients each sent one RPC-bound subscribe")
time.sleep(0.5)
t1 = time.time()
w = WS("127.0.0.1", port)
try:
    m = w.recv_json(120)
    print(f"fresh client: welcome after {time.time()-t1:.2f}s")
    w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"})})
    m = w.recv_json(120)
    print(f"fresh client: public-stream {m.get('type')} after {time.time()-t1:.2f}s")
except Exception as e:
    print(f"fresh client: no reply after {time.time()-t1:.2f}s ({e!r})")
