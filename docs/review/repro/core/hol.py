import sys, time, json, socket, threading
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); n = int(sys.argv[2])
conns = []
t0 = time.time()
for i in range(n):
    w = WS("127.0.0.1", port)
    m = w.recv_json(5)
    assert m.get("type") == "welcome", m
    w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "SlowChannel", "i": i})})
    conns.append(w)
print(f"{n} clients sent RPC-bound subscribe in {time.time()-t0:.2f}s")
time.sleep(0.5)
# now a fresh client that needs no RPC at all
t1 = time.time()
w = WS("127.0.0.1", port)
try:
    m = w.recv_json(40)
    print(f"fresh client welcome after {time.time()-t1:.2f}s: {m}")
    w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"})})
    m = w.recv_json(40)
    print(f"fresh client public-stream confirm after {time.time()-t1:.2f}s: {m}")
except Exception as e:
    print(f"fresh client: no reply after {time.time()-t1:.2f}s ({e!r})")
# an existing busy... different existing idle client? measure ping reply on fresh client
try:
    t2 = time.time(); w.send_json({"command": "ping"}); m = w.recv_json(40); print(f"ping reply after {time.time()-t2:.2f}s: {m}")
except Exception as e:
    print(f"ping: {e!r}")
