import sys, time, json
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1])
for i in range(3):
    t = time.time(); w = WS("127.0.0.1", port)
    try:
        while True:
            m = w.recv_json(8); print(f"conn{i} +{time.time()-t:.1f}s", json.dumps(m, sort_keys=True))
            if "__close__" in m: break
    except Exception as e: print(f"conn{i} +{time.time()-t:.1f}s", repr(e))
