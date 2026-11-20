import sys, time, json, socket, os, signal
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); pid = int(sys.argv[2])
w = WS("127.0.0.1", port); print(w.recv_json(3).get("type"))
w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"})}); print(w.recv_json(3).get("type"))
os.kill(pid, signal.SIGTERM); t=time.time()
try:
    while True:
        m = w.recv_json(5, skip_ping=False); print(f"+{time.time()-t:.2f}s", m)
        if "__close__" in m: break
except Exception as e:
    print(f"+{time.time()-t:.2f}s", repr(e))
