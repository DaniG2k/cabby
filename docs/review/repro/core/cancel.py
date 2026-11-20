import sys, time, json, urllib.request
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); bport = int(sys.argv[2])
w = WS("127.0.0.1", port); m = w.recv_json(3); sid = m["sid"]
w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "SlowChannel"})})
time.sleep(0.4); t = time.time()
body = json.dumps({"command": "disconnect", "payload": {"identifier": json.dumps({"sid": sid}, separators=(",", ":")), "reconnect": False}}).encode()
r = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{bport}/_broadcast", data=body, method="POST")); print("remote disconnect POST:", r.status)
try:
    while True:
        m = w.recv_json(4); print(f"+{time.time()-t:.2f}s client got", m)
        if "__close__" in m: break
except Exception as e: print(f"+{time.time()-t:.2f}s", repr(e))
time.sleep(0.5)
met = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics").read().decode()
print(f"+{time.time()-t:.2f}s metrics:", [l for l in met.splitlines() if l.startswith(("anycable_go_rpc_canceled_total", "anycable_go_rpc_call_total", "anycable_go_rpc_pending_num", "anycable_go_rpc_error_total"))])
time.sleep(3)
met = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics").read().decode()
print(f"+{time.time()-t:.2f}s metrics:", [l for l in met.splitlines() if l.startswith(("anycable_go_rpc_canceled_total", "anycable_go_rpc_call_total", "anycable_go_rpc_error_total"))])
