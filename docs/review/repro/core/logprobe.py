import sys, time, json, socket
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1])
w = WS("127.0.0.1", port)
out = []
def rx(t=1.0):
    try:
        m = w.recv_json(t); out.append(m); return m
    except Exception as e:
        out.append(f"<{type(e).__name__}>"); return None
rx()
w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"})}); rx()
w.send_json({"command": "subscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"})}); rx(0.5)
w.send_json({"command": "message", "identifier": "nope", "data": "{}"}); rx(0.5)
w.send_json({"command": "bogus", "identifier": "x"}); rx(0.5)
w.send_json({"command": "message", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"}), "data": {"a": 1}}); rx(0.5)
w.send_json({"command": "history", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"}), "history": {"since": 1}}); rx(0.5)
w.send_json({"command": "unsubscribe", "identifier": json.dumps({"channel": "$pubsub", "stream_name": "public1"})}); rx(0.5)
w.send("this is not json"); rx(0.5)
w.send_json({"command": "ping"}); rx(0.5)
for m in out: print(m)
w.close()
