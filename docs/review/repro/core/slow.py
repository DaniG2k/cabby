import sys, time, json, socket, threading, http.client
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); bport = int(sys.argv[2]); pid = int(sys.argv[3])
ID = json.dumps({"channel": "$pubsub", "stream_name": "s"})
def rss():
    for l in open(f"/proc/{pid}/status"):
        if l.startswith("VmRSS:"): return int(l.split()[1]) // 1024
slow = WS("127.0.0.1", port); slow.recv_json(3); slow.send_json({"command": "subscribe", "identifier": ID}); slow.recv_json(3)
slow.s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
ok = WS("127.0.0.1", port); ok.recv_json(3); ok.send_json({"command": "subscribe", "identifier": ID}); ok.recv_json(3)
got = [0]
def reader():
    try:
        while True:
            m = ok.recv_json(5)
            if "__close__" in m: break
            got[0] += 1
    except Exception: pass
threading.Thread(target=reader, daemon=True).start()
N = 3000; payload = json.dumps("x" * 10000)
c = http.client.HTTPConnection("127.0.0.1", bport); peak = rss(); t0 = time.time()
for i in range(N):
    c.request("POST", "/_broadcast", body=json.dumps({"stream": "s", "data": payload})); r = c.getresponse(); r.read()
    if i % 100 == 0: peak = max(peak, rss())
t1 = time.time(); time.sleep(3); peak = max(peak, rss())
print(f"published {N} x 10KB in {t1-t0:.1f}s; healthy client got {got[0]}; rss peak {peak} MB")
# now read whatever the slow client has
n = 0; last = None; t = time.time()
try:
    while True:
        m = slow.recv_json(3); 
        if "__close__" in m or m.get("type") == "disconnect": last = m; print("slow client got:", m)
        else: n += 1
        if "__close__" in m: break
except Exception as e:
    print("slow client end:", repr(e))
print(f"slow client drained {n} messages")
