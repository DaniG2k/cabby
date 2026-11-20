"""Backends on 127.0.0.2 and 127.0.0.3 (plain TCP, log accepts). At T_MOVE the .2 backend dies and DNS moves to .3."""
import sys, socket, threading, time, json
sys.path.insert(0, ".")
from wsc import WS
wsport = int(sys.argv[1]); rpcport = int(sys.argv[2]); dnsfile = sys.argv[3]; total = int(sys.argv[4])
T0 = time.time(); accepts = {"127.0.0.2": [], "127.0.0.3": []}; conns = {"127.0.0.2": [], "127.0.0.3": []}; alive = {"127.0.0.2": True, "127.0.0.3": True}
def backend(ip):
    ls = socket.socket(); ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); ls.bind((ip, rpcport)); ls.listen(64); ls.settimeout(0.2)
    while alive[ip]:
        try: c, _ = ls.accept()
        except socket.timeout: continue
        accepts[ip].append(round(time.time() - T0, 1)); conns[ip].append(c)
    ls.close()
    for c in conns[ip]:
        try: c.close()
        except Exception: pass
for ip in accepts: threading.Thread(target=backend, args=(ip,), daemon=True).start()
T_MOVE = 6
moved = False
while time.time() - T0 < total:
    if not moved and time.time() - T0 >= T_MOVE:
        alive["127.0.0.2"] = False; open(dnsfile, "w").write("127.0.0.3\n"); moved = True
        print(f"t={time.time()-T0:.1f}s: backend 127.0.0.2 stopped, DNS for rpc.test now 127.0.0.3")
    try:
        w = WS("127.0.0.1", wsport, timeout=2)
        try: w.recv_json(0.3)
        except Exception: pass
        w.close()
    except Exception as e: pass
    time.sleep(0.7)
for ip in accepts:
    a = accepts[ip]; print(f"connections accepted on {ip}: {len(a)}; before move: {len([x for x in a if x < T_MOVE])}; after move: {len([x for x in a if x >= T_MOVE])}; first after move at t={next((x for x in a if x >= T_MOVE), None)}")
