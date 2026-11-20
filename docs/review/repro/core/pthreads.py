import sys, time
sys.path.insert(0, ".")
from wsc import WS
port = int(sys.argv[1]); pid = int(sys.argv[2]); n = int(sys.argv[3]); path = sys.argv[4]
def threads():
    for l in open(f"/proc/{pid}/status"):
        if l.startswith("Threads:"): return int(l.split()[1])
print("threads before:", threads())
cs = []
for i in range(n):
    w = WS("127.0.0.1", port, path=path); cs.append(w)
m = cs[0].recv(3); print("first frame:", m[1][:120])
time.sleep(1.5)
print(f"threads with {n} idle connections:", threads())
time.sleep(35)
print(f"threads with {n} idle connections after 35s:", threads())
