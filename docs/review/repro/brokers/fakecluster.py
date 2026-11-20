"""A fake single-node Redis Cluster: owns all slots, records PUBLISH commands, supports RESP2/RESP3 handshakes and SUBSCRIBE."""
import socket, threading, time, sys, json
class Fake:
    def __init__(self, port, delay=0.0):
        self.port = port; self.delay = delay
        self.published = []; self.commands = []; self.lock = threading.Lock()
        self.ls = socket.socket(); self.ls.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ls.bind(("127.0.0.1", port)); self.ls.listen(64)
        threading.Thread(target=self.accept, daemon=True).start()
    def accept(self):
        while True:
            try: c, _ = self.ls.accept()
            except OSError: return
            threading.Thread(target=self.serve, args=(c,), daemon=True).start()
    def serve(self, c):
        f = c.makefile("rb"); resp3 = False; subs = 0
        def read():
            line = f.readline()
            if not line: return None
            if not line.startswith(b"*"): return [line.strip()]
            n = int(line[1:]); args = []
            for _ in range(n):
                l = int(f.readline()[1:]); args.append(f.read(l + 2)[:-2])
            return args
        def bulk(b): return b"$%d\r\n%s\r\n" % (len(b), b)
        try:
            while True:
                a = read()
                if a is None: return
                cmd = a[0].upper()
                with self.lock: self.commands.append(b" ".join(a[:3])[:80])
                if cmd == b"HELLO":
                    resp3 = len(a) > 1 and a[1] == b"3"
                    if resp3: c.sendall(b"%3\r\n" + bulk(b"server") + bulk(b"redis") + bulk(b"version") + bulk(b"7.2.0") + bulk(b"proto") + b":3\r\n")
                    else: c.sendall(b"*6\r\n" + bulk(b"server") + bulk(b"redis") + bulk(b"version") + bulk(b"7.2.0") + bulk(b"proto") + b":2\r\n")
                elif cmd in (b"AUTH", b"SELECT", b"READONLY", b"READWRITE"): c.sendall(b"+OK\r\n")
                elif cmd == b"CLIENT": c.sendall(b"+OK\r\n")
                elif cmd == b"PING": c.sendall(b"+PONG\r\n")
                elif cmd == b"CLUSTER" and a[1].upper() == b"SLOTS":
                    c.sendall(b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n" + bulk(b"127.0.0.1") + b":%d\r\n" % self.port + bulk(b"a" * 40))
                elif cmd == b"SUBSCRIBE":
                    for ch in a[1:]:
                        subs += 1
                        c.sendall((b">3\r\n" if resp3 else b"*3\r\n") + bulk(b"subscribe") + bulk(ch) + b":%d\r\n" % subs)
                elif cmd == b"UNSUBSCRIBE":
                    for ch in a[1:]:
                        subs = max(0, subs - 1)
                        c.sendall((b">3\r\n" if resp3 else b"*3\r\n") + bulk(b"unsubscribe") + bulk(ch) + b":%d\r\n" % subs)
                elif cmd == b"PUBLISH":
                    if self.delay: time.sleep(self.delay)
                    with self.lock: self.published.append(a[1].decode())
                    c.sendall(b":0\r\n")
                else:
                    c.sendall(b"-ERR unknown command '" + a[0] + b"'\r\n")
        except Exception as e:
            pass
