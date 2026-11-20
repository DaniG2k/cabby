import socket, json, os, time
class Nats:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.f = self.s.makefile("rb")
        self.info = self.f.readline()
        self.s.sendall(b'CONNECT {"verbose":false,"pedantic":false,"name":"probe","lang":"py","version":"0","protocol":1,"headers":true,"no_responders":true}\r\nPING\r\n')
        self.sid = 0
        self._until_pong()
    def _until_pong(self):
        while True:
            l = self.f.readline()
            if l.startswith(b"PONG"): return
            if l.startswith(b"-ERR"): raise RuntimeError(l)
    def publish(self, subj, data=b""):
        if isinstance(data, str): data = data.encode()
        self.s.sendall(b"PUB %s %d\r\n%s\r\n" % (subj.encode(), len(data), data))
    def request(self, subj, data=b"", timeout=3):
        if isinstance(data, (dict, list)): data = json.dumps(data)
        if isinstance(data, str): data = data.encode()
        self.sid += 1
        inbox = "_INBOX.probe.%s" % os.urandom(6).hex()
        self.s.sendall(b"SUB %s %d\r\nUNSUB %d 1\r\nPUB %s %s %d\r\n%s\r\n" % (inbox.encode(), self.sid, self.sid, subj.encode(), inbox.encode(), len(data), data))
        self.s.settimeout(timeout)
        while True:
            l = self.f.readline()
            if l.startswith(b"PING"): self.s.sendall(b"PONG\r\n"); continue
            if l.startswith(b"MSG"):
                n = int(l.split()[-1]); d = self.f.read(n + 2)[:-2]
                try: return json.loads(d)
                except Exception: return d
            if l.startswith(b"HMSG"):
                parts = l.split(); hn = int(parts[-2]); n = int(parts[-1]); d = self.f.read(n + 2)[:-2]
                return {"_headers": d[:hn].decode(errors="replace"), "_body": d[hn:].decode(errors="replace")}
            if not l: raise RuntimeError("closed")
    def js(self, api, data=b""):
        return self.request("$JS.API." + api, data)
