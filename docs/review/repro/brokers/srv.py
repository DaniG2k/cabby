import subprocess, time, socket, urllib.request, os, signal, random
ROOT = "/home/daniele/Documents/apps/cabby"
BIN = {"rust": ROOT + "/target/release/cabby", "go": ROOT + "/target/reference-bin/anycable-go"}

def free_port():
    while True:
        p = random.randint(20000, 60000)
        s = socket.socket()
        try:
            s.bind(("127.0.0.1", p)); s.close(); return p
        except OSError:
            s.close()

class Server:
    def __init__(self, kind, args, env=None, wait=True, name=None):
        self.kind = kind
        self.port = free_port()
        self.log = f"{os.path.dirname(os.path.abspath(__file__))}/log-{name or kind}-{self.port}.txt"
        self.f = open(self.log, "wb")
        e = {k: v for k, v in os.environ.items() if not k.startswith("ANYCABLE_") and k not in ("REDIS_URL", "PORT")}
        e.update(env or {})
        self.p = subprocess.Popen([BIN[kind], f"--port={self.port}", "--host=127.0.0.1", f"--http_broadcast_port={self.port}"] + args, stdout=self.f, stderr=subprocess.STDOUT, env=e)
        if wait: self.wait_up()
    def wait_up(self, timeout=10):
        end = time.time() + timeout
        while time.time() < end:
            if self.p.poll() is not None: raise RuntimeError(f"{self.kind} exited: {self.logs()}")
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.port}/health", timeout=1).read(); return
            except Exception: time.sleep(0.1)
        raise RuntimeError("not up: " + self.logs())
    def broadcast(self, payload, path="/_broadcast"):
        import json
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method="POST")
        try:
            return urllib.request.urlopen(req, timeout=5).status
        except urllib.error.HTTPError as e:
            return e.code
    def logs(self):
        self.f.flush()
        return open(self.log, "rb").read().decode(errors="replace")
    def alive(self): return self.p.poll() is None
    def stop(self):
        if self.p.poll() is None:
            self.p.send_signal(signal.SIGTERM)
            try: self.p.wait(timeout=10)
            except Exception: self.p.kill()
        self.f.close()
    def rss_kb(self):
        try:
            for l in open(f"/proc/{self.p.pid}/status"):
                if l.startswith("VmRSS"): return int(l.split()[1])
        except Exception: return None
