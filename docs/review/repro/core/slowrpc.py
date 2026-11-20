"""HTTP RPC backend: /connect, /command, /disconnect. Sleeps DELAY seconds on command (and on connect if SLOW_CONNECT)."""
import sys, json, time, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
port = int(sys.argv[1]); delay = float(sys.argv[2]); slow_connect = len(sys.argv) > 3 and sys.argv[3] == "1"
log = open(sys.argv[4], "a") if len(sys.argv) > 4 else sys.stderr
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        n = int(self.headers.get("content-length", 0)); body = self.rfile.read(n)
        log.write(f"{time.time():.3f} {self.path} {body[:200]!r}\n"); log.flush()
        if self.path.endswith("/connect"):
            if slow_connect: time.sleep(delay)
            out = {"status": 1, "identifiers": "{\"id\":\"1\"}", "transmissions": ["{\"type\":\"welcome\"}"]}
        elif self.path.endswith("/command"):
            time.sleep(delay)
            req = json.loads(body)
            out = {"status": 1, "streams": ["s1"], "transmissions": [json.dumps({"type": "confirm_subscription", "identifier": req.get("identifier")})]}
        else:
            out = {"status": 1}
        data = json.dumps(out).encode()
        try:
            self.send_response(200); self.send_header("content-type", "application/json"); self.send_header("content-length", str(len(data))); self.end_headers(); self.wfile.write(data)
        except Exception as e:
            log.write(f"{time.time():.3f} write failed {e}\n"); log.flush()
ThreadingHTTPServer.request_queue_size = 512
ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
