"""Scriptable HTTP RPC backend; records every request (headers of interest + body) to a log."""
import sys, json, time, re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
port = int(sys.argv[1]); log = open(sys.argv[2], "w")
def norm(s):
    s = re.sub(r'[A-Za-z0-9_-]{21}', '<SID>', s)
    return s
class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def do_POST(self):
        n = int(self.headers.get("content-length", 0)); body = self.rfile.read(n).decode()
        hs = {k.lower(): v for k, v in self.headers.items() if k.lower() not in ("host", "content-length", "user-agent", "accept-encoding")}
        log.write(norm(f"{self.command} {self.path} {json.dumps(hs, sort_keys=True)} {body}") + "\n"); log.flush()
        req = json.loads(body) if body else {}
        status = 200; out = {}
        if self.path.endswith("/connect"):
            url = (req.get("env") or {}).get("url", "")
            if "mode=reject" in url: out = {"status": 2, "transmissions": ['{"type":"disconnect","reason":"unauthorized","reconnect":false}']}
            elif "mode=error" in url: out = {"status": 0, "error_msg": "boom"}
            elif "mode=500" in url: status = 500; out = {"oops": 1}
            elif "mode=garbage" in url: self.send_response(200); self.send_header("content-length", "3"); self.end_headers(); self.wfile.write(b"{{{"); return
            elif "mode=401" in url: status = 401
            else: out = {"status": 1, "identifiers": '{"user":"u<1>"}', "transmissions": ['{"type":"welcome"}'], "env": {"cstate": {"a": "1", "tok": "x&y"}}}
        elif self.path.endswith("/command"):
            ident = req.get("identifier", ""); cmd = req.get("command"); ch = json.loads(ident).get("channel", "") if ident.startswith("{") else ident
            conf = json.dumps({"type": "confirm_subscription", "identifier": ident}); rej = json.dumps({"type": "reject_subscription", "identifier": ident})
            if cmd == "subscribe":
                if ch == "Reject": out = {"status": 2, "transmissions": [rej]}
                elif ch == "Error": out = {"status": 0, "error_msg": "sub failed"}
                elif ch == "Disc": out = {"status": 2, "disconnect": True, "transmissions": [rej]}
                elif ch == "State": out = {"status": 1, "streams": ["a", "b"], "transmissions": [conf], "env": {"cstate": {"a": "", "c": "3"}, "istate": {"k": "v"}}}
                elif ch == "NoTx": out = {"status": 1, "streams": ["a"]}
                elif ch == "Bcast": out = {"status": 1, "streams": ["a"], "transmissions": [conf], "broadcasts": [{"stream": "a", "data": "\"hello-from-rpc\""}]}
                else: out = {"status": 1, "streams": ["a"], "transmissions": [conf]}
            elif cmd == "message":
                data = req.get("data", "")
                if "stop_all" in data: out = {"status": 1, "stop_streams": True, "transmissions": ['{"x":"stopped"}']}
                elif "stop_b" in data: out = {"status": 1, "stopped_streams": ["b"], "transmissions": ['{"x":"stop_b"}']}
                elif "disc" in data: out = {"status": 1, "disconnect": True, "transmissions": ['{"type":"disconnect","reason":"go away","reconnect":true}']}
                elif "err" in data: out = {"status": 0, "error_msg": "perform failed"}
                elif "fail" in data: out = {"status": 2}
                else: out = {"status": 1, "transmissions": ['{"echo":' + json.dumps(data) + '}'], "env": {"istate": {"n": "1"}}}
            else: out = {"status": 1}
        else:
            out = {"status": 1}
        data = json.dumps(out).encode()
        self.send_response(status); self.send_header("content-type", "application/json"); self.send_header("content-length", str(len(data))); self.end_headers(); self.wfile.write(data)
ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
