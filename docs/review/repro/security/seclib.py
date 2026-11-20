"""Shared helpers for the security repro scripts (stdlib only).

Every script takes the server binary as its first argument, so the same probe
runs against Cabby (target/release/cabby) and the Go reference
(target/reference-bin/anycable-go).
"""
import base64, hashlib, hmac, json, os, socket, struct, subprocess, sys, time, urllib.request


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Server:
    """Starts the server under test and collects its output in a file."""

    def __init__(self, binary, args, env=None, name="server"):
        self.port = free_port()
        self.log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{name}.{self.port}.log")
        self.log = open(self.log_path, "wb")
        full_env = dict(os.environ, ANYCABLE_DISABLE_TELEMETRY="true")
        full_env.update(env or {})
        cmd = [binary, f"--port={self.port}", "--host=127.0.0.1"] + args
        self.cmd = cmd
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=subprocess.STDOUT, env=full_env)
        deadline = time.time() + 15
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise SystemExit(f"server exited at start ({self.proc.returncode}); see {self.log_path}")
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.1)
        raise SystemExit("server did not start listening")

    def alive(self):
        return self.proc.poll() is None

    def health(self):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/health", timeout=3) as r:
                return r.status
        except Exception as e:  # noqa: BLE001
            return f"{type(e).__name__}: {e}"

    def status(self, key):
        """A field of /proc/<pid>/status (e.g. Threads, VmRSS), as an int."""
        try:
            for line in open(f"/proc/{self.proc.pid}/status"):
                if line.startswith(key + ":"):
                    return int(line.split()[1])
        except OSError:
            pass
        return -1

    def output(self):
        self.log.flush()
        return open(self.log_path, "rb").read().decode("utf-8", "replace")

    def stop(self):
        if self.alive():
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.log.close()


class WS:
    """A minimal RFC 6455 client (text frames, masked)."""

    def __init__(self, port, path="/cable", headers=None, protocol=None, timeout=5):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        lines = [
            f"GET {path} HTTP/1.1",
            f"Host: 127.0.0.1:{port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {key}",
            "Sec-WebSocket-Version: 13",
        ]
        if protocol:
            lines.append(f"Sec-WebSocket-Protocol: {protocol}")
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        self.sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
        self.buf = b""
        head = self._until(b"\r\n\r\n")
        self.status_line = head.split(b"\r\n", 1)[0].decode()
        if b" 101 " not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"handshake failed: {self.status_line}")

    def _until(self, marker):
        while marker not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed during handshake")
            self.buf += chunk
        head, self.buf = self.buf.split(marker, 1)
        return head

    def _need(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send(self, text, opcode=1):
        data = text.encode() if isinstance(text, str) else text
        mask = os.urandom(4)
        head = bytes([0x80 | opcode])
        n = len(data)
        if n < 126:
            head += bytes([0x80 | n])
        elif n < 65536:
            head += bytes([0x80 | 126]) + struct.pack(">H", n)
        else:
            head += bytes([0x80 | 127]) + struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data)) if n < 4096 else _mask(data, mask)
        self.sock.sendall(head + mask + masked)

    def send_json(self, obj):
        self.send(json.dumps(obj))

    def recv(self, timeout=3):
        """(opcode, payload) of the next frame; raises on timeout or close."""
        self.sock.settimeout(timeout)
        b0, b1 = self._need(2)
        n = b1 & 0x7F
        if n == 126:
            (n,) = struct.unpack(">H", self._need(2))
        elif n == 127:
            (n,) = struct.unpack(">Q", self._need(8))
        payload = self._need(n)
        return b0 & 0x0F, payload

    def recv_json(self, timeout=3, skip=("ping",)):
        """The next text frame as JSON, skipping pings."""
        deadline = time.time() + timeout
        while True:
            left = deadline - time.time()
            if left <= 0:
                raise TimeoutError("no message")
            op, payload = self.recv(left)
            if op == 8:
                raise ConnectionError(f"close frame {payload!r}")
            if op != 1:
                continue
            msg = json.loads(payload)
            if isinstance(msg, dict) and (msg.get("type") in skip or msg.get("event") == "pusher:ping"):
                continue
            return msg

    def try_json(self, timeout=2):
        try:
            return self.recv_json(timeout)
        except Exception as e:  # noqa: BLE001
            return f"<{type(e).__name__}: {e}>"

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def _mask(data, mask):
    n = len(data)
    key = int.from_bytes((mask * (n // 4 + 1))[:n], "big")
    return (int.from_bytes(data, "big") ^ key).to_bytes(n, "big")


def http_post(port, path, body, headers=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, method="POST", headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code


def pusher_auth(app_key, secret, socket_id, channel, channel_data=None):
    s = f"{socket_id}:{channel}" + (f":{channel_data}" if channel_data is not None else "")
    return f"{app_key}:{hmac.new(secret.encode(), s.encode(), hashlib.sha256).hexdigest()}"


def binary_arg():
    if len(sys.argv) < 2:
        raise SystemExit(f"usage: python3 {sys.argv[0]} <path to cabby or anycable-go binary> [options]")
    return sys.argv[1]


def verdict(name, vulnerable, detail=""):
    print(f"[{'VULNERABLE' if vulnerable else 'ok'}] {name}{': ' + detail if detail else ''}")
