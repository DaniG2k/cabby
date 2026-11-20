"""Minimal blocking WebSocket client over raw sockets (text frames only)."""
import base64, os, socket, struct, time, json

class WS:
    def __init__(self, host, port, path="/cable", headers=None, timeout=10):
        self.s = socket.create_connection((host, port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        req = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
        for k, v in (headers or {}).items():
            req += f"{k}: {v}\r\n"
        req += "\r\n"
        self.s.sendall(req.encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = self.s.recv(4096)
            if not d:
                raise IOError("closed during handshake: %r" % buf)
            buf += d
        head, rest = buf.split(b"\r\n\r\n", 1)
        self.handshake = head.decode(errors="replace")
        self.buf = rest
        if " 101 " not in self.handshake.split("\r\n")[0]:
            raise IOError("handshake failed: " + self.handshake)

    def send(self, text, opcode=1):
        data = text.encode() if isinstance(text, str) else text
        mask = os.urandom(4)
        n = len(data)
        hdr = bytes([0x80 | opcode])
        if n < 126: hdr += bytes([0x80 | n])
        elif n < 65536: hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
        else: hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        self.s.sendall(hdr + mask + masked)

    def send_json(self, obj):
        self.send(json.dumps(obj))

    def _need(self, n):
        while len(self.buf) < n:
            d = self.s.recv(65536)
            if not d:
                raise EOFError("eof")
            self.buf += d

    def recv(self, timeout=5):
        """Returns (opcode, payload bytes); raises socket.timeout / EOFError."""
        self.s.settimeout(timeout)
        self._need(2)
        b0, b1 = self.buf[0], self.buf[1]
        n = b1 & 0x7f
        off = 2
        if n == 126:
            self._need(4); n = struct.unpack(">H", self.buf[2:4])[0]; off = 4
        elif n == 127:
            self._need(10); n = struct.unpack(">Q", self.buf[2:10])[0]; off = 10
        self._need(off + n)
        payload = self.buf[off:off + n]
        self.buf = self.buf[off + n:]
        return b0 & 0x0f, payload

    def recv_json(self, timeout=5, skip_ping=True):
        end = time.time() + timeout
        while True:
            left = end - time.time()
            if left <= 0:
                raise socket.timeout("timeout")
            op, p = self.recv(left)
            if op == 8:
                code = struct.unpack(">H", p[:2])[0] if len(p) >= 2 else None
                return {"__close__": code, "reason": p[2:].decode(errors="replace")}
            if op != 1:
                continue
            m = json.loads(p)
            if skip_ping and m.get("type") == "ping":
                continue
            return m

    def close(self):
        try: self.s.close()
        except Exception: pass
