import socket, os, base64, struct, json, time, select

class WS:
    def __init__(self, port, path="/cable", headers=None, timeout=5):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        key = base64.b64encode(os.urandom(16)).decode()
        h = f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: actioncable-v1-ext-json\r\nOrigin: http://127.0.0.1\r\n"
        for k, v in (headers or {}).items():
            h += f"{k}: {v}\r\n"
        self.s.sendall((h + "\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = self.s.recv(4096)
            if not d: raise RuntimeError("closed during handshake: %r" % buf)
            buf += d
        head, rest = buf.split(b"\r\n\r\n", 1)
        self.head = head.decode()
        if " 101 " not in self.head.split("\r\n")[0]:
            raise RuntimeError("handshake failed: " + self.head)
        self.buf = rest
        self.closed = None

    def send(self, obj):
        data = (obj if isinstance(obj, str) else json.dumps(obj)).encode()
        mask = os.urandom(4)
        n = len(data)
        hdr = bytes([0x81])
        if n < 126: hdr += bytes([0x80 | n])
        elif n < 65536: hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
        else: hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
        self.s.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def _fill(self, n, deadline):
        while len(self.buf) < n:
            t = deadline - time.time()
            if t <= 0: return False
            r, _, _ = select.select([self.s], [], [], t)
            if not r: return False
            d = self.s.recv(65536)
            if not d:
                self.closed = self.closed or "eof"
                return False
            self.buf += d
        return True

    def recv(self, timeout=2.0):
        """Returns next text frame decoded as JSON (or raw str), None on timeout/close."""
        deadline = time.time() + timeout
        while True:
            if not self._fill(2, deadline): return None
            b0, b1 = self.buf[0], self.buf[1]
            n = b1 & 0x7f; off = 2
            if n == 126:
                if not self._fill(4, deadline): return None
                n = struct.unpack(">H", self.buf[2:4])[0]; off = 4
            elif n == 127:
                if not self._fill(10, deadline): return None
                n = struct.unpack(">Q", self.buf[2:10])[0]; off = 10
            if not self._fill(off + n, deadline): return None
            payload = self.buf[off:off + n]; self.buf = self.buf[off + n:]
            op = b0 & 0x0f
            if op == 8:
                code = struct.unpack(">H", payload[:2])[0] if len(payload) >= 2 else None
                self.closed = ("close", code, payload[2:].decode(errors="replace"))
                return None
            if op in (9, 10): continue
            txt = payload.decode(errors="replace")
            try: return json.loads(txt)
            except Exception: return txt

    def recv_all(self, timeout=1.0, skip_ping=True):
        out = []
        end = time.time() + timeout
        while time.time() < end:
            m = self.recv(max(0.01, end - time.time()))
            if m is None:
                if self.closed: break
                continue
            if skip_ping and isinstance(m, dict) and m.get("type") == "ping": continue
            out.append(m)
        return out

    def close(self):
        try: self.s.close()
        except Exception: pass

class Redis:
    def __init__(self, port, host="127.0.0.1"):
        self.s = socket.create_connection((host, port), timeout=5)
        self.f = self.s.makefile("rb")
    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            if not isinstance(a, bytes): a = str(a).encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.s.sendall(out)
        return self._read()
    def _read(self):
        line = self.f.readline()
        t, rest = line[:1], line[1:-2]
        if t == b"+": return rest.decode()
        if t == b"-": return Exception(rest.decode())
        if t == b":": return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0: return None
            d = self.f.read(n + 2)[:-2]
            return d
        if t == b"*":
            n = int(rest)
            if n < 0: return None
            return [self._read() for _ in range(n)]
        raise RuntimeError("bad reply %r" % line)
