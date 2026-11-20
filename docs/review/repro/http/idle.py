import socket, time, threading
def t(port, name, out):
    res=[]
    # 1. keep-alive idle after a request
    s = socket.create_connection(("127.0.0.1", port)); s.settimeout(1)
    s.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n"); r = s.recv(4096)
    t0=time.time(); closed=None
    while time.time()-t0 < 12:
        try:
            d = s.recv(4096)
            if d == b"": closed = time.time()-t0; break
        except socket.timeout: pass
    res.append("idle keep-alive conn closed by server after: %s" % ("%.1fs" % closed if closed else "not within 12s"))
    if not closed:
        s.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
        try: res.append("  second request after idle: %r" % s.recv(4096).split(b"\r\n")[0])
        except Exception as e: res.append("  second request err %s" % e)
    # 2. fresh connection sending nothing
    s = socket.create_connection(("127.0.0.1", port)); s.settimeout(1)
    t0=time.time(); closed=None; data=b""
    while time.time()-t0 < 12:
        try:
            d = s.recv(4096)
            if d == b"": closed = time.time()-t0; break
            data += d
        except socket.timeout: pass
    res.append("silent fresh conn closed after: %s data=%r" % ("%.1fs" % closed if closed else "not within 12s", data[:60]))
    # 3. partial headers trickle
    s = socket.create_connection(("127.0.0.1", port)); s.settimeout(1)
    s.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n")
    t0=time.time(); closed=None; data=b""
    while time.time()-t0 < 12:
        try:
            s.sendall(b"X-A: b\r\n")
        except Exception as e:
            closed = time.time()-t0; break
        try:
            d = s.recv(4096)
            if d == b"": closed = time.time()-t0; break
            data += d
        except socket.timeout: pass
    res.append("slow headers conn closed after: %s data=%r" % ("%.1fs" % closed if closed else "not within 12s", data[:60]))
    # 4. slow body
    s = socket.create_connection(("127.0.0.1", port)); s.settimeout(1)
    s.sendall(b"POST /api/publish HTTP/1.1\r\nHost: x\r\nContent-Length: 1000\r\n\r\n")
    t0=time.time(); closed=None; data=b""
    while time.time()-t0 < 12:
        try: s.sendall(b"x")
        except Exception as e: closed = time.time()-t0; break
        try:
            d = s.recv(4096)
            if d == b"": closed = time.time()-t0; break
            data += d
        except socket.timeout: pass
    res.append("slow body (unauth POST /api/publish) closed after: %s data=%r" % ("%.1fs" % closed if closed else "not within 12s", data[:40]))
    out[name]=res
out={}
ths=[threading.Thread(target=t, args=(48213,"rust",out)), threading.Thread(target=t, args=(48214,"go",out))]
[x.start() for x in ths]; [x.join() for x in ths]
for k,v in out.items():
    print(k); [print("  ", l) for l in v]
