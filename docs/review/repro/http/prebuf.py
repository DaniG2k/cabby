import socket, time, sys
def rss(pid):
    for l in open("/proc/%d/status" % pid):
        if l.startswith("VmRSS"): return int(l.split()[1]) // 1024
def run(port, pid, name, auth=None):
    N = 120 * 1024 * 1024
    s = socket.create_connection(("127.0.0.1", port)); s.settimeout(0.05)
    h = "POST /api/publish HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n" % (N + 1000)
    if auth: h += "Authorization: %s\r\n" % auth
    s.sendall((h + "\r\n").encode())
    r0 = rss(pid); sent = 0; chunk = b"x" * (1 << 20); resp = b""; peak = r0; err = None
    s.settimeout(2)
    try:
        while sent < N:
            s.sendall(chunk); sent += len(chunk)
            if sent % (20 << 20) == 0: peak = max(peak, rss(pid))
    except Exception as e: err = type(e).__name__
    peak = max(peak, rss(pid))
    try: resp = s.recv(4096)
    except Exception as e: resp = b"<no response yet: %s>" % type(e).__name__.encode()
    print("%s: rss before=%dMB peak=%dMB, sent=%dMB of body (never completed), send error=%s, response=%r" % (name, r0, peak, sent >> 20, err, resp.split(b"\r\n")[0]))
    s.close(); time.sleep(0.5); print("   rss after close=%dMB" % rss(pid))
run(48215, int(sys.argv[1]), "rust unauthenticated (wrong token)", "Bearer wrong")
run(48216, int(sys.argv[2]), "go   unauthenticated (wrong token)", "Bearer wrong")
