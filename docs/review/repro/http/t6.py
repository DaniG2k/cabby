import hmac, hashlib
APP = b"s3cr3t"
bkey = hmac.new(APP, b"broadcast-cable", hashlib.sha256).hexdigest()
b = '{"stream":"a","data":"1"}'
for name, raw in [
 ("bc expect", req("POST", "/_broadcast", {"Authorization": "Bearer " + bkey, "Expect": "100-continue"}, body=b)),
 ("bc nonascii auth", req("POST", "/_broadcast", {"Authorization": b"Bearer p\xc3\xa4ss"}, body=b)),
 ("pipelined", req("GET", "/health") + req("GET", "/health")),
 ("CL+TE", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer " + bkey.encode() + b"\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"),
 ("dup CL differing", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\nabcde"),
 ("neg CL", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nContent-Length: -1\r\n\r\n"),
 ("huge CL", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999999999999999999\r\n\r\n"),
 ("no host 1.1", b"GET /health HTTP/1.1\r\n\r\n"),
 ("bad method", b"G\x01T /health HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("absolute uri", b"GET http://example.com/health HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("options *", b"OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("double slash", b"GET //health HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("dot segs", b"GET /x/../health HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("pct path", b"GET /%68ealth HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("bad pct path", b"GET /health%zz HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("space in path", b"GET /hea lth HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("http/0.9-ish", b"GET /health\r\n\r\n"),
 ("lf only", b"GET /health HTTP/1.1\nHost: x\n\n"),
 ("header w/ space before colon", b"GET /health HTTP/1.1\r\nHost : x\r\n\r\n"),
 ("obs-fold", b"GET /health HTTP/1.1\r\nHost: x\r\nX-A: a\r\n b\r\n\r\n"),
 ("120 headers", b"GET /health HTTP/1.1\r\nHost: x\r\n" + b"".join(b"X-H%d: v\r\n" % i for i in range(120)) + b"\r\n"),
 ("big header 500k", b"GET /health HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a"*500000 + b"\r\n\r\n"),
 ("big header 100k", b"GET /health HTTP/1.1\r\nHost: x\r\nX-Big: " + b"a"*100000 + b"\r\n\r\n"),
 ("long uri 70k", b"GET /health?" + b"a"*70000 + b" HTTP/1.1\r\nHost: x\r\n\r\n"),
 ("TE gzip, chunked", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer " + bkey.encode() + b"\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n"),
 ("TE identity", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer " + bkey.encode() + b"\r\nTransfer-Encoding: identity\r\n\r\n"),
 ("TE chunked bad size", b"POST /_broadcast HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer " + bkey.encode() + b"\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n\r\n"),
 ("connect", b"CONNECT example.com:80 HTTP/1.1\r\nHost: example.com:80\r\n\r\n"),
 ("upgrade h2c health", b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQCAAAAAAIAAAAA\r\n\r\n"),
 ("h2 prior knowledge", b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"),
 ("HTTP/1.0 keepalive", b"GET /health HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"),
 ("conn close", b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"),
]:
    cmp(name, raw, total=4)
