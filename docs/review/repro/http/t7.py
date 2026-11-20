b1 = '{"stream":"caf\\u00e9","data":"\\"m1\\""}'
b2 = '{"stream":"caf\\u00e9","data":"\\"m2\\""}'
cmp("pub1", req("POST", "/_broadcast", body=b1))
cmp("pub2", req("POST", "/_broadcast", body=b2))
cmp("sse utf8 stream history_since", req("GET", "/events?stream=caf%C3%A9&history_since=1", {"X-Request-ID": "r1"}), wait=0.8)
cmp("sse utf8 Last-Event-ID (as EventSource sends it: raw UTF-8)", req("GET", "/events?stream=caf%C3%A9", {"X-Request-ID": "r1", "Last-Event-ID": b"1/EPOCH/caf\xc3\xa9"}), wait=0.8)
cmp("sse ascii Last-Event-ID control", req("GET", "/events?stream=a", {"X-Request-ID": "r1", "Last-Event-ID": b"1/EPOCH/a"}), wait=0.8)
cmp("ds utf8 x-signed hdr", req("GET", "/ds/a", {"X-Request-ID": "r1", "X-Signed": b"caf\xc3\xa9"}))
cmp("x-request-id utf8", req("GET", "/events?stream=a", {"X-Request-ID": b"caf\xc3\xa9"}), wait=0.8)
cmp("origin utf8 OPTIONS", req("OPTIONS", "/events", {"Origin": b"http://caf\xc3\xa9.example.com"}))
