import hmac, hashlib, time, urllib.parse, base64, json
APP = b"s3cr3t"
def b64(x): return base64.urlsafe_b64encode(x).rstrip(b"=")
def jwt(payload, key=APP):
    h = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode()); p = b64(json.dumps(payload).encode())
    return (h + b"." + p + b"." + b64(hmac.new(key, h + b"." + p, hashlib.sha256).digest())).decode()
tok = jwt({"ext": json.dumps({"user": "u1"}), "exp": int(time.time()) + 600})
ident = urllib.parse.quote(json.dumps({"channel": "$pusher", "stream": "private-secret-room"}))
cmp("SSE (JWT only, no Pusher signature) subscribes to Pusher private channel", req("GET", "/events?identifier=" + ident + "&jid=" + tok, {"X-Request-ID": "r1"}), wait=0.8)
ident2 = urllib.parse.quote(json.dumps({"channel": "$pusher", "stream": "presence-room"}))
cmp("SSE presence channel w/o signature", req("GET", "/events?identifier=" + ident2 + "&jid=" + tok, {"X-Request-ID": "r1"}), wait=0.8)
