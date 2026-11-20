b = '{"stream":"a","data":"1"}'
k = "Bearer pässwörd".encode("utf-8")
cmp("bc utf8 secret, correct", req("POST", "/_broadcast", {"Authorization": k}, body=b))
cmp("bc utf8 secret, wrong", req("POST", "/_broadcast", {"Authorization": b"Bearer p\xc3\xa4ssw\xc3\xb6rX"}, body=b))
cmp("bc no auth", req("POST", "/_broadcast", body=b))
cmp("api utf8 secret, correct", req("POST", "/api/publish", {"Authorization": k}, body=b))
cmp("api presence utf8 secret", req("GET", "/api/presence/a/users", {"Authorization": k}))
cmp("api latin1 bytes of secret", req("POST", "/api/publish", {"Authorization": "Bearer pässwörd".encode("latin1")}, body=b))
