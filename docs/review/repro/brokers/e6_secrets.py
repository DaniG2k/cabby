import sys, time, os, subprocess
from srv import Server, BIN, free_port
cases = {
  "redis wrong auth": ["--public", "--pubsub=redis", "--broadcast_adapter=http,redisx,redis", "--redis_url=redis://bob:s3cretpw@127.0.0.1:32768"],
  "redis unreachable": ["--public", "--pubsub=redis", "--broadcast_adapter=http,redisx,redis", "--redis_url=redis://:s3cretpw@127.0.0.1:1"],
  "redis sentinel unreachable": ["--public", "--pubsub=redis", "--broadcast_adapter=http,redisx", "--redis_url=redis://:s3cretpw@mymaster", "--redis_sentinels=sent:s3cretpw2@127.0.0.1:1,127.0.0.1:2"],
  "redis bad url": ["--public", "--pubsub=redis", "--redis_url=redis://bob:s3cretpw@127.0.0.1:notaport/x/y"],
  "nats unreachable": ["--public", "--pubsub=nats", "--broker=nats", "--broadcast_adapter=http,nats", "--nats_servers=nats://bob:s3cretpw@127.0.0.1:1"],
}
for label, args in cases.items():
    for kind in ("go", "rust"):
        s = Server(kind, args + ["--log_level=debug"], wait=False, name="sec-" + kind)
        time.sleep(9 if "nats" not in label else 12)
        logs = s.logs(); alive = s.alive(); s.stop()
        hits = [l[:330] for l in logs.splitlines() if "s3cretpw" in l]
        print(f"== {label} [{kind}] alive_after_wait={alive} secret_lines={len(hits)}")
        seen = set()
        for h in hits:
            k = h[24:140]
            if k in seen: continue
            seen.add(k); print("     ", h)
        if "bad url" in label or "sentinel" in label: print("   tail:", [l[:260] for l in logs.splitlines()[-3:]])
