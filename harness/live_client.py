"""Send one command every --interval ms for --duration seconds to a live cluster (follows the Leader hint).
usage: python live_client.py [--interval 200] [--duration 30] [--nodes 5]"""
import json
import sys
import time
import urllib.request


def arg(name, default):
    return type(default)(sys.argv[sys.argv.index(name) + 1]) if name in sys.argv else default


def post(i, body):
    req = urllib.request.Request(f"http://node{i}:8000/client", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=1) as r:
        return json.loads(r.read())


interval, duration, n = arg("--interval", 200) / 1000, arg("--duration", 30), arg("--nodes", 5)
target, sent, acc, k = 0, 0, 0, 0
end = time.time() + duration
while time.time() < end:
    k += 1
    for _ in range(n):
        try:
            r = post(target, {"command": f"c{k}"})
        except Exception:
            target = (target + 1) % n
            continue
        if r["accepted"]:
            acc += 1
            break
        target = r["leader"] if r["leader"] is not None else (target + 1) % n
    sent += 1
    time.sleep(interval)
print(f"sent {sent} commands, {acc} accepted by a Leader")
