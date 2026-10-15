"""Run scripts against the node containers.
usage: python replay.py <script.json | directory> ... [--trace]
Exit status 0 = every script's expect block is satisfied.  Each script starts from a fresh /init."""
import glob
import json
import os
import sys

from ieharness import Cluster, Docker, HttpNode, ScriptError, check_expect

MAX_NODES = 5


def run_one(path, docker, trace):
    sc = json.load(open(path))
    name = os.path.basename(path)
    n = len(sc["config"]["levels"])
    if n > MAX_NODES:
        print(f"SKIP  {name}: needs {n} nodes, the compose file has {MAX_NODES} (such scripts are replayed on the reference only)")
        return None
    nodes = {i: HttpNode(i, f"node{i}:8000", docker) for i in range(n)}
    for i in range(MAX_NODES):                      # bring back anything a previous script killed
        try:
            docker.start(f"node{i}")
        except Exception:
            pass
    for nd in nodes.values():
        nd.wait_ready()
    cl = Cluster(nodes, sc["config"], print if trace else None)
    try:
        cl.init_nodes()
        res = cl.run(sc["ops"])
    except ScriptError as e:
        print(f"FAIL  {name}: script error: {e}")
        return False
    except Exception as e:                          # a node answered with an error or did not answer
        print(f"FAIL  {name}: {type(e).__name__}: {e}")
        return False
    ok, why = check_expect(sc.get("expect", {}), res)
    print(f"{'PASS' if ok else 'FAIL'}  {name}  violations={sorted({v.split(':')[0] for v in res['violations']}) or '-'}",
          flush=True)
    for w in why:
        print("      " + w)
    return ok


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    paths = []
    for a in args:
        paths += sorted(glob.glob(os.path.join(a, "*.json"))) if os.path.isdir(a) else [a]
    docker = Docker()
    results = [run_one(p, docker, "--trace" in sys.argv) for p in paths]
    ran = [r for r in results if r is not None]
    print(f"{sum(ran)}/{len(ran)} scripts passed" + (f", {len(results) - len(ran)} skipped" if len(ran) < len(results) else ""))
    return 0 if all(ran) else 1


if __name__ == "__main__":
    sys.exit(main())
