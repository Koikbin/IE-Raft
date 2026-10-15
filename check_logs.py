#!/usr/bin/env python3
"""Check live-mode logs.   usage: python check_logs.py node0.log node1.log ...   (or: docker compose logs --no-color > all.log; python check_logs.py all.log)

Each line is one JSON object (lines that are not JSON are ignored; a `docker compose logs` prefix is stripped).
Checks:  Election Safety (at most one Leader per term), State Machine Safety (every index is committed and applied
with the same command on every node, and every node applies indexes in order), and levels in {1,2,3}.
Prints a timeline of Leader changes and level changes."""
import collections
import json
import sys


def read(paths):
    ev = []
    for p in paths:
        for line in open(p, errors="replace"):
            k = line.find("{")
            if k < 0:
                continue
            try:
                d = json.loads(line[k:])
            except ValueError:
                continue
            if isinstance(d, dict) and {"t", "node", "event", "term", "role", "level"} <= d.keys():
                ev.append(d)
    ev.sort(key=lambda d: d["t"])
    return ev


def main(paths):
    ev = read(paths)
    if not ev:
        print("no log lines found")
        return 2
    t0 = ev[0]["t"]
    bad = []
    leaders = collections.defaultdict(set)
    committed, applied = {}, {}
    last_applied = collections.defaultdict(int)
    print("timeline (ms since first line):")
    for d in ev:
        ts = d["t"] - t0
        if d["event"] == "start":                       # a (re)started node rebuilds its state machine from index 1
            last_applied[d["node"]] = 0
        if d["level"] not in (1, 2, 3):
            bad.append(f"level {d['level']} on node {d['node']} at {ts} ms")
        if d["event"] == "role_change" and d["role"] == "Leader":
            leaders[d["term"]].add(d["node"])
            print(f"  {ts:7d}  node {d['node']} becomes Leader of term {d['term']}")
            if len(leaders[d["term"]]) > 1:
                bad.append(f"Election Safety: term {d['term']} has leaders {sorted(leaders[d['term']])}")
        elif d["event"] == "level_change":
            print(f"  {ts:7d}  node {d['node']} level {d.get('old')} -> {d['level']}")
        elif d["event"] == "commit":
            if committed.setdefault(d["index"], d["command"]) != d["command"]:
                bad.append(f"State Machine Safety: index {d['index']} committed as {committed[d['index']]!r} and {d['command']!r}")
        elif d["event"] == "apply":
            if applied.setdefault(d["index"], d["command"]) != d["command"]:
                bad.append(f"State Machine Safety: index {d['index']} applied as {applied[d['index']]!r} and {d['command']!r}")
            if d["index"] != last_applied[d["node"]] + 1:
                bad.append(f"node {d['node']} applied index {d['index']} after {last_applied[d['node']]}")
            last_applied[d["node"]] = d["index"]
    print(f"\nterms with a Leader: {len(leaders)}   committed entries: {len(committed)}   applied entries: {len(applied)}")
    if bad:
        print("\nVIOLATIONS:")
        for b in dict.fromkeys(bad):
            print("  " + b)
        return 1
    print("\nOK: no violation found in these logs")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
