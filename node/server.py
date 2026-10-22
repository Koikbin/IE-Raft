"""Your node's HTTP service.  Handout section 5.1 (scripted mode), 5.2 (persistence), 5.3 (live mode, Part 3).

Standard library only is enough (http.server, json, os).  Listen on 0.0.0.0:8000.

Endpoints (scripted mode):
  POST /init    {node_id, peers, initial_level, initial_view, commit_mode, prevote}
                create the node, save the body to /data/config.json, write the initial /data/state.json
  POST /event   {"kind": "election_timeout" | "heartbeat_timeout"}
                {"kind": "client_request", "command": ...}
                {"kind": "message", "message": {...}}
                answer {"out": [...]} with the messages the node returns, sorted by "to"
  GET  /state   the node's get_state() snapshot
  GET  /health  200 when ready
"""
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

from raft_core import IERaftNode

DATA_DIR = "/data"
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
STATE_PATH = os.path.join(DATA_DIR, "state.json")

node = None


def write_atomic(path, obj):
    """Temp file, fsync, rename over the target (section 5.2)."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def make_node(cfg):
    return IERaftNode(int(cfg["node_id"]), [int(p) for p in cfg["peers"]],
                      initial_level=cfg.get("initial_level", 1),
                      initial_view={int(k): v for k, v in (cfg.get("initial_view") or {}).items()},
                      commit_mode=cfg.get("commit_mode", "A"),
                      prevote=bool(cfg.get("prevote", False)))


def load_after_crash():
    """Container start: if config and state exist, rebuild the node and apply the restart rules."""
    global node
    if os.path.exists(CONFIG_PATH) and os.path.exists(STATE_PATH):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        with open(STATE_PATH) as f:
            state = json.load(f)
        node = make_node(cfg)
        node.restart(state)


def handle_event(ev):
    kind = ev["kind"]
    if kind == "election_timeout":
        out = node.handle_election_timeout()
    elif kind == "heartbeat_timeout":
        out = node.handle_heartbeat_timeout()
    elif kind == "client_request":
        out = node.handle_client_request(ev["command"])
    elif kind == "message":
        m = ev["message"]
        t = m["type"]
        if t == "RequestVote":
            out = [node.handle_request_vote(m)]
        elif t == "RequestVoteReply":
            out = node.handle_request_vote_response(m)
        elif t == "AppendEntries":
            out = [node.handle_append_entries(m)]
        elif t == "AppendEntriesReply":
            out = node.handle_append_entries_response(m)
        else:
            out = []
    else:
        raise ValueError(f"unknown event kind {kind!r}")
    # persist before answering: the answer may depend on the new term / vote / log
    write_atomic(STATE_PATH, node.persistent_state())
    return sorted(out, key=lambda m: m["to"])


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"ok": True})
        elif self.path == "/state":
            if node is None:
                self._send(409, {"error": "not initialized"})
            else:
                self._send(200, node.get_state())
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        global node
        if self.path == "/init":
            cfg = self._body()
            node = make_node(cfg)
            os.makedirs(DATA_DIR, exist_ok=True)
            write_atomic(CONFIG_PATH, cfg)
            write_atomic(STATE_PATH, node.persistent_state())
            self._send(200, {"ok": True})
        elif self.path == "/event":
            if node is None:
                self._send(409, {"error": "not initialized"})
            else:
                self._send(200, {"out": handle_event(self._body())})
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *args):
        pass


def main():
    # TODO(IE) [Part 3]: live mode (MODE=live): timers, POST /message, POST /client, JSON log lines (section 5.3).
    # TODO(IE) [Part 3]: route PreVote / PreVoteReply messages to your node's Pre-Vote handlers.
    load_after_crash()
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()


if __name__ == "__main__":
    main()
