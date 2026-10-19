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

Reminders:
  * JSON turns dict keys into strings: level_table / initial_view arrive with string keys, convert them.
  * Persist current_term, voted_for and log BEFORE you answer (write a temp file, fsync, rename over state.json).
  * On start, if /data/config.json and /data/state.json exist, rebuild the node from them and call node.restart(state).
"""
from raft_core import IERaftNode   # noqa: F401  (your IE rules go into raft_core.py)


def main():
    # TODO(IE): scripted mode: /init, /event, /state, /health (handout section 5.1).
    # TODO(IE): persistence: /data/config.json and /data/state.json, written before every answer (section 5.2).
    # TODO(IE) [Part 3]: live mode (MODE=live): timers, POST /message, POST /client, JSON log lines (section 5.3).
    # TODO(IE) [Part 3]: route PreVote / PreVoteReply messages to your node's Pre-Vote handlers.
    raise NotImplementedError("write the HTTP service (handout section 5.1)")


if __name__ == "__main__":
    main()
