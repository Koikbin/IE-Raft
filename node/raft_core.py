"""Provided Raft core: plain Raft (Figure 2 of the Raft paper) behind the IE-Raft node interface (handout section 4.2).

What this file does:  leader election by node-count majority, log replication, commit by node-count majority
(only entries of the current term), persistence helpers, restart.
What YOU add (search for "TODO(IE)"):  levels, level views, weighted quorums, Epoch settlement, demotion,
promotion, the missed-era rule, commit variants A and B, and the messages' `level` / `level_table` fields.

Rules you must keep (handout section 4.4):  every handler returns at most one message per destination,
sorted by target node id, smallest first.  Messages are plain dicts.
"""
import copy


class IERaftNode:
    def __init__(self, node_id, peers, initial_level=1, initial_view=None, commit_mode="A", prevote=False):
        self.node_id = node_id
        self.peers = sorted(peers)
        self.all_ids = sorted(self.peers + [node_id])
        # persistent
        self.current_term = 0
        self.voted_for = None
        self.log = []                       # log[i-1] is the entry with index i: {"index", "term", "command"}
        # volatile
        self.commit_index = 0
        self.last_applied = 0
        self.role = "Follower"              # "Follower" | "Candidate" | "Leader"
        self.leader_id = None
        self.state_machine = []
        self.next_index = {}
        self.match_index = {}
        self.votes_granted = {}             # voter_id -> reported level (incl. self)
        # TODO(IE): level, level_view, timeouts_in_epoch, last_leader_term, commit_mode are IE state.
        self.level = initial_level
        self.level_view = dict(initial_view or {})
        self.level_view[node_id] = self.level
        self.timeouts_in_epoch = 0
        self.last_leader_term = 0
        self.commit_mode = commit_mode
        self.prevote = prevote              # Part 3 only

    # ------------------------------------------------------------ helpers
    def _last(self):
        return (self.log[-1]["index"], self.log[-1]["term"]) if self.log else (0, 0)

    def _majority(self):
        # TODO(IE): replace by the weighted quorum Q computed from this node's level view (section 3.3).
        return len(self.all_ids) // 2 + 1

    def _ae_for(self, j):
        ni = self.next_index.get(j, len(self.log) + 1)
        prev_i = ni - 1
        prev_t = self.log[prev_i - 1]["term"] if prev_i >= 1 else 0
        return {"type": "AppendEntries", "from": self.node_id, "to": j, "term": self.current_term,
                "leader_id": self.node_id, "prev_log_index": prev_i, "prev_log_term": prev_t,
                "entries": [dict(e) for e in self.log[ni - 1:]], "leader_commit": self.commit_index}
        # TODO(IE): add "level_table": the Leader's level view (section 3.2).

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.state_machine.append(self.log[self.last_applied - 1]["command"])

    # ------------------------------------------------------------ election
    def handle_election_timeout(self):
        if self.role == "Leader":
            return []
        # TODO(IE): count the timeout, demote if needed (section 3.5).
        self.role = "Candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_granted = {self.node_id: self.level}
        self.leader_id = None
        if len(self.votes_granted) >= self._majority():
            return self._become_leader()
        li, lt = self._last()
        return [{"type": "RequestVote", "from": self.node_id, "to": j, "term": self.current_term,
                 "candidate_id": self.node_id, "last_log_index": li, "last_log_term": lt}
                for j in self.peers]

    def handle_request_vote(self, args):
        if args["term"] > self.current_term:
            self.current_term = args["term"]
            self.voted_for = None
            self.role = "Follower"
            self.leader_id = None
        granted = False
        if args["term"] >= self.current_term:
            li, lt = self._last()
            up_to_date = (args["last_log_term"], args["last_log_index"]) >= (lt, li)
            if self.voted_for in (None, args["candidate_id"]) and up_to_date:
                granted = True
                self.voted_for = args["candidate_id"]
        return {"type": "RequestVoteReply", "from": self.node_id, "to": args["candidate_id"],
                "term": self.current_term, "vote_granted": granted}
        # TODO(IE): add "level": this node's own level.

    def handle_request_vote_response(self, resp):
        if resp["term"] > self.current_term:
            self.current_term = resp["term"]
            self.voted_for = None
            self.role = "Follower"
            return []
        if self.role != "Candidate" or resp["term"] != self.current_term:
            return []
        if resp["vote_granted"]:
            self.votes_granted[resp["from"]] = self.level      # TODO(IE): use resp["level"], weighted count
            if len(self.votes_granted) >= self._majority():
                return self._become_leader()
        return []

    def _become_leader(self):
        self.role = "Leader"
        self.leader_id = self.node_id
        self.next_index = {j: len(self.log) + 1 for j in self.peers}
        self.match_index = {j: 0 for j in self.peers}
        # TODO(IE): write the voters' levels into level_view, settle the Epoch (sections 3.2, 3.6).
        return [self._ae_for(j) for j in self.peers]

    # ------------------------------------------------------------ replication
    def handle_append_entries(self, args):
        def reply(success, match=0):
            return {"type": "AppendEntriesReply", "from": self.node_id, "to": args["leader_id"],
                    "term": self.current_term, "success": success, "match_index": match}
            # TODO(IE): add "level": this node's level after settlement.
        T = args["term"]
        if T < self.current_term:
            return reply(False)
        if self.role == "Leader" and T == self.current_term:
            return reply(False)
        # TODO(IE): missed-era rule, Epoch settlement and level_view replacement go here (section 3.7).
        if T > self.current_term:
            self.current_term = T
            self.voted_for = None
        self.role = "Follower"
        self.leader_id = args["leader_id"]
        pi = args["prev_log_index"]
        if pi > 0 and (len(self.log) < pi or self.log[pi - 1]["term"] != args["prev_log_term"]):
            return reply(False)
        idx = pi
        for e in args["entries"]:
            idx += 1
            if len(self.log) >= idx:
                if self.log[idx - 1]["term"] != e["term"]:
                    del self.log[idx - 1:]
                    self.log.append(dict(e))
            else:
                self.log.append(dict(e))
        if args["leader_commit"] > self.commit_index:
            self.commit_index = min(args["leader_commit"], idx)
            self._apply()
        return reply(True, idx)

    def handle_append_entries_response(self, resp):
        if resp["term"] > self.current_term:
            self.current_term = resp["term"]
            self.voted_for = None
            self.role = "Follower"
            return []
        if self.role != "Leader" or resp["term"] != self.current_term:
            return []
        j = resp["from"]
        # TODO(IE): update level_view[j] from resp["level"].
        out = []
        if resp["success"]:
            self.match_index[j] = max(self.match_index[j], resp["match_index"])
            self.next_index[j] = self.match_index[j] + 1
            self._advance_commit()
            if self.next_index[j] <= len(self.log):
                out.append(self._ae_for(j))
        else:
            self.next_index[j] = max(1, self.next_index[j] - 1)
            out.append(self._ae_for(j))
        return out

    def _advance_commit(self):
        for N in range(len(self.log), self.commit_index, -1):
            if self.log[N - 1]["term"] != self.current_term:
                continue
            replicated = 1 + sum(1 for j in self.peers if self.match_index[j] >= N)
            if replicated >= self._majority():           # TODO(IE): commit variants A and B (section 3.4)
                self.commit_index = N
                self._apply()
                break

    def handle_heartbeat_timeout(self):
        if self.role != "Leader":
            return []
        return [self._ae_for(j) for j in self.peers]

    def handle_client_request(self, command):
        if self.role != "Leader":
            return []
        self.log.append({"index": len(self.log) + 1, "term": self.current_term, "command": command})
        return [self._ae_for(j) for j in self.peers]

    # ------------------------------------------------------------ persistence, restart, inspection
    def persistent_state(self):
        return {"current_term": self.current_term, "voted_for": self.voted_for, "log": copy.deepcopy(self.log)}

    def restart(self, persisted):
        """Container start after a crash: persistent state comes back, everything else is reset."""
        self.current_term = persisted["current_term"]
        self.voted_for = persisted["voted_for"]
        self.log = copy.deepcopy(persisted["log"])
        self.role = "Follower"
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.state_machine = []
        # TODO(IE): section 3.5 rule 2 (level back to 1, level_view only this node, counters reset).
        self.level = 1
        self.level_view = {self.node_id: 1}
        self.timeouts_in_epoch = 0
        self.last_leader_term = self.current_term

    def get_state(self):
        """Read-only snapshot used by the harness; keep these keys."""
        return {"id": self.node_id, "term": self.current_term, "role": self.role, "level": self.level,
                "view": dict(self.level_view),
                "log": [(e["index"], e["term"], e["command"]) for e in self.log],
                "commit": self.commit_index, "last_applied": self.last_applied, "voted_for": self.voted_for,
                "timeouts": self.timeouts_in_epoch, "last_leader_term": self.last_leader_term}
