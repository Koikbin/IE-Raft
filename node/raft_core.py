"""Provided Raft core: plain Raft (Figure 2 of the Raft paper) behind the IE-Raft node interface (handout section 4.2).

What this file does:  leader election by node-count majority, log replication, commit by node-count majority
(only entries of the current term), persistence helpers, restart.
What YOU add (search for "TODO(IE)"):  levels, level views, weighted quorums, Epoch settlement, demotion,
promotion, the missed-era rule, commit variants A and B, and the messages' `level` / `level_table` fields.
Part 3 additions are marked "TODO(IE) [Part 3]": Pre-Vote (new handlers and a change to the election timeout).

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
        # IE state (section 3.1); JSON turns view keys into strings, so convert them back to ints
        self.level = initial_level
        self.level_view = {int(k): v for k, v in (initial_view or {}).items()}
        self.level_view[node_id] = self.level
        self.timeouts_in_epoch = 0
        self.last_leader_term = 0
        self.commit_mode = commit_mode
        self.prevote = prevote              # Part 3 only
        # TODO(IE) [Part 3]: Pre-Vote needs more state here (for example the pre-vote term and the pre-votes collected).

    # ------------------------------------------------------------ helpers
    def _last(self):
        return (self.log[-1]["index"], self.log[-1]["term"]) if self.log else (0, 0)

    def _set_level(self, level):
        """level_view[self] always equals level (section 3.1)."""
        self.level = level
        self.level_view[self.node_id] = level

    def _weight(self, ids):
        """Total weight of ids under this node's view; missing entries count as 1."""
        return sum(self.level_view.get(j, 1) for j in ids)

    def _quorum(self):
        """Q(i) = floor(W(i) / 2) + 1, W over all n nodes (section 3.3)."""
        return self._weight(self.all_ids) // 2 + 1

    def _ae_for(self, j):
        ni = self.next_index.get(j, len(self.log) + 1)
        prev_i = ni - 1
        prev_t = self.log[prev_i - 1]["term"] if prev_i >= 1 else 0
        return {"type": "AppendEntries", "from": self.node_id, "to": j, "term": self.current_term,
                "leader_id": self.node_id, "prev_log_index": prev_i, "prev_log_term": prev_t,
                "entries": [dict(e) for e in self.log[ni - 1:]], "leader_commit": self.commit_index,
                "level_table": dict(self.level_view)}

    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.state_machine.append(self.log[self.last_applied - 1]["command"])

    def _settle(self, T, voted_for_leader):
        """Observe the Epoch of term T (section 3.6). Callers only call this when T > last_leader_term."""
        if T > self.last_leader_term and voted_for_leader and self.timeouts_in_epoch <= 1:
            self._set_level(min(3, self.level + 1))
        self.timeouts_in_epoch = 0
        self.last_leader_term = T

    # ------------------------------------------------------------ election
    def handle_election_timeout(self):
        if self.role == "Leader":
            return []
        # section 3.5 rule 1: the first timeout in an Epoch is free, later ones demote, before becoming Candidate
        self.timeouts_in_epoch += 1
        if self.timeouts_in_epoch >= 2:
            self._set_level(max(1, self.level - 1))
        # TODO(IE) [Part 3]: if self.prevote, send PreVote messages instead of starting the election at once.
        self.role = "Candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_granted = {self.node_id: self.level}
        self.leader_id = None
        if sum(self.votes_granted.values()) >= self._quorum():
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
        # the reply carries the voter's own level; level_view is not touched (section 3.2 rule 1)
        return {"type": "RequestVoteReply", "from": self.node_id, "to": args["candidate_id"],
                "term": self.current_term, "vote_granted": granted, "level": self.level}

    def handle_request_vote_response(self, resp):
        if resp["term"] > self.current_term:
            self.current_term = resp["term"]
            self.voted_for = None
            self.role = "Follower"
            return []
        if self.role != "Candidate" or resp["term"] != self.current_term:
            return []
        if resp["vote_granted"]:
            self.votes_granted[resp["from"]] = resp["level"]
            if sum(self.votes_granted.values()) >= self._quorum():
                return self._become_leader()
        return []

    # TODO(IE) [Part 3]: add handle_prevote(args) and handle_prevote_response(resp) (PreVote / PreVoteReply).
    #   Your server.py must route these two message types to them.

    def _become_leader(self):
        self.role = "Leader"
        self.leader_id = self.node_id
        self.next_index = {j: len(self.log) + 1 for j in self.peers}
        self.match_index = {j: 0 for j in self.peers}
        # section 3.2 rule 2: the voters' reported levels go into the view, other entries keep their values
        for v, lv in self.votes_granted.items():
            if v != self.node_id:
                self.level_view[v] = lv
        # section 3.6: becoming Leader observes the new Epoch; its own vote counts as voting for itself
        if self.current_term > self.last_leader_term:
            self._settle(self.current_term, True)
        return [self._ae_for(j) for j in self.peers]

    # ------------------------------------------------------------ replication
    def handle_append_entries(self, args):
        def reply(success, match=0):
            return {"type": "AppendEntriesReply", "from": self.node_id, "to": args["leader_id"],
                    "term": self.current_term, "success": success, "match_index": match, "level": self.level}
        T = args["term"]
        # 1. stale term: reject, no settlement
        if T < self.current_term:
            return reply(False)
        # a different Leader in a term that already has one (this node, or the Leader it follows): reject
        if T == self.current_term and (self.role == "Leader" or
                                       (self.leader_id is not None and self.leader_id != args["leader_id"])):
            return reply(False)
        # 2. missed-era rule, using the gap before currentTerm is updated
        if T - self.current_term >= 2:
            self._set_level(1)
        # 3. did this node vote for this Leader in term T? then update currentTerm
        voted_for_leader = (T == self.current_term and self.voted_for == args["leader_id"])
        if T > self.current_term:
            self.current_term = T
            self.voted_for = None
        # 4. Candidate or Follower becomes (stays) Follower
        self.role = "Follower"
        self.leader_id = args["leader_id"]
        # 5. settle the Epoch
        if T > self.last_leader_term:
            self._settle(T, voted_for_leader)
        # 6. replace the view with the Leader's table, then the own entry
        self.level_view = {int(k): v for k, v in args["level_table"].items()}
        self.level_view[self.node_id] = self.level
        # 7. Raft log consistency check and append
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
        # 8. reply with the level after settlement
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
        self.level_view[j] = resp["level"]              # section 3.2 rule 4
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
            holders = [self.node_id] + [j for j in self.peers if self.match_index[j] >= N]
            if self.commit_mode == "B":                  # variant B: majority of nodes
                ok = len(holders) >= len(self.all_ids) // 2 + 1
            else:                                        # variant A: weighted, under the Leader's view
                ok = self._weight(holders) >= self._quorum()
            if ok:
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
        self.next_index = {}
        self.match_index = {}
        self.votes_granted = {}
        # section 3.5 rule 2: level back to 1, level_view only this node, last_leader_term = persisted term
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
