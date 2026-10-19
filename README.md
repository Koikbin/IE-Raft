# IE-Raft starter

Course starter for the IE-Raft project (see the handout for the rules, problems and grading).

## What is here

| Path | What it is |
|---|---|
| `node/raft_core.py` | **Provided Raft core**: plain Raft behind the `IERaftNode` interface (handout section 4.2). You add the IE rules; search for `TODO(IE)`. |
| `node/server.py` | **You write this**: the HTTP service of handout section 5 (scripted mode, persistence, and live mode in Part 3). |
| `node/Dockerfile` | Builds your node from the course base image `python:3.12-slim`. Install every dependency here; there is no network at grading time. |
| `compose.yaml` | Five nodes `node0`..`node4` (container names fixed, port 8000, one volume each, mounted at `/data`) and the test harness. |
| `harness/` | The harness container: `replay.py` replays a script against the node containers, `ieharness.py` numbers messages and checks the properties of Raft Figure 3 after every operation, `live_client.py` sends commands in live mode. Do not edit; the grader uses its own copy. |
| `scripts/` | Public conformance scripts (`01_...` to `06_...`). Put your attack scripts here too (`p4_1_n5.json`, ...). |
| `check_logs.py` | Checks live-mode logs (Part 3). |
| `Makefile` | `make test`, `make selfcheck`, `make up`, `make down`. |

## What you have to write

Search for `TODO(IE)` in `node/` to find every place; `TODO(IE) [Part 3]` marks the Part 3 additions.

| Where | What | When |
|---|---|---|
| `node/raft_core.py` | the IE rules: levels, level views, weighted quorums, Epoch settlement, demotion, promotion, missed-era rule, commit variants A and B, `level` / `level_table` in messages, restart rules | Part 1 |
| `node/server.py` | the whole HTTP service: scripted mode and persistence | Part 1 |
| `node/server.py` | live mode: timers, `POST /message`, `POST /client`, JSON log lines | Part 3 |
| `node/raft_core.py`, `node/server.py` | Pre-Vote: new `PreVote` / `PreVoteReply` handlers, a change to the election timeout, routing of the two message types | Part 3 |
| `node/Dockerfile` | install `iproute2` if you use `tc netem` | Part 3 |

Everything else in `raft_core.py` is plain Raft and you should not need to change it. Do not edit `harness/` (the grader uses its own copy), keep the keys of `get_state()`, and keep the message rules below.

## Quick start

```
make test          # build, start five fresh nodes, replay every script in scripts/ against your containers
make selfcheck     # make test, and from Part 2 on also your scripts/p*.json on the reference (needs ./reference)
make down          # stop and remove containers and volumes
```

Add `--trace` to see every operation and the numbers of the messages it created:
`docker compose run --rm harness python replay.py /scripts/01_first_election.json --trace`
(run `docker compose up -d --wait node0 node1 node2 node3 node4` first).

## Rules the harness relies on

* Every handler returns **at most one message per destination, sorted by target id**. The harness numbers messages in the order you return them, so a different order desynchronises every script after it.
* `POST /event` answers `{"out": [...]}`; for a delivered `RequestVote` or `AppendEntries` the single message is the reply, addressed to the sender.
* JSON turns dictionary keys into strings. `level_table` and `initial_view` arrive with string keys; convert them.
* Write `current_term`, `voted_for` and `log` to `/data/state.json` **before** answering (temp file, `fsync`, rename). On start, if `/data/config.json` and `/data/state.json` exist, rebuild the node from them and call `restart`. Everything else comes back reset.
* `GET /state` returns `get_state()`; keep the keys that `raft_core.py` uses.
* `POST /init` replaces the node completely; the harness calls it at the start of every script.

## Script format

See handout section 6. Scripts with more than five nodes are skipped by `make test` (they are replayed on the reference only).

## Live mode (Part 3)

```
MODE=live docker compose up -d --wait node0 node1 node2 node3 node4
docker compose run --rm -T harness python live_client.py --interval 200 --duration 30
docker compose logs --no-color --no-log-prefix > all.log && python check_logs.py all.log
```
Timers are set with `HEARTBEAT_MS`, `ELECTION_MIN_MS`, `ELECTION_MAX_MS` (defaults 50, 300, 600). Set `PREVOTE=1` for Pre-Vote.
Your node must answer `POST /client` (body `{"command": ...}`) with `{"accepted": true|false, "leader": id|null}`: `accepted` is true when the node is the Leader and appended the command; otherwise it is false and `leader` is the id of the Leader the node currently knows, or `null`. `live_client.py` relies on this.
Fault injection: see handout section 5.4 (`docker kill`, `docker network disconnect`, `docker pause`, `tc netem`).
