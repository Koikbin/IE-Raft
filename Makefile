# IE-Raft starter.  Requires Docker with the compose plugin.
SCRIPTS ?= scripts
export SCRIPTS_DIR := $(abspath $(SCRIPTS))
export COMPOSE_PROJECT_NAME := ieraft
NODES := node0 node1 node2 node3 node4

.PHONY: build up test selfcheck attack down reference
build:
	docker compose build $(NODES) harness

up: build
	docker compose up -d --wait $(NODES)

# Replay every script in $(SCRIPTS) (default: scripts/) against your containers.
test: build
	@docker compose down -v --remove-orphans >/dev/null 2>&1; \
	docker compose up -d --wait $(NODES) >/dev/null 2>&1 || { echo "the cluster did not start: run 'docker compose up' to see why"; exit 1; }; \
	docker compose run --rm -T harness python replay.py /scripts; rc=$$?; \
	docker compose down -v --remove-orphans >/dev/null 2>&1; exit $$rc

# Public conformance scripts on your containers, and (from Part 2 on) your attack scripts on the reference.
selfcheck:
	@$(MAKE) --no-print-directory test SCRIPTS=scripts
	@if ls scripts/p*.json >/dev/null 2>&1; then \
	   if [ -d reference ]; then $(MAKE) --no-print-directory attack; \
	   else echo "scripts/p*.json found but reference/ is missing: it is released after the late-submission window of Part 1"; fi; fi

# Build the reference image from ./reference (released after Part 1) and check your attack scripts scripts/p*.json on it.
reference:
	docker build -q -t ieraft-ref reference/replay >/dev/null

attack: reference
	@docker run --rm -v $(abspath scripts):/scripts:ro ieraft-ref python replay_ref.py /scripts --pattern 'p*.json'

down:
	docker compose down -v --remove-orphans
