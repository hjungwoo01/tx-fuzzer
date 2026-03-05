# Multi-DB Evaluation Plan

Runbook: see `docs/eval/multidb.md` for full execution and testing instructions.

## What Exists Today (Repo Recon)

### Runner / Replay entrypoints
- Python replay runner: `scripts/replay_litmus.py`
  - Replays JSON traces with per-client FIFO preserved via per-client queues.
  - Supports optional delay injection before selected replay step.
  - Has `replay` and `scheduled` modes.
- Go runner entrypoint: `cmd/runner/main.go` (binary `runner`)
  - Core scheduling logic in `internal/runner/runner.go`, `internal/runner/replay.go`, `internal/runner/hybrid.go`.
- Campaign driver: `scripts/run_campaign.py`
  - Mutation + replay loop, state hashing, corpus retention.

### Experiment harness
- PostgreSQL suite harness: `scripts/experiments/run_suite.py`
  - Runs workload×strategy matrices from YAML (`experiments/postgres_quick.yaml`, `experiments/postgres_long.yaml`).
  - Outputs `summary.jsonl`, `summary.csv`, `summary.md`, and bug artifacts.

### Trace/event schema and schedule properties
- Replay consumes JSON events with fields including:
  - `client_id` / `client`
  - `event_id`
  - `type` (`begin`, `commit`, `rollback`, `sql`, or `invoke`)
  - `sql`, optional `args`, optional `delay`
- Existing replay groups events per client, preserving per-client order.
- Delays are injected before statement execution.

### Oracle and canonical hashing
- Workloads live under `workloads/*.py` with common interface:
  - `setup_sql()`, `seed_trace(...)`, `run_trial(...)`, `oracle(...)`, optional `canonical_state(...)`
- Current suite hash path:
  - `scripts/experiments/run_suite.py` uses `canonical_state_hash(...)`
  - prefers workload `canonical_state(...)` when present.

## Planned Changes

1. Add a dedicated multi-DB experiment framework under `scripts/experiments/`:
   - shared DB adapter interface (Postgres/MySQL/SQLite)
   - cross-DB micro workload library with common logical semantics and canonical state hashing
   - stress baseline runner + deterministic scheduled modes (random delays / heuristic)
   - coverage-over-time collection and plotting
   - case-study runners (Postgres/MySQL/SQLite)
   - overhead benchmark runner
   - report generation (`summary.md`, `report.md`, CSV/JSONL outputs)

2. Extend replay CLI compatibility:
   - add `--db {postgres|mysql|sqlite}` and `--sqlite-path` in `scripts/replay_litmus.py`
   - retain backward compatibility with existing `--target`.

3. Extend adapter factory:
   - add SQLite adapter support in existing adapter/factory path used by replay and scripts.

4. Add multi-DB Docker orchestration:
   - `docker-compose-multidb.yml` with pinned Postgres/MySQL versions and healthchecks.

5. Add one-command entry scripts:
   - `scripts/repro_all.sh`
   - `scripts/experiments/run_multidb_quick.sh`

6. Add tests:
   - unit-ish checks for canonical state hashing / normalization for micro workloads.
