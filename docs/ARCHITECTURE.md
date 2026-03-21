# Architecture

tx-fuzzer currently has one primary story and one lower-level execution layer.

## Primary workflow: schedule exploration

The main workflow lives in `scripts/schedule_pool/` and
`scripts/experiments/run_final_eval.py`.

At a high level:

1. Choose a workload with fixed SQL and seed data.
2. Represent a candidate schedule as ordered `{event, delay_ms}` entries.
3. Execute the workload on a fresh database state with those delays applied.
4. Hash a canonical logical end state.
5. Retain schedules that discover a new state, a new bug, or a better replay
   profile.
6. Replay retained schedules exactly to measure determinism.

The point is to explore interleavings while keeping the workload itself stable.

## Main Python components

### `scripts/experiments/`

- `db_adapter.py`: normalized adapter layer for PostgreSQL, MySQL, and SQLite
- `micro_workloads.py`: cross-DB workloads with buggy and fixed variants,
  canonical-state functions, and oracles
- `micro_runner.py`: stress/random/heuristic baselines
- `case_studies.py`: case-study workloads
- `run_final_eval.py`: main end-to-end evaluation harness
- `run_schedule_pool_eval.py`: older schedule-pool evaluation harness
- `run_multidb.py` and `run_multidb_suite.py`: older multi-DB evaluation paths

### `scripts/schedule_pool/`

- `schedule.py`: schedule and schedule-record data model
- `event_id.py`: stable event identifiers
- `base_strategy.py`: fresh-seed schedule generators
- `mutator.py`: schedule mutation operators and caps
- `pool.py`: retention store and weighted sampling
- `retention.py`: admission policy
- `runner.py`: pool-based search and exact replay

## Schedule model

The schedule pool treats a schedule as a persistent artifact:

```text
Schedule = [{event: stable_event_id, delay_ms: non_negative_float}, ...]
```

Important properties:

- canonical ordering by event ID
- deduplication by event
- JSON/JSONL serialization
- replay through exact delay maps

This is the repository’s central abstraction for controlled concurrency
testing.

## Lower-level execution layer: Go runner

The Go path exists for PostgreSQL YAML workloads and Elle histories.

Key files:

- `cmd/runner/main.go`
- `internal/config/config.go`
- `internal/runner/runner.go`
- `internal/runner/replay.go`
- `internal/runner/hybrid.go`
- `internal/history/history.go`

This layer supports:

- YAML transaction templates
- query-level Elle annotations
- normal randomized concurrent execution
- explicit replay schedules
- hybrid replay plus background clients
- Elle-compatible history output

## Legacy and supporting paths

These remain useful, but they are not the main interface:

- BEC analysis and history retargeting scripts
- litmus tests
- repro pipeline packs
- historical evaluation scripts under `scripts/experiments/`

Keep them accurate when touched, but do not let them redefine the repository’s
primary identity.

## Extension points

### Add a new schedule-pool workload

1. Add setup, buggy, fixed, oracle, and canonical-state logic in
   `scripts/experiments/micro_workloads.py` or an adjacent module.
2. Define stable event IDs if the default workload template is not sufficient.
3. Add tests for canonical hashing and schedule behavior.
4. Update the main docs if the new workload changes the public story.

### Add a new Go/YAML workload

1. Add the YAML config under `workloads/`.
2. Make sure Elle metadata is present where history analysis needs it.
3. Validate with `go run ./cmd/runner ...` and, if relevant, Elle.

## Documentation rule

Current docs live in [README.md](../README.md),
[multidb_evaluation.md](multidb_evaluation.md), and this file. Historical notes
should be marked as archival when they stop matching the implementation.
