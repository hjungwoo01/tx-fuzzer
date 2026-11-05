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
- `injection.py`: statement-level delay injection (`InstrumentedAdapter`),
  execution trace, strict canonical reads (`StrictAdapter` / `HarnessError`)
- `execution.py`: `execute_trial`, the single trial executor used by search,
  replay, minimization and the CLI
- `minimize.py`: delta debugging over delay points + delay shrinking
- `timeline.py`: interleaving timeline and delay-plan rendering

### `isotest/`

The workshop CLI (`python -m isotest`): `doctor`, `list`, `lab`, `explore`,
`minimize`, `show`, `replay`, `sweep`, `stress`, `selfcheck`.  It is a thin
layer over `scripts/schedule_pool/`.  It adds per-machine calibration of the
serial-baseline stagger and per-session database sandboxes (a private
PostgreSQL schema or MySQL database), and it accepts user-supplied variants
(`--variant file.py:function`).

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

### Injection modes

How a schedule's delay points are honored is explicit, and every artifact
records it (`injection_mode`):

- `step`: pause immediately before the named BEGIN, the q-th statement of
  the transaction, or COMMIT/ROLLBACK.  Delays on one worker add up.  This
  is what the event-id scheme describes, and it is the CLI default.
- `start_skew` (historical): each worker sleeps `begin + max(exec)` before
  its transaction and commit delays are ignored, so only per-worker start
  offsets are controlled.  It stays the default of the research entry points
  (`run_pool_search`, `SchedulePoolRunner`) so published results reproduce.
  Artifacts without a recorded mode load as `start_skew`.

Artifacts also record `n_workers` and `stagger_ms`, the baseline start
offset between consecutive workers.  With a stagger longer than one
transaction, the no-pause baseline is serial, so a minimized schedule's size
is the number of preemptions the bug needs.

Delay points are positional: "the q-th statement" of a *different* variant
is a different statement.  Validate a fix by sweeping or exploring that
variant itself, not only by replaying the bug's schedule (the CLI warns when
scheduled pauses are never reached).

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
