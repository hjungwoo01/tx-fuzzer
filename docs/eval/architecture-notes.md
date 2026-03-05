# tx-fuzzer Architecture Notes (Evaluation Orientation)

## Trace JSON Schema

Every event in a trace is a JSON object with these fields:

| Field       | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `client_id` | int    | yes      | Logical client/process ID (0-based). Each client runs on its own DB connection |
| `sql`       | string | yes      | SQL statement to execute |
| `type`      | string | yes      | Event kind: `"begin"`, `"commit"`, `"rollback"`, `"sql"`, `"invoke"`, `"ok"` |
| `event_id`  | string | optional | Human-readable label (e.g., `"foo_select"`, `"bar_commit"`) |
| `session`   | string | optional | Session name grouping (e.g., `"foo"`, `"bar"`, `"trouble"`) |
| `delay`     | float  | optional | Pre-execution delay in seconds (injected by mutator) |
| `args`      | list   | optional | SQL bind parameters (`$1`, `$2` → `%s` in psycopg2) |

### Go Runner TraceStep (internal/runner/runner.go)

```go
type TraceStep struct {
    ClientID int    `json:"client_id"`
    SQL      string `json:"sql"`
    Type     string `json:"type"`
}
```

## Concurrent Execution Model

### Go Runner (`internal/runner/runner.go`)
- Spawns N goroutines (one per `client_id` / process).
- Each goroutine picks transaction templates from a weighted `Chooser`.
- Per-txn: `BEGIN` → execute queries → `COMMIT/ROLLBACK`.
- Supports `BarrierEvery` (cyclic barrier sync), `RandomKillPct`, `InterQueryDelayMS`.
- Writes `logs/trace_latest.json` (flat list of TraceStep) + `history.edn` (Elle format).

### Python Replay (`scripts/replay_litmus.py`)
- **True client concurrency**: one Python thread per `client_id`, each with its own DB connection.
- Groups trace events by `client_id`, assigns global step numbers.
- All threads start together via `threading.Barrier`.
- **Per-client FIFO enforced**: each thread executes its events in trace order.
- Supports `--inject-delay-at-step N --delay-seconds D` and per-event `delay` field.
- Statement timeout: 5000ms default (deadlock detection).
- Output: JSON `{"execution_history": [...], "final_state": [[k,v], ...]}`.

### Discovery Engine (`scripts/discover_update_conflict_out.py`)
- `DeterministicExecutor`: 3 sessions (foo/bar/trouble), each on own connection.
- All start via `threading.Barrier(3)`, execute their FIFO steps.
- Delays injected by `delays: Dict[event_id, seconds]` — `time.sleep(delay)` before each step.

## Delay Injection (Pre-Send Sleep)

Delays are applied **before** executing each SQL statement:
- In `replay_litmus.py`: `if delay > 0: time.sleep(delay)` (per-event `delay` field)
- In `discover_update_conflict_out.py`: `time.sleep(delays.get(eid, 0.0))` before each session step
- In `run_campaign.py` mutation: `trace[target_idx]["delay"] = delay` (persisted in trace JSON)

### Tiered Heuristic Delay System (5 tiers)

| Tier | Name                   | Weight | Delay Range   | Strategy |
|------|------------------------|--------|---------------|----------|
| 1    | `tier_rw_conflict`     | 10     | 30–150ms      | Delay reader to widen window for conflicting writer |
| 2    | `tier_wr_conflict`     | 7      | 30–120ms      | Delay read after write on same key |
| 3    | **`tier_commit_hold`** | **9**  | **40–200ms**  | **Delay COMMIT/ABORT to keep modified tuples in-flight** |
| 3b   | `tier_rw_conflict_writer` | 6   | 20–100ms      | Delay writes to let reads establish snapshots |
| 4    | `tier_ww_conflict`     | 5      | 20–80ms       | Delay conflicting writes |
| 5    | `tier_fallback`        | 1      | 10–50ms       | Any SQL event (generic low-priority) |

Selection: `select_delays_lottery()` — weighted random sampling of up to K candidates.

## Output Capture

- **Execution history**: list of dicts with `event_id`, `session`, `sql`, `duration_ms`, `rows`, `rows_affected`, `error`, `sqlstate`.
- **Timestamps**: monotonic `time.monotonic()` per event.
- **Final state**: `SELECT k, v FROM kv ORDER BY k` (or table-specific dump).
- **Errors**: captured per-statement, `sqlstate` field preserves PostgreSQL error codes (e.g., `40001`).

## Elle / Oracle Integration

### Clojure Elle (`elle/`)
- Invoked via `clj -M:run <history.edn>` from Go main or `make check`.
- Reads EDN history with `:invoke`/`:ok`/`:fail` records containing `:txn` micro-ops.
- Detects dependency cycles (serialization anomalies).

### Python Analysis (`scripts/analyze_history.py`)
- Parses `.edn` histories, builds wr/ww/rw dependency graph via NetworkX.
- Detects strongly connected components = serialization anomalies.
- Flags dirty/intermediate reads, near-miss cycles.

### Custom Oracles
- `discover_update_conflict_out.py`: `check_bug_found()` — checks mutual invisibility (both selects empty, both committed, no 40001).
- `run_campaign.py`: coverage oracle — SHA-256 hash of final `kv` state.

## Extension Points

### Adding New Workloads
- **Python workloads**: Create `workloads/<name>.py` with `setup_sql()`, `seed_trace()`, `oracle()`.
- **YAML workloads**: Add to `workloads/` with transaction templates, elle annotations.
- **Litmus tests**: Add `litmus/test_<name>.py` using `LitmusTester` (two-connection pattern).

### Adding Experiment Harness
- `scripts/experiments/run_suite.py` — new file, reads YAML config from `experiments/`.
- Integrates with existing `discover_update_conflict_out.py` patterns (DSN helpers, schema setup, trial runners).
- Does NOT modify existing CLIs — purely additive.

### Custom Invariants
- Each workload's `oracle()` function returns `OracleResult(ok, bug_type, details)`.
- Can check: final DB state invariants, execution history analysis, error code patterns.

## Key Files Map

| Purpose | File | Key Functions |
|---------|------|---------------|
| Go runner core | `internal/runner/runner.go` | `Run()`, `TraceStep` |
| Go replay | `internal/runner/replay.go` | `RunReplay()` |
| Go history | `internal/history/history.go` | `Write()`, `WriteTxn()`, `Mop` |
| Python replay | `scripts/replay_litmus.py` | `ReplayLitmus` class |
| Fuzzing campaign | `scripts/run_campaign.py` | `CampaignRunner`, mutation functions |
| SSI discovery | `scripts/discover_update_conflict_out.py` | `run_discovery_campaign()`, `build_delay_candidates()` |
| Analysis | `scripts/analyze_history.py` | dependency graph, cycle detection |
| Docker infra | `docker-compose-repro.yml` | PG 12.3 (54323), PG 12.4 (54324) |

## PostgreSQL Instances (Pinned for Evaluation)

| Version | Port  | Purpose |
|---------|-------|---------|
| 12.3    | 54323 | Vulnerable (SSI update-conflict-out bug) |
| 12.4    | 54324 | Fixed control for SSI bug |
| 16.2    | 54326 | Modern baseline for realistic workloads |

All configured with: `synchronous_commit=off`, `fsync=off`, `full_page_writes=off`, `autovacuum=off`, tmpfs data dir.
