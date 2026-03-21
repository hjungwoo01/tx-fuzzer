# Multi-DB schedule-pool evaluation

This is the current reference guide for the repository’s main workflow.

## Scope

The schedule-pool evaluation path measures:

- canonical-state coverage over time
- time to first bug on case studies
- replay determinism of retained schedules
- throughput cost of schedule exploration

It is schedule-only: SQL and seed data stay fixed, and the search happens by
injecting delays at stable event IDs.

## Primary entrypoint

The preferred command is:

```bash
python scripts/experiments/run_final_eval.py
```

Built-in modes:

| Mode | Flag | Current defaults |
|------|------|------------------|
| Smoke | `--smoke` | 5s coverage, 1 rep, 5 replays |
| Quick | `--quick` | 15s coverage, 3 reps, 10 replays |
| Paper | `--paper` | 60s coverage, 10 reps, 20 replays |

## Recommended runs

### SQLite smoke

```bash
python scripts/experiments/run_final_eval.py --smoke --db sqlite
```

### Full quick run

```bash
docker compose -f docker-compose-multidb.yml up -d
python scripts/experiments/run_final_eval.py \
  --quick \
  --db postgres mysql sqlite \
  --output-dir results/schedule_pool_quick
```

## Main implementation files

- `scripts/experiments/run_final_eval.py`: main orchestrator
- `scripts/experiments/micro_workloads.py`: cross-DB micro workloads
- `scripts/experiments/micro_runner.py`: baseline stress/random/heuristic runs
- `scripts/experiments/case_studies.py`: case-study workloads
- `scripts/schedule_pool/runner.py`: pool-based search and replay
- `scripts/schedule_pool/pool.py`: weighted schedule retention and sampling
- `scripts/schedule_pool/mutator.py`: mutation operators
- `scripts/schedule_pool/retention.py`: admission policy

## Outputs

The final evaluation writes a structured result directory that includes:

- coverage CSV and plots
- case-study summaries
- determinism summaries
- throughput measurements
- retained schedule JSONL artifacts
- generated markdown and CSV reports

## Related but older entrypoints

These still exist, but they are not the preferred public workflow:

- `scripts/experiments/run_schedule_pool_eval.py`
- `scripts/experiments/run_multidb.py`
- `scripts/experiments/run_multidb_suite.py`

Use them only when you specifically need their older report layouts or
compatibility behavior.
