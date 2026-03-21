# Workshop Guide

This repository’s workshop-facing artifact is the schedule-exploration
workflow.

## What the artifact demonstrates

- controlled concurrency testing by exploring schedules instead of mutating SQL
- state-space guidance through canonical-state hashing
- retention of interesting schedules for replay and regression use
- cross-database evaluation on PostgreSQL, MySQL, and SQLite

The clearest entry point is:

```bash
python scripts/experiments/run_final_eval.py
```

## Repro modes

### Smoke

Fastest path, SQLite only, no Docker:

```bash
python scripts/experiments/run_final_eval.py --smoke --db sqlite
```

Current defaults from the implementation:

- coverage duration: 5s
- repetitions: 1
- case-study replay count: 5

### Quick

Recommended workshop/demo run:

```bash
docker compose -f docker-compose-multidb.yml up -d
python scripts/experiments/run_final_eval.py \
  --quick \
  --db postgres mysql sqlite \
  --output-dir results/schedule_pool_quick
```

Current defaults from the implementation:

- coverage duration: 15s
- repetitions: 3
- exact replay count: 10

### Paper

Longest built-in mode:

```bash
python scripts/experiments/run_final_eval.py \
  --paper \
  --db postgres mysql sqlite \
  --output-dir results/schedule_pool_paper
```

Current defaults from the implementation:

- coverage duration: 60s
- repetitions: 10
- exact replay count: 20

## Output layout

`run_final_eval.py` writes a structured result tree that includes:

- micro-coverage outputs
- case-study outputs
- determinism and replay summaries
- throughput measurements
- pooled schedule JSONL artifacts
- generated markdown and CSV summaries

## What is secondary

These workflows remain in the repository, but they are not the main workshop
artifact:

- Go/PostgreSQL YAML runner plus Elle histories
- BEC-style history analysis and retargeting
- litmus utilities
- one-off repro packs and historical benchmark scripts

They are useful supporting material, but they should not be presented as the
central contribution ahead of schedule exploration.
