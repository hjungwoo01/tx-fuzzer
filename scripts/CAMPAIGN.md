# Experimental trace-evolution campaign

`scripts/run_campaign.py` is an older coverage-guided trace-evolution workflow.
It is still useful, but it is not the repository’s primary concurrency-testing
path anymore.

The current main workflow is schedule exploration via
`scripts/experiments/run_final_eval.py`.

## What this script does

- generates or mutates traces
- replays them
- hashes observed states
- evolves from traces that discover novel states

## Quick start

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
python scripts/run_campaign.py --iterations 10
```

## Validation

- install Python dependencies from `requirements.txt`
- ensure PostgreSQL is reachable
- use `workloads/fuzz.yaml` if you want a shorter, fuzz-friendly seed workload

Treat this as an experimental side path, not the headline interface.
