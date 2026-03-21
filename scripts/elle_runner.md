# Elle-style append runner

`scripts/elle_runner.py` is a Postgres-only helper that drives an append-only
JSONB workload and optionally combines it with the Docker fault injector in
`scripts/nemesis.py`.

It is an auxiliary tool, not the main schedule-pool workflow.

## What it is for

- append-only visibility experiments
- fault-injection demos
- checking whether committed IDs are missing after chaos events

## Example

```bash
python scripts/elle_runner.py \
  --threads 16 \
  --key 1 \
  --isolation "READ COMMITTED" \
  --duration 60
```

Use this when you specifically want the append-only workload under faults. For
the main controlled-concurrency workflow, use `run_final_eval.py`.
