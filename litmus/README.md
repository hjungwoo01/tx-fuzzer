# Litmus tests

This directory contains small, focused anomaly tests. They are useful for quick
sanity checks, but they are auxiliary to the main schedule-pool workflow.

## What lives here

- direct litmus scripts under `litmus/test_*.py`
- helper utilities in `scripts/litmus_utils.py`
- adapter-backed entrypoints such as `scripts/run_litmus.py`

## Typical usage

PostgreSQL example:

```bash
python litmus/test_g1_lost_update.py --isolation "READ COMMITTED"
```

Adapter-driven example:

```bash
python scripts/run_litmus.py --target mysql --test lost_update
```

## When to use litmus tests

- confirm a known anomaly or control quickly
- debug a single isolation behavior
- smoke-test a database adapter

If you want the main artifact or research workflow, use
`scripts/experiments/run_final_eval.py` instead.
