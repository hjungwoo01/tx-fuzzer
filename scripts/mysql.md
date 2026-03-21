# MySQL helper workflows

This file documents the auxiliary MySQL scripts. These are useful, but they are
not the repository’s main schedule-exploration path.

## Quick start

Bring up the helper MySQL container:

```bash
docker compose up -d
```

Run a litmus test:

```bash
python scripts/run_litmus.py --target mysql --test lost_update
```

## Other helper paths

- `scripts/run_mixed_attack.py`: anomaly injection under background noise
- `scripts/nemesis.py`: Docker-level fault injection against a MySQL container

## Notes

- These helpers are for focused adapter or anomaly checks.
- The main evaluation workflow for the repository is
  `python scripts/experiments/run_final_eval.py`.
