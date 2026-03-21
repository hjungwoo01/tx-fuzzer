# Redis helper workflows

The Redis scripts are auxiliary utilities for focused experiments. They are not
part of the repository’s main schedule-pool evaluation path.

## Quick start

Bring up the helper Redis container:

```bash
docker compose up -d
```

Run a litmus-style check:

```bash
python scripts/run_litmus.py --target redis --test atomicity
```

## Other helper paths

- `scripts/run_mixed_attack.py`: anomaly injection under background noise
- `scripts/nemesis.py`: Docker-level fault injection against a Redis container

Use these scripts when you want targeted adapter experiments, not when you want
the main controlled schedule-exploration workflow.
