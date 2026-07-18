# Pre-saved ScheduleArtifacts

These artifacts demonstrate known isolation anomalies found by isotest.
Each file is a JSON-serialized `ScheduleArtifact` that can be replayed
directly against the corresponding database.

## Artifacts

| File | DB | Workload | Anomaly |
|------|----|----------|---------|
| `bank_overdraft_postgres.json` | PostgreSQL | bank_overdraft | TOCTOU overdraft (balance goes negative) |
| `job_queue_double_claim_postgres.json` | PostgreSQL | job_queue_claim | Double claim (two workers claim same job) |
| `lost_append_mysql.json` | MySQL | list_append | Lost update (appended token missing) |

## Replay

```bash
# Start the target database
docker compose -f docker-compose-multidb.yml up -d

# Replay an artifact
python scripts/run_demo.py replay examples/repros/bank_overdraft_postgres.json
python scripts/run_demo.py replay examples/repros/job_queue_double_claim_postgres.json
python scripts/run_demo.py replay examples/repros/lost_append_mysql.json
```

The replay command executes the exact schedule 20 times (configurable via
`--n-replays`) and reports:
- **hash_match_rate** — fraction of replays producing the same FinalStateHash
- **bug_replay_rate** — fraction of replays where the Oracle detected a bug
- **unique_hashes** — number of distinct end-states observed

## Generate your own

```bash
python scripts/run_demo.py explore --db postgres --workload bank_overdraft --duration 30
```
