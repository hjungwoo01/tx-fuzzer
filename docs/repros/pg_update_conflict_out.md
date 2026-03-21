# Repro pack: `pg_update_conflict_out`

This is an auxiliary reproduction pack for the historical PostgreSQL 12.3 SSI
`update_conflict_out` bug.

## Current pack ID

`pg_update_conflict_out`

## Current entrypoint

```bash
python scripts/repro_pipeline.py --pack pg_update_conflict_out --mode smoke
```

## Notes

- This pack is useful for focused reproduction work.
- The repository’s main workflow is still schedule exploration via
  `scripts/experiments/run_final_eval.py`.
