# Repro pack: `mysql_rr_phantom`

This is an auxiliary reproduction pack for the MySQL phantom/non-existent-row
case used by the repro pipeline.

## Current pack ID

`mysql_rr_phantom`

## Current entrypoint

```bash
python scripts/repro_pipeline.py --pack mysql_rr_phantom --mode smoke
```

## Notes

- Historical aliases such as `mysql_rr_nonexistent_row` map to this pack.
- This is a focused repro path, not the main repository workflow.
