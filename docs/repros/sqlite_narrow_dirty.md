# Repro pack: `sqlite_narrow_dirty`

This is an auxiliary reproduction pack for the narrow dirty-read case used in
the repro pipeline.

## Current pack ID

`sqlite_narrow_dirty`

## Current entrypoint

```bash
python scripts/repro_pipeline.py --pack sqlite_narrow_dirty --mode smoke
```

## Notes

- This pack is for focused reproduction and regression work.
- The primary repository workflow remains schedule exploration.
