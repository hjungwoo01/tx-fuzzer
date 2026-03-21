# Archival note: schedule-pool validation snapshot

Validation should now be taken from the live test and smoke commands, not from
this historical note.

Current validation commands:

```bash
make test
python scripts/experiments/run_final_eval.py --smoke --db sqlite
```
