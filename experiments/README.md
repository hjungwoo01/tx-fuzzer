# Historical experiment drivers

This directory contains older experiment entrypoints that are still useful for
comparison and regression work, but they are no longer the repository’s main
public workflow.

Use these current docs first:

- [README.md](../README.md)
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- [docs/multidb_evaluation.md](../docs/multidb_evaluation.md)

Notable legacy scripts here:

- `run_scaling_benchmark.py`: Postgres-only random vs BEC benchmark
- `run_multidb.py`: older multi-DB evaluation harness
- `run_multidb_suite.py`: older suite/report path
- `run_schedule_pool_eval.py`: earlier schedule-pool evaluation harness

If you keep using one of these paths, document that choice explicitly so it is
clear you are not describing the main schedule-exploration workflow.
