# Documentation Map

Use this file to distinguish current documentation from historical notes.

## Current reference docs

- [README.md](../README.md): repository overview and quick start
- [ARCHITECTURE.md](ARCHITECTURE.md): current component map
- [multidb_evaluation.md](multidb_evaluation.md): main schedule-pool evaluation workflow
- [../WORKSHOP.md](../WORKSHOP.md): workshop/demo entry points
- [../DOCKER.md](../DOCKER.md): container setups

## Active auxiliary docs

- [../BENCHMARK.md](../BENCHMARK.md): historical BEC benchmark path
- [../litmus/README.md](../litmus/README.md): focused litmus tests
- [../scripts/mysql.md](../scripts/mysql.md): MySQL helper workflows
- [../scripts/redis.md](../scripts/redis.md): Redis helper workflows
- [repros/](repros): repro-pack notes

## Generated from the main workflow

`scripts/experiments/run_final_eval.py` currently writes:

- `docs/notes/schedule_pool_final_methodology.md`
- `docs/notes/schedule_pool_final_results.md`

## Archival material

The following directories are kept for provenance and design history. They
should not be treated as the authoritative description of the current tool:

- `docs/eval/`
- `docs/notes/` except for the two generated final-evaluation notes above
- `reports/`
- `results/`
- `runs/`

When updating the repository, either refresh those notes explicitly or mark
them as archival so they do not conflict with the current docs.
