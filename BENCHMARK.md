# Historical benchmark: BEC vs random

This file documents the older Postgres-only benchmark path built around
`benchmark_runner.py`, the Go YAML runner, Elle histories, and BEC-style
history analysis.

It is still runnable, but it is not the main workflow of the repository anymore.
The current primary tool is the schedule-exploration path documented in
[README.md](README.md), [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), and
[docs/multidb_evaluation.md](docs/multidb_evaluation.md).

## What this benchmark does

The benchmark compares:

- repeated random execution of a diluted workload
- BEC-guided retargeting from an initial history

The core entrypoint is:

```bash
python benchmark_runner.py --mode both
```

Modes:

- `random`
- `bec1`
- `both`

## Dependencies

- PostgreSQL
- Go toolchain for `cmd/runner`
- Python dependencies from `requirements.txt`
- Clojure if you want Elle checks

## Typical usage

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
go run ./cmd/runner -config workloads/seed.yaml -out seed_history.edn -init
python benchmark_runner.py --mode both
```

## Notes

- This benchmark is centered on the older BEC analysis workflow.
- It is useful for historical comparison and regression checks.
- It should not be presented as the repository’s primary interface ahead of the
  schedule-pool workflow.
