# tx-fuzzer

tx-fuzzer is a schedule-aware concurrency testing toolkit. Its main workflow
explores controlled schedules by injecting delays at stable event IDs, keeping
the SQL and seed data fixed while searching for interesting interleavings.

Today the repository has two active layers:

- `scripts/schedule_pool/` and `scripts/experiments/run_final_eval.py`
  implement the main schedule-exploration workflow across PostgreSQL, MySQL,
  and SQLite.
- `cmd/runner/` and `internal/runner/` implement a Go/PostgreSQL runner that
  executes YAML workloads, writes Elle-compatible histories, and supports
  replay-style schedules.

The schedule-exploration path is the primary tool. The Go runner, litmus
utilities, BEC helpers, and repro packs are still useful, but they are
secondary workflows.

## Core idea

The repository is intentionally schedule-only:

- It does not mutate SQL text.
- It does not invent new schemas on the fly.
- It searches timing and ordering space by adding delays to stable event IDs.
- It hashes canonical end states to measure coverage.
- It stores schedules that discover new states or bugs so they can be replayed.

That makes the resulting artifacts easier to reason about than fully generative
transaction fuzzers: every retained schedule is a small, serializable object
that can be replayed against a fresh database state.

## Quick start

### 1. Install dependencies

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

The Python tooling is the easiest entry point. The Go runner additionally needs
a recent Go toolchain and Clojure only if you want to run Elle locally.

### 2. Run the schedule-pool smoke test

This exercises the main workflow on SQLite and does not require Docker.

```bash
make schedule-pool-smoke
```

Equivalent command:

```bash
python scripts/experiments/run_final_eval.py --smoke --db sqlite
```

### 3. Run the multi-DB quick evaluation

Bring up the evaluation databases:

```bash
docker compose -f docker-compose-multidb.yml up -d
```

Then run the main quick evaluation:

```bash
python scripts/experiments/run_final_eval.py \
  --quick \
  --db postgres mysql sqlite \
  --output-dir results/schedule_pool_quick
```

This produces coverage, case-study, determinism, throughput, and pool-analysis
artifacts under the chosen output directory.

## Using the Go runner

The Go runner is the lower-level execution path for PostgreSQL YAML workloads.
Use it when you want Elle histories, replay schedules, or direct control over
transaction templates.

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
go run ./cmd/runner -config workloads/write_skew.yaml -out history.edn -elle
```

Useful capabilities supported by the YAML/Go path:

- weighted transaction mixes
- per-transaction isolation levels and timeouts
- query-level Elle annotations
- replay schedules via `replay.enabled`
- hybrid replay plus background load

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/multidb_evaluation.md](docs/multidb_evaluation.md) for the current
workflow map.

## Repository map

- `scripts/schedule_pool/`: schedule model, event IDs, mutation, retention,
  pool sampling, replay
- `scripts/experiments/`: multi-DB workloads, runners, case studies, final
  evaluation harnesses
- `cmd/runner/`, `internal/`: Go execution engine for YAML workloads and Elle
  histories
- `workloads/`: YAML workloads for the Go runner and Python workload modules
- `docs/`: current docs plus archival evaluation notes
- `bench/known_bugs/`: benchmark suite for fixed known-anomaly cases

## Validation

Run the repository’s broad local verification sweep with:

```bash
make verify
```

Run the smaller core test suite with:

```bash
make test
```

Or run them separately:

```bash
go test ./...
python -m pytest tests/test_schedule_pool.py tests/test_multidb_canonical_hash.py
```

## Documentation guide

Current reference docs:

- [docs/README.md](docs/README.md)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/multidb_evaluation.md](docs/multidb_evaluation.md)
- [WORKSHOP.md](WORKSHOP.md)

Generated companion notes from `run_final_eval.py`:

- `docs/notes/schedule_pool_final_methodology.md`
- `docs/notes/schedule_pool_final_results.md`

Historical plans, one-off experiment notes, and archived result writeups remain
in `docs/eval/`, `docs/notes/`, `reports/`, `results/`, and `runs/`. They are
kept for provenance, but they are not the authoritative description of the
current tool.
