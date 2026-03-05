# Running the Coverage-Guided Evolutionary Fuzzing Campaign

`run_campaign.py` runs a coverage-guided evolutionary fuzzing loop: generate/mutate traces, replay them, hash outputs, and evolve from traces that discover new states.

## Prerequisites

- **Go** – used to generate the baseline trace
- **Python 3** – with dependencies from the project root (`pip install -r requirements.txt`)
- **Postgres** – the Go runner only supports Postgres for trace generation

Replay can target Postgres or MySQL; the initial seed trace is always produced by the Go runner against Postgres.

## Quick start

From the **project root** (`tx-fuzzer/`):

```bash
# 1. Set DATABASE_URL (required by the Go runner for trace generation)
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

# 2. Run a short campaign (e.g. 10 iterations)
python scripts/run_campaign.py --iterations 10
```

## Starting Postgres

If you don’t have Postgres running:

```bash
docker run -d --name postgres-txfuzzer \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  postgres:15-alpine
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--iterations N` | 1000 | Number of mutation/replay iterations (0 = use with `--forever`) |
| `--forever` | off | Run until interrupted |
| `--target` | postgres | Replay backend: `postgres`, `mysql` |
| `--dsn` | (from run_litmus) | Override connection string for replay |
| `--isolation` | READ COMMITTED | Isolation level for replay |
| `--config` | workloads/example.yaml | Workload config for seed trace generation. Use **workloads/fuzz.yaml** for shorter, read-heavy transactions (fewer deadlocks, more SUCCESS states). |
| `--corpus` | corpus/ | Directory for discovered states and population (resume from here if present) |
| `--seed` | (random) | Random seed for reproducibility |

## Examples

```bash
# Reproducible run with 50 iterations
python scripts/run_campaign.py --iterations 50 --seed 42

# Fuzz-friendly workload (1–3 ops per txn, read-heavy) to get SUCCESS states
python scripts/run_campaign.py --iterations 50 --config workloads/fuzz.yaml

# Replay against MySQL (start MySQL first, e.g. docker compose up -d)
python scripts/run_campaign.py --iterations 20 --target mysql

# Run until Ctrl+C, using a custom corpus directory
python scripts/run_campaign.py --forever --corpus ./my_corpus
```

## Output

- **Console:** Progress line `Iteration: N | Unique States Found: M`. When a new state is found: `Found New State #M! (Type: …) -> state_<hash>.json`.
- **Corpus:** Each run is saved in its own folder under the corpus directory: `corpus/run_<timestamp>/` (e.g. `corpus/run_2026-02-14T12-30-45Z/`). Inside that folder, each new state is written to `state_<hash>.json` (full trace + metadata) and `states_index.jsonl` indexes them for that run.
- **Evolution:** The campaign keeps an in-memory population of traces that discovered new states and chooses the next parent from that population (or the base trace if the population is empty). On restart, all existing `run_*/` folders and any legacy `state_*.json` in the corpus root are scanned and loaded into the population so runs can resume and continue evolving.
