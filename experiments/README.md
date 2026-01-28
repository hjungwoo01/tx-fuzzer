# Scaling Benchmark: Random vs BEC-1

Compares **Time-to-Bug** between:

- **Random**: Concurrent execution; each client repeatedly picks a transaction from the mix at random (true random order of transactions). Run until Elle detects a consistency violation or timeout.
- **BEC-1**: Exploration → analyze history → generate deterministic replay (or key-flood workload) → attack run. When strategy is `interleave`, uses a fixed sequential replay; otherwise weighted key flooding.

## Prerequisites

1. **Postgres** running; DSN used from workload YAML (generated from `workloads/templates/blind_write.yaml`). The Go runner also requires `DATABASE_URL` for its startup check (use the same value).
2. **Go** (for `./cmd/runner`).
3. **Python 3** with `pyyaml`, `edn_format`, `networkx`, `matplotlib`.
4. **Clojure** (for Elle history check): `clj -M:run <history.edn> --model <model>`.

```bash
# From repo root
pip install -r requirements.txt
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
# Optional: init schema (benchmark template has init_schema: true, so first run can init)
go run ./cmd/runner -config workloads/templates/blind_write.yaml -out /dev/null -init
```

## Run the benchmark

From the **repository root**:

```bash
python experiments/run_scaling_benchmark.py
```

- Runs over key counts `[1000, 5000, 10000, 25000, 50000]` (see `KEY_COUNTS` in the script).
- For each key count: **5 trials** of Random and **5 trials** of BEC-1 (`N_TRIALS`).

## Where to see results

- **Console**: Per key count, per strategy, you get mean ± std and timeouts, e.g.  
  `random: 45.2±12.1s (5 trials)` or `bec-1: 8.3±0.5s (5 trials, 1 timeout)`.
- **Plot**: Saved as `experiments/benchmark_results.png` — Time-to-Bug (seconds) vs Key Space Size for Random vs BEC-1 (mean ± std).

## Faster / smaller run

Edit `experiments/run_scaling_benchmark.py`:

- `KEY_COUNTS = [1000, 5000]` — fewer key counts.
- `N_TRIALS = 2` — fewer trials per strategy.
- `RANDOM_TIMEOUT_S = 30` — shorter random timeout.

Then run again:

```bash
python experiments/run_scaling_benchmark.py
```
