## tx-fuzzer

This repository contains a small transaction fuzzer and runner that produces
Elle-compatible histories (`.edn`) for PostgreSQL. You write workloads in YAML,
run them with the Go runner, and then use Elle (plus a few helper scripts in
this repo) to look for transactional anomalies.

At a high level:

1. Define a workload in `workloads/*.yaml` (transactions, queries, and keys).
2. Run the workload against Postgres with `go run ./cmd/runner`.
3. Inspect the resulting `history.edn` with Elle or the helper scripts.
4. (Optional) Use the BEC-1 helpers to generate a follow-up workload that
   targets the most interesting anomalies.

---

### Requirements

- Go toolchain (for `go run ./cmd/runner`).
- A reachable PostgreSQL instance; set `DATABASE_URL` for the runner.
- Python 3.9+ with the following packages: `pyyaml`, `edn_format`, `networkx`, `matplotlib`.

Install the Python dependencies (ideally inside a virtualenv):

```bash
python -m venv .venv
. .venv/bin/activate
pip install pyyaml edn_format networkx matplotlib
```

---

### Basic workflow

1. **Start Postgres and export `DATABASE_URL`**

   Make sure your database is running and reachable:

   ```bash
   export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
   ```

2. **Pick or write a workload**

   There are example workloads under `workloads/` (e.g. `seed.yaml`,
   `write_skew.yaml`, `next.yaml`). You can copy and tweak these, or write your
   own.

3. **Run the workload and produce a history**

   ```bash
   go run ./cmd/runner -config workloads/your_workload.yaml -out history.edn
   ```

4. **Analyze the history**

   - With Elle directly (Clojure):

     ```bash
     clj -J-Djava.awt.headless=true -M:run history.edn --model snapshot-isolation
     ```

   - Or with the Python helper:

     ```bash
     python scripts/analyze_history.py history.edn
     ```

5. **Optionally generate a targeted follow‑up workload**

   You can use the BEC‑1 helpers (`scripts/analyze_bec1.py` and
   `scripts/next_workload_from_profile.py`) to generate a new workload that
   focuses on promising anomalies (see the section below on "Using the BEC‑1
   helpers").

---

### Example: testing Postgres REPEATABLE READ

`workloads/write_skew.yaml` is a workload tailored for exercising Postgres
`REPEATABLE READ` (snapshot isolation) via a classic write‑skew anomaly.

1. **Start the database container**

   ```bash
   docker start txfuzz-pg
   ```

2. **Seed the data and run the write‑skew workload**

   ```bash
   export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

   # Initialize data
   go run ./cmd/runner -config workloads/seed.yaml -out seed_history.edn

   # Run the RR/write‑skew workload
   go run ./cmd/runner -config workloads/write_skew.yaml -out history.edn
   ```

3. **Check the resulting history with Elle**

   ```bash
   clj -J-Djava.awt.headless=true -M:run history.edn --model snapshot-isolation
   ```

   `--model/--models` accepts comma‑separated values (for example
   `repeatable-read,read-committed`) and aliases like `rr`, `si`, and `rc`.
   Postgres `REPEATABLE READ` implements snapshot isolation semantics, so
   `--model snapshot-isolation` is the most accurate check for RR on Postgres.

---

### Using the BEC‑1 helpers

Once you have a history and want to automatically propose a "next" workload
that targets anomalies more directly, you can use the BEC‑1 analysis helpers.

1. **Run the BEC‑1 analysis**

   From a virtual environment with the Python dependencies installed:

   ```bash
   python scripts/analyze_bec1.py
   ```

   This produces a `bec1_target.json` file describing keys/transactions that
   look interesting from the BEC‑1/BEC‑k heuristics.

2. **Generate the next workload**

   ```bash
   python scripts/next_workload_from_profile.py
   ```

   This reads `bec1_target.json` and writes a follow‑up workload to
   `workloads/next.yaml`.

3. **Run the new workload**

   ```bash
   go run ./cmd/runner -config workloads/next.yaml -out history.edn
   ```

You can then repeat the analysis/re‑run cycle as needed.

---

### Workload Transactions

Transaction templates can now specify ordered query lists executed on a shared session:

```yaml
workload:
  transactions:
    - name: transfer
      isolation: "SERIALIZABLE"
      timeout: "2s"
      rollback_on_error: true          # default; stop immediately on the first query error
      inter_query_delay_ms: 10         # optional pacing between sequential queries
      commit_strategy: random          # commit | rollback | random (defaults to commit)
      queries:
        - sql: "UPDATE accounts SET balance = balance - $1 WHERE id = $2"
          args: [100, "$1"]
          elle: {f: ":write", key: "$2"}
        - sql: "UPDATE accounts SET balance = balance + $1 WHERE id = $3"
          args: [100, "$2"]
          elle: {f: ":write", key: "$3"}
        - sql: "SELECT balance FROM accounts WHERE id = $2"
          args: ["$2"]
          elle: {f: ":read", key: "$2", value_from_result_col: 0}
```

- Every query runs sequentially within the same `BEGIN`/`COMMIT` block, and per-query `:invoke`/`:ok`/`:fail` events share a common `txn_id`.
- Terminal `:commit`/`:rollback` events are recorded according to `commit_strategy`, making intentional aborts visible in histories.
- Legacy `steps` arrays remain supported; the runner treats each step as a single-query transaction and can mix both styles in one workload.

Optional pacing and error-handling controls (`inter_query_delay_ms`, `rollback_on_error`) help model real client behaviour and surface edge cases (for example, continuing after a recoverable error before rolling back).

---

### Inspecting histories manually

You can invoke the analyzer directly on any Elle history:

```bash
python scripts/analyze_history.py history.edn --skip-graph
```

The analyzer prints:

- Edge counts (WR/WW/RW), dependency density, and strongly connected components.
- Detected cycles and near‑miss paths (possible G2 anomalies).
- Suggestions for intensifying contention (for example, hot keys, scheduling tweaks).
- Optional PNG visualization (`dependency_graph.png`) when the graph is small enough.
- Adjacency export (`dependency_adjacency.tsv`) so you can inspect the full graph even when PNG rendering is skipped; large graphs fall back to a sparse edge list.
- Anomaly hints: ambiguous WR edges (duplicate writers), initial‑state WR edges,
  RW edges across multiple overwriters, and dirty/intermediate read flags.

---

### Troubleshooting

- **Runner fails immediately**: confirm `DATABASE_URL` is exported and reachable; use `cmd/runner`'s `--init` once to initialize schema.
- **No edges detected**: increase `scheduling.clients` or decrease the key set size in your workload.
- **Graph rendering skipped**: graphs with more than `--max-graph-nodes` (default 200) are intentionally not rendered; rerun the analyzer with a higher limit if needed.
- **Python import errors**: install the dependencies listed above and ensure you activate the virtual environment before running the scripts.

---

### Project structure

- `cmd/runner/` – Go entry point that exercises workloads against the target database and writes Elle histories.
- `internal/` – Go support packages (config parsing, workload execution, history writer).
- `scripts/analyze_history.py` – EDN history analysis and graph export.
- `scripts/analyze_bec1.py` – BEC‑1/BEC‑k history analysis to identify promising anomalies/targets.
- `scripts/next_workload_from_profile.py` – Generates a follow‑up workload (for example `workloads/next.yaml`) from a BEC‑1 profile.
- `workloads/` – Example workload definitions you can use as templates.


