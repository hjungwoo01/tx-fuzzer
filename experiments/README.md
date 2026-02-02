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

The script acts as an **Isolation Level Scanner**: it tests Postgres isolation levels in order (`READ COMMITTED` → `REPEATABLE READ` → `SERIALIZABLE`). For each level it:

1. Regenerates the workload YAML with `isolation: <LEVEL>` on every transaction.
2. Runs the full scaling benchmark (all key counts, 5 trials of Random and BEC-1 per key count).
3. Records **Pass** (0 BEC-1 bugs found) or **Fail** (at least one BEC-1 bug found).

**Short-circuit:** If a level **Passes** (no anomalies found), that level is marked **SAFE** and the script stops—it does not test stronger levels (they are assumed safe).

At the end you get a summary table, for example:

```
=== Isolation Level Summary ===
READ COMMITTED: VULNERABLE (Found 50 successful BEC-1 attacks)
REPEATABLE READ: SAFE (0 successful BEC-1 attacks)
```

(SERIALIZABLE is not listed if REPEATABLE READ was already SAFE.)

- Key counts: `[1000, 5000, 10000, 25000, 50000]` (see `KEY_COUNTS` in the script).
- Per key count: **5 trials** of Random and **5 trials** of BEC-1 (`N_TRIALS`).

**Options:**

| Flag | Description |
|------|-------------|
| `--write-skew` | Use Write Skew (G2) workload for BEC-1 instead of Lost Update (G1). |
| `--chaos` | **Chaos Mode:** periodically restart the Postgres Docker container to test crash recovery. |
| `--chaos-container NAME` | Docker container name to restart in Chaos Mode (default: `postgres`). |

Example with Write Skew workload:

```bash
python experiments/run_scaling_benchmark.py --write-skew
```

---

## Chaos Mode (database crash recovery)

Chaos Mode runs a background thread that **restarts the Postgres Docker container** every few seconds while the benchmark runs. This exercises:

- **Crash recovery:** the DB goes down and comes back; the Go runner retries until the DB is available again.
- **Atomic visibility:** Elle’s analysis can detect violations (e.g. a transaction committed but its data is missing after recovery).

### How to run with Chaos Mode

1. **Postgres in Docker** with a known container name (default: `txfuzz-pg`). For example:

   ```bash
   docker run -d --name txfuzz-pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:15
   ```

2. From the **repository root**, run the scaling benchmark with Chaos Mode (this will target `txfuzz-pg` by default):

   ```bash
   python experiments/run_scaling_benchmark.py --chaos
   ```

   If your Postgres container has a different name:

   ```bash
   python experiments/run_scaling_benchmark.py --chaos --chaos-container my-postgres
   ```

3. The chaos thread restarts the container every **5 seconds**, then waits for Postgres to be ready (`pg_isready`) before the next cycle. Workload runs that hit a down DB are **retried** (see below) instead of failing immediately.

### Retry behavior when the DB is down

When Chaos Mode (or any outage) causes the Go runner to fail due to DB unavailability, the benchmark **retries** the workload for up to **60 seconds** (configurable). You can change the retry window with an environment variable:

```bash
# Retry for up to 120 seconds when the DB is unreachable
export TXFUZZER_DB_RETRY_WINDOW=120
python experiments/run_scaling_benchmark.py --chaos
```

After the retry window expires, the run is treated as a failure (same as before). Check the Analysis phase (Elle) for atomic visibility or other consistency violations in the generated history.

## Where to see results

- **Console**: Per key count, per strategy, you get mean ± std and timeouts, e.g.  
  `random: 45.2±12.1s (5 trials)` or `bec-1: 8.3±0.5s (5 trials, 1 timeout)`.
- **Isolation summary**: At the end, `=== Isolation Level Summary ===` lists each tested level as `SAFE` or `VULNERABLE (Found N successful BEC-1 attacks)`.
- **Plot**: Saved as `experiments/benchmark_results.png` — Time-to-Bug (seconds) vs Key Space Size for Random vs BEC-1 (mean ± std) for the last isolation level that was run.

## Faster / smaller run

Edit `experiments/run_scaling_benchmark.py`:

- `KEY_COUNTS = [1000, 5000]` — fewer key counts.
- `N_TRIALS = 2` — fewer trials per strategy.
- `RANDOM_TIMEOUT_S = 30` — shorter random timeout.

Then run again:

```bash
python experiments/run_scaling_benchmark.py
```

## BEC-1 analysis & Patch Recommender

You can also run the **BEC-1 analyzer** (with the Patch Recommender) directly on any Jepsen/Elle `history.edn`.

### 1. Generate a history

- **Using the scaling benchmark**: it already writes `experiments/history.edn` during exploration/attacks.
- **Using your own workload**: run the Go runner (or your harness) with Elle integration so it emits a `history.edn`.

### 2. Run the BEC-1 analyzer

From the **repository root**:

```bash
python scripts/analyze_bec1.py --history experiments/history.edn --out bec1_target.json
```

- `--history`: path to the Elle history file (defaults to `./history.edn`).
- `--out`: where to write the BEC-1 profile (defaults to `bec1_target.json`).

This will:

1. Extract transactions from the history.
2. Build the dependency graph and search for the **single best RW edge** that closes a cycle.
3. Emit a profile in `bec1_target.json` with the chosen `target_key`, `p1_name`, `p2_name`, and `strategy`.

### 3. Patch Recommender output

When BEC-1 finds a **real cycle**, the Patch Recommender analyzes the two transactions forming the cycle and prints a **red** recommendation to stderr:

- **Case A – Lost Update (G1):**  
  - Condition: cycle involves `SELECT ...` followed by `UPDATE ...` on the **same key**.  
  - Output:  
    - Red log line like:  
      `Critical G1 Vulnerability detected. PATCH: Change your SQL to SELECT ... FOR UPDATE to lock rows during the read.`  
    - Extra fields in `bec1_target.json`:
      - `"cycle_type": "G1_lost_update"`
      - `"diagnosis": "Critical G1 Lost Update cycle detected."`
      - `"patch_recommendation": "Change your SQL to \`SELECT ... FOR UPDATE\` to lock rows during the read."`

- **Case B – Write Skew (G2):**  
  - Condition: transactions read key **A** and write key **B** where A ≠ B (classic write-skew shape).  
  - Output:  
    - Red log line like:  
      `G2 Write Skew detected. PATCH: Switch isolation level to SERIALIZABLE or use predicate locking.`  
    - Extra fields in `bec1_target.json`:
      - `"cycle_type": "G2_write_skew"`
      - `"diagnosis": "G2 Write Skew cycle detected."`
      - `"patch_recommendation": "Switch isolation level to \`SERIALIZABLE\` or use predicate locking."`

If BEC-1 only has a **suspicion** (no actual cycle), it still writes `bec1_target.json` for exploration, but **no patch recommendation** is printed or embedded.
