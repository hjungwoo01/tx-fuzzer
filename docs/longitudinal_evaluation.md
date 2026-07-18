# Longitudinal State-Coverage Evaluation Guide

This document provides step-by-step instructions for running 24-hour
state-space discovery benchmarks using IsoTest's schedule exploration
framework. It targets the `smallbank` and `multikey_transfer` workloads
and is intended for reviewers or researchers replicating our longitudinal
coverage results.

---

## 1. Overview & Goal

Traditional stress testing (concurrent transactions with no controlled
scheduling) discovers a subset of reachable states quickly but flatlines
early: after the first few minutes, no new canonical states are observed
regardless of how long the test runs. The state space appears exhausted
because unguided concurrency revisits the same interleavings.

IsoTest's explicit schedule control—delay injection at stable Event IDs
combined with pool-based retention of novel interleavings—continues to
expand the reachable state space over extended periods. The coverage curve
eventually asymptotes, but at a significantly higher plateau than stress
testing achieves.

The longitudinal evaluation measures this asymptotic behavior by running
each scheduler for an extended duration (up to 24 hours) and recording a
time-series of cumulative unique `FinalStateHash` values. The resulting
CSV is suitable for plotting coverage curves (X = elapsed time, Y =
cumulative unique states) that demonstrate the advantage of controlled
schedule exploration.

**Primary target workloads:**

| Workload | Schema | Oracle Invariant |
|----------|--------|------------------|
| `smallbank` | 2 tables (savings, checking), 10 customers | Conservation: total balance = 200,000; non-negative balances |
| `multikey_transfer` | 1 table (accounts), 20 rows | Conservation: total balance = 20,000 |

Both workloads exhibit TOCTOU (Time-of-Check-to-Time-of-Use) race
conditions in their buggy variants, making them ideal targets for
measuring how effectively each scheduler discovers distinct anomalous
states over time.

---

## 2. Prerequisites for Long-Running Tests

### 2.1 Terminal Persistence

A 24-hour run will outlive any SSH session or laptop sleep cycle. Always
execute long-running evaluations inside a terminal multiplexer.

**Using tmux:**

```bash
# Start a new named session
tmux new-session -s isotest-longitudinal

# Detach at any time: Ctrl-b, then d
# Re-attach later:
tmux attach-session -t isotest-longitudinal
```

**Using screen:**

```bash
screen -S isotest-longitudinal

# Detach: Ctrl-a, then d
# Re-attach:
screen -r isotest-longitudinal
```

### 2.2 Docker Environment Setup

The multi-database evaluation requires PostgreSQL and MySQL instances
provided via Docker Compose.

```bash
# Build the IsoTest artifact image
make build

# Start the multi-DB stack (PostgreSQL 12.3, 12.4, 16.2; MySQL 5.6, 5.7, 8.0)
docker compose -f docker-compose-multidb.yml up -d

# Verify all services are healthy
docker compose -f docker-compose-multidb.yml ps
```

Wait until all health checks pass before proceeding. PostgreSQL containers
typically become ready within 10 seconds; MySQL containers may take up to
60 seconds on first initialization.

### 2.3 Python Environment

If running natively (outside Docker):

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

### 2.4 DSN Configuration

The evaluation script reads DSNs from environment variables or falls back
to defaults. Override as needed:

| Variable | Default |
|----------|---------|
| `POSTGRES_DSN` | `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable` |
| `MYSQL_DSN` | `mysql://root:root@localhost:3307/test` |
| `SQLITE_DSN` | `/tmp/tx_fuzzer_multidb.sqlite` |

For the multi-DB Docker Compose stack, the default PostgreSQL DSN targets
port 5432. To target a specific version, set the DSN explicitly:

```bash
export POSTGRES_DSN="postgres://postgres:postgres@localhost:54326/postgres?sslmode=disable"  # pg16.2
```

---

## 3. The 24-Hour Calibration Run (Single Target)

Before committing to the full evaluation matrix, run a single 24-hour
calibration to establish baseline behavior and verify infrastructure
stability.

**Target configuration:**
- Database: PostgreSQL (port 54326, version 16.2)
- Workload: `multikey_transfer`
- Schedulers: `random`, `heuristic`
- Duration: 86,400 seconds (24 hours)
- Repetitions: 1
- Sample interval: 60 seconds (yields ~1,440 data points)

### 3.1 Launch Command

```bash
python scripts/evaluate_coverage_longitudinal.py \
    --db postgres \
    --workloads multikey_transfer \
    --strategies random heuristic \
    --duration 86400 \
    --reps 1 \
    --sample-interval 60 \
    --output-dir results/longitudinal/calibration
```

### 3.2 Running Inside Docker

If you prefer to run inside the IsoTest container (which has all
dependencies pre-installed), use the `make shell` target and execute from
within:

```bash
make shell
# Inside the container:
python scripts/evaluate_coverage_longitudinal.py \
    --db postgres \
    --workloads multikey_transfer \
    --strategies random heuristic \
    --duration 86400 \
    --reps 1 \
    --sample-interval 60 \
    --output-dir /app/results/longitudinal/calibration
```

Note: When running inside Docker, ensure the PostgreSQL container is
network-reachable. If running with `docker compose`, both containers share
the default bridge network. Set `POSTGRES_DSN` to point at the service
hostname (e.g., `pg16_2`) or the host's mapped port.

### 3.3 Running in the Background (Native)

To detach the process entirely from the terminal (in addition to tmux):

```bash
nohup python scripts/evaluate_coverage_longitudinal.py \
    --db postgres \
    --workloads multikey_transfer \
    --strategies random heuristic \
    --duration 86400 \
    --reps 1 \
    --sample-interval 60 \
    --output-dir results/longitudinal/calibration \
    > results/longitudinal/calibration.log 2>&1 &

echo $!  # Record the PID for monitoring
```

Monitor progress by tailing the log:

```bash
tail -f results/longitudinal/calibration.log
```

---

## 4. Running the Full Evaluation Matrix

Once the calibration run confirms infrastructure stability and provides a
reference asymptote time, run the full matrix for the paper.

### 4.1 Matrix Parameters

| Dimension | Values |
|-----------|--------|
| Workloads | `smallbank`, `multikey_transfer` |
| Schedulers | `stress`, `random`, `heuristic` |
| Databases | `postgres` (or additionally `mysql`, `sqlite`) |
| Repetitions | 3 (for statistical confidence) |
| Duration | Adjust based on calibration (default: 86,400s) |

### 4.2 Full 24-Hour Run

```bash
python scripts/evaluate_coverage_longitudinal.py \
    --db postgres \
    --workloads smallbank multikey_transfer \
    --strategies stress random heuristic \
    --duration 86400 \
    --reps 3 \
    --sample-interval 60 \
    --n-workers 4 \
    --output-dir results/longitudinal/full_24h
```

### 4.3 Abbreviated Run (4 Hours, Based on Calibration)

If the calibration run shows the asymptote is reached well before 24
hours, reduce the duration accordingly:

```bash
python scripts/evaluate_coverage_longitudinal.py \
    --db postgres \
    --workloads smallbank multikey_transfer \
    --strategies stress random heuristic \
    --duration 14400 \
    --reps 3 \
    --sample-interval 30 \
    --n-workers 4 \
    --output-dir results/longitudinal/full_4h
```

### 4.4 Multi-Database Evaluation Script

For the complete cross-database comparison, use the following script:

```bash
#!/usr/bin/env bash
# run_full_longitudinal.sh — Full longitudinal evaluation matrix
set -euo pipefail

DURATION="${1:-86400}"   # Default: 24 hours; pass argument to override
REPS=3
SAMPLE_INTERVAL=60
OUTPUT_BASE="results/longitudinal"
WORKLOADS="smallbank multikey_transfer"
STRATEGIES="stress random heuristic"

echo "=== IsoTest Longitudinal Evaluation ==="
echo "  Duration:   ${DURATION}s per campaign"
echo "  Reps:       ${REPS}"
echo "  Workloads:  ${WORKLOADS}"
echo "  Strategies: ${STRATEGIES}"
echo "========================================"

for DB in postgres mysql sqlite; do
    echo ""
    echo "--- Database: ${DB} ---"
    python scripts/evaluate_coverage_longitudinal.py \
        --db "${DB}" \
        --workloads ${WORKLOADS} \
        --strategies ${STRATEGIES} \
        --duration "${DURATION}" \
        --reps "${REPS}" \
        --sample-interval "${SAMPLE_INTERVAL}" \
        --n-workers 4 \
        --output-dir "${OUTPUT_BASE}/${DB}"
done

echo ""
echo "=== All campaigns complete. Results in ${OUTPUT_BASE}/ ==="
```

Run with:

```bash
chmod +x run_full_longitudinal.sh
./run_full_longitudinal.sh         # 24 hours per DB
./run_full_longitudinal.sh 14400   # 4 hours per DB
```

### 4.5 Including Pool-Based Strategies

To additionally compare the pool-augmented schedulers (which retain and
mutate interesting interleavings), add them to the strategy list:

```bash
python scripts/evaluate_coverage_longitudinal.py \
    --db postgres \
    --workloads smallbank multikey_transfer \
    --strategies stress random heuristic "pool(random)" "pool(heuristic)" \
    --duration 86400 \
    --reps 3 \
    --sample-interval 60 \
    --output-dir results/longitudinal/full_with_pool
```

---

## 5. Output Artifacts & Analysis

### 5.1 Output Location

All results are written to the directory specified by `--output-dir`.
The default is `results/longitudinal/`.

Each run produces two CSV files:

| File | Content |
|------|---------|
| `coverage_timeseries.csv` | Raw time-series: one row per sample point |
| `coverage_longitudinal.csv` | Per-strategy summary aggregated across repetitions |

### 5.2 Time-Series CSV Format (`coverage_timeseries.csv`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_s` | float | Seconds elapsed since campaign start |
| `db` | string | Database identifier (e.g., `postgres`) |
| `workload` | string | Workload name (e.g., `multikey_transfer`) |
| `strategy` | string | Scheduler name (e.g., `heuristic`) |
| `rep` | int | Repetition index (0-based) |
| `cumulative_unique_states` | int | Count of distinct `FinalStateHash` values observed so far |
| `cumulative_trials` | int | Total trials executed so far |
| `cumulative_bugs` | int | Total bug-triggering trials so far |

Example rows:

```csv
timestamp_s,db,workload,strategy,rep,cumulative_unique_states,cumulative_trials,cumulative_bugs
60.001,postgres,multikey_transfer,stress,0,3,142,2
120.003,postgres,multikey_transfer,stress,0,3,287,5
180.002,postgres,multikey_transfer,stress,0,3,431,8
60.001,postgres,multikey_transfer,heuristic,0,5,138,4
120.004,postgres,multikey_transfer,heuristic,0,7,276,9
180.001,postgres,multikey_transfer,heuristic,0,8,412,14
```

### 5.3 Summary CSV Format (`coverage_longitudinal.csv`)

| Column | Type | Description |
|--------|------|-------------|
| `db` | string | Database identifier |
| `workload` | string | Workload name |
| `strategy` | string | Scheduler name |
| `reps` | int | Number of completed repetitions |
| `final_states_mean` | float | Mean final unique-state count across reps |
| `final_states_std` | float | Standard deviation of final state counts |
| `final_trials_mean` | float | Mean total trials executed |
| `final_bugs_mean` | float | Mean total bugs found |
| `duration_s` | float | Mean campaign wall-clock duration |

### 5.4 Interpretation

To identify the state-space asymptote from `coverage_timeseries.csv`:

1. Filter rows for a specific `(db, workload, strategy, rep)` combination.
2. Plot `timestamp_s` (X-axis) versus `cumulative_unique_states` (Y-axis).
3. The asymptote is the timestamp where `cumulative_unique_states` stops
   incrementing—the curve plateaus and no new canonical states are
   discovered regardless of additional trials.

**Key signals to look for:**

- **Stress testing flatline:** The `stress` strategy typically plateaus
  within the first 5-10 minutes, indicating that unguided concurrency has
  exhausted its reachable state subset.
- **Controlled scheduling extended growth:** The `random`, `heuristic`,
  and pool-based strategies continue discovering new states for
  significantly longer before reaching their (higher) asymptotes.
- **Bug accumulation:** A monotonically increasing `cumulative_bugs`
  column paired with growing `cumulative_unique_states` indicates that
  new states correlate with new anomalous interleavings—the scheduler is
  exploring genuinely novel territory, not revisiting known states.

### 5.5 Quick Plotting (Optional)

For a quick visual check using Python:

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results/longitudinal/full_24h/coverage_timeseries.csv")
df_mk = df[df["workload"] == "multikey_transfer"]

for strategy, group in df_mk.groupby("strategy"):
    avg = group.groupby("timestamp_s")["cumulative_unique_states"].mean()
    plt.plot(avg.index / 3600, avg.values, label=strategy)

plt.xlabel("Elapsed Time (hours)")
plt.ylabel("Cumulative Unique States")
plt.title("Longitudinal Coverage: multikey_transfer")
plt.legend()
plt.savefig("results/longitudinal/coverage_plot.png", dpi=150)
```

---

## Appendix: CLI Reference

```
usage: evaluate_coverage_longitudinal.py [-h]
    [--db DB [DB ...]]
    [--workloads WORKLOADS [WORKLOADS ...]]
    [--strategies STRATEGIES [STRATEGIES ...]]
    [--duration DURATION]
    [--reps REPS]
    [--n-workers N_WORKERS]
    [--sample-interval SAMPLE_INTERVAL]
    [--delay-ms DELAY_MS]
    [--seed SEED]
    [--output-dir OUTPUT_DIR]

Options:
  --db              Database(s) to test (default: sqlite)
  --workloads       Workload names (default: bank_overdraft, job_queue_claim, list_append)
  --strategies      Scheduler strategies (default: all five)
  --duration        Campaign duration in seconds (default: 600)
  --reps            Repetitions per (strategy, workload) pair (default: 3)
  --n-workers       Concurrent sessions per trial (default: 4)
  --sample-interval Seconds between coverage samples (default: 2.0)
  --delay-ms        Delay parameter for random/heuristic baselines (default: 15.0)
  --seed            Base random seed (default: 42)
  --output-dir      Output directory (default: results/longitudinal/)
```
