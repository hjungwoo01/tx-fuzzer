# Benchmark: Time-To-Failure Measurement for Write Skew Detection

## Overview

This benchmark measures **Time-To-Failure (TTF)** for detecting Write Skew anomalies using two strategies:
1. **Random**: Repeatedly run a diluted workload until a failure is detected
2. **BEC-1**: Use BEC-1 analysis to generate a targeted attack workload

## The Mismatch Strategy

### Configuration Mismatch

The benchmark uses an intentional configuration mismatch between the database and the consistency checker:

- **Database**: Configured as `READ COMMITTED` (Weak isolation level)
  - Write Skew is **technically allowed** at this isolation level
  - The database will not prevent Write Skew anomalies from occurring

- **Checker**: Configured as `SERIALIZABLE` (Strict consistency model)
  - Elle is instructed to check for **any cycle** in the dependency graph
  - Any Write Skew pattern will be flagged as a **failure**

### Why This Mismatch?

This setup simulates a real-world scenario where:

1. An application **requires** strict consistency (serializable semantics)
2. The application was **accidentally deployed** on a weak isolation level (`READ COMMITTED`)
3. The application developer expects serializable behavior but the database doesn't enforce it

By measuring how quickly we can **expose this configuration error**, we can compare:
- **Random fuzzing**: How long until random execution finds a Write Skew?
- **BEC-1 guided fuzzing**: How long until BEC-1 analysis generates a targeted attack?

## Workload: `diluted_skew.yaml`

The benchmark uses a "diluted" Write Skew workload designed to make failures rare:

- **Keyspace**: 10,000 keys (large search space)
- **Transaction Mix**:
  - 45% safe reads (single-key reads)
  - 45% safe writes (single-key writes)
  - 10% risky skew transactions (read two keys, write one)

The dilution makes Write Skew anomalies **unlikely** under random execution, allowing us to measure how long it takes to find them.

### Critical Configuration

The workload includes a top-level `consistency_model: serializable` field:

```yaml
consistency_model: serializable
```

This tells the Go Runner and Elle to:
- Check the history against **serializable** semantics
- Flag **any cycle** in the dependency graph as a failure
- Treat Write Skew as a violation, even though the database is running in `READ COMMITTED`

## Running the Benchmark

### Prerequisites

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   Or install manually:
   ```bash
   pip install pyyaml edn_format networkx matplotlib
   ```

2. Ensure PostgreSQL is running and `DATABASE_URL` is set
   ```bash
   export DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
   ```

3. Initialize the database schema:
   ```bash
   go run ./cmd/runner -config workloads/seed.yaml -out seed_history.edn -init
   ```

### Random Mode

Run random fuzzing until a failure is detected:

```bash
python benchmark_runner.py --mode random --max-iterations 100
```

This will:
1. Loop `diluted_skew.yaml` repeatedly
2. After each iteration, check the history with Elle (using `serializable` model)
3. If `valid: false` (cycle detected), **STOP** and record the time
4. Report Time-To-Failure

### BEC-1 Mode

Run BEC-1 guided fuzzing:

```bash
python benchmark_runner.py --mode bec1
```

This will:
1. **Exploration**: Run `diluted_skew.yaml` (should PASS due to dilution)
   - If it fails immediately, abort with warning: "Search space too small"
2. **Analysis**: Run `analyze_bec1.py` to identify promising RW edges
3. **Retargeting**: Run `next_workload_from_profile.py` to generate `workloads/next.yaml`
4. **Attack**: Run `workloads/next.yaml` (should FAIL - cycle detected)
5. Measure total duration

### Both Modes

Run both modes and compare:

```bash
python benchmark_runner.py --mode both
```

## Interpreting Results

### Random Mode Results

- **Time-To-Failure**: The time until a Write Skew is detected
- **Iterations**: The number of workload runs before failure
- If no failure after `max-iterations`, the benchmark reports "No failure detected"

### BEC-1 Mode Results

- **Total Duration**: Time for exploration + analysis + retargeting + attack
- **Expected Outcome**: Attack workload should fail (cycle detected)
- If exploration fails immediately, the search space may be too small

### Comparison

The key metric is: **How much faster is BEC-1 compared to Random?**

- If BEC-1 is significantly faster, it demonstrates the value of guided fuzzing
- If Random is faster, it may indicate the search space is small enough that random exploration is sufficient

## Technical Details

### Elle Exit Codes

The benchmark checks Elle's exit code to determine validity:
- **0**: Valid (no cycles detected)
- **3**: Unknown (could not determine validity)
- **4**: Invalid (cycle detected - **FAILURE**)

### Retargeting

The `diluted_skew.yaml` workload includes `elle: { key: "$1" }` metadata in the skew transactions. This allows the retargeting script (`next_workload_from_profile.py`) to:
1. Identify retargetable transactions
2. Modify the workload to focus on a specific key identified by BEC-1
3. Generate a "hot" version of transactions that target the promising key

### Consistency Model Extraction

The benchmark runner (and `analyze_and_next.py`) checks for the top-level `consistency_model` field first, then falls back to extracting isolation level from transactions. This allows workloads to explicitly specify the checker's consistency model independent of the database's isolation level.

## Example Output

```
============================================================
MODE: Random
============================================================
Elle model: serializable
Max iterations: 100

--- Iteration 1 ---
Running workload: workloads/diluted_skew.yaml
  ✓ History is VALID (no cycles detected)
  Duration: 35.42s

--- Iteration 2 ---
...

--- Iteration 15 ---
Running workload: workloads/diluted_skew.yaml
  ✗ History is INVALID (cycle detected - FAILURE!)
  Duration: 32.18s

============================================================
FAILURE DETECTED at iteration 15
Time-to-Failure: 512.34 seconds
============================================================
```

## Troubleshooting

- **"Search space too small"**: The exploration workload failed immediately. Consider increasing the keyspace or reducing the skew transaction weight.

- **"BEC-1 profile has no target_key"**: The analysis didn't find any promising edges. The history may not have enough dependencies.

- **"BEC-1 analysis timed out"** or **Exit code -9**: The analysis is taking too long or running out of memory. This can happen with very large history files (100+ MB). Solutions:
  - Reduce the exploration workload duration (e.g., change `duration: "30s"` to `duration: "10s"` in `diluted_skew.yaml`)
  - Reduce the number of clients (e.g., `clients: 2` instead of `clients: 4`)
  - The analysis will automatically limit candidates to 10,000 to prevent excessive computation

- **Elle command not found**: Ensure Clojure is installed and `clj` is in your PATH.

### Performance Tips

For faster BEC-1 analysis:
- **Default workload duration is now 10s** (reduced from 30s) to generate smaller histories
- Fewer clients (2-3 instead of 4) will generate smaller histories
- The analysis automatically:
  - Limits transactions to 5,000 for very large histories (>50MB)
  - Limits candidate evaluation to 5,000 edges
  - Uses path precomputation for small graphs (<1000 nodes)
  - Stops early if a short cycle (length ≤3) is found
