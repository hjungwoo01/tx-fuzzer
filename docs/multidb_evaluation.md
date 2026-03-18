# Multi-DB Evaluation Framework

Documentation for the multi-database evaluation framework that
measures state-space coverage, time-to-exposure for known bugs, and
scheduling-overhead across PostgreSQL, MySQL, and SQLite.

> **Constraint**: This is a *scheduling-only* fuzzer — it never mutates SQL or
> data.  Per-session FIFO order is always preserved.  The only degree of freedom
> is the timing between events (injected delays).

---

## Table of Contents

1. [Quick Start](#1-quick-start)
2. [Prerequisites](#2-prerequisites)
3. [File Reference](#3-file-reference)
4. [Running Tests](#4-running-tests)
5. [Strategies](#5-strategies)
6. [Micro Workloads](#6-micro-workloads)
7. [Case Studies](#7-case-studies)
8. [Overhead Benchmark](#8-overhead-benchmark)
9. [Output Artifacts](#9-output-artifacts)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Quick Start

```bash
# ── Smoke test (SQLite only, ~90 seconds, no Docker needed) ──
bash scripts/run_multidb_smoke.sh

# ── Quick suite (all DBs, ~15–25 minutes) ──
docker compose -f docker-compose-multidb.yml up -d
bash scripts/run_multidb_quick.sh

# ── Long suite (all DBs, ~3 hours) ──
docker compose -f docker-compose-multidb.yml up -d
python3 -m scripts.experiments.run_multidb --suite long
```

---

## 2. Prerequisites

### Python

- Python ≥ 3.10 (tested on 3.12)
- Install dependencies:

```bash
pip install -r requirements.txt
```

Key packages: `psycopg2-binary`, `pymysql`, `matplotlib`, `pyyaml`.
SQLite uses the Python stdlib `sqlite3` module — no extra install needed.

### Docker (for Postgres / MySQL)

```bash
docker compose -f docker-compose-multidb.yml up -d
```

This starts:

| Service      | Image            | Host Port | Purpose                                     |
|--------------|------------------|-----------|---------------------------------------------|
| `pg12_3`     | `postgres:12.3`  | `54323`   | Buggy — SSI update_conflict_out vulnerable  |
| `pg12_4`     | `postgres:12.4`  | `54324`   | Fixed — SSI update_conflict_out patched     |
| `pg16_2`     | `postgres:16.2`  | `54326`   | Modern baseline (optional)                  |
| `mysql8_0_34`| `mysql:8.0.34`   | `33080`   | Non-repeatable-read case study              |

All Postgres containers run with `synchronous_commit=off`, `fsync=off`, and
`tmpfs` data directories for fast ephemeral benchmarking.

Wait for healthy status:

```bash
docker compose -f docker-compose-multidb.yml ps
```

### SQLite

No setup required.  SQLite uses the host system's Python `sqlite3` module.
The default DSN creates a temporary file at `/tmp/tx_fuzzer_multidb.sqlite`.

---

## 3. File Reference

All source lives under `scripts/experiments/`.

### `scripts/experiments/__init__.py`

Package marker.  No user-facing code.

---

### `scripts/experiments/error_taxonomy.py`

**Purpose**: Normalises vendor-specific database errors into a common taxonomy
so that all downstream code speaks one language.

**Key exports**:

| Symbol | Description |
|--------|-------------|
| `ErrorCategory` (enum) | `OK`, `SERIALIZATION_FAIL`, `DEADLOCK`, `LOCK_TIMEOUT`, `BUSY`, `OTHER` |
| `classify_pg_error(exc)` | Maps psycopg2 exceptions → `ErrorCategory`.  `40001` → `SERIALIZATION_FAIL`, `40P01` → `DEADLOCK`, `55P03` → `LOCK_TIMEOUT`. |
| `classify_mysql_error(exc)` | Maps PyMySQL exceptions.  `errno 1213` → `DEADLOCK`, `errno 1205` → `LOCK_TIMEOUT`. |
| `classify_sqlite_error(exc)` | Maps `sqlite3` exceptions.  "database is locked" / "locked" → `BUSY`. |
| `classify_error(db, exc)` | Dispatcher: choose classifier by `db` string. |
| `fisher_exact_test(a, b, c, d)` | Two-sided Fisher exact test for 2×2 tables.  Returns p-value. |
| `rate_ratio_ci(n1_s, n1_t, n2_s, n2_t)` | Rate ratio with Katz log-interval 95% CI.  Returns `(ratio, ci_lo, ci_hi)`. |

**Design note**: MySQL `errno 1213` is mapped to `DEADLOCK` (not
`SERIALIZATION_FAIL`) even though its SQLSTATE is `40001` — this matches
MySQL's actual semantics.

---

### `scripts/experiments/db_adapter.py`

**Purpose**: Provides a `DBAdapter` abstract interface and concrete adapters
for Postgres, MySQL, and SQLite with normalised error handling.

**Key exports**:

| Symbol | Description |
|--------|-------------|
| `ExecResult` | Dataclass: `rows`, `rowcount`, `error_category`, `sqlstate`, `error_msg`, `elapsed_ms`. |
| `DBAdapter` (ABC) | Interface: `connect`, `setup`, `begin`, `exec`, `commit`, `rollback`, `close`, `exec_autocommit`, `query_one`. |
| `PostgresExpAdapter` | psycopg2-based.  Auto-rollback on error (Postgres enters error state).  Explicit txn management via `autocommit=True`. |
| `MySQLExpAdapter` | PyMySQL-based.  Parses DSN URIs (`mysql://user:pass@host:port/db`). |
| `SQLiteExpAdapter` | stdlib `sqlite3`.  Supports WAL mode, configurable `busy_timeout`, shared-cache URIs, `read_uncommitted` pragma. |
| `make_adapter(db, dsn, **kwargs)` | Factory.  Creates + connects the right adapter.  Optional kwargs: `sqlite_shared_cache`, `sqlite_read_uncommitted`, `sqlite_busy_timeout_ms`. |
| `DEFAULT_DSNS` | Dict of defaults: PG `localhost:5432`, MySQL `localhost:3307`, SQLite `/tmp/tx_fuzzer_multidb.sqlite`. |

**Usage**:
```python
from scripts.experiments.db_adapter import make_adapter
a = make_adapter("sqlite", "/tmp/test.sqlite")
a.setup(["CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)"])
a.begin()
r = a.exec("SELECT * FROM t")
print(r.rows)
a.commit()
a.close()
```

---

### `scripts/experiments/micro_workloads.py`

**Purpose**: Defines three cross-DB micro workloads, each with a buggy variant,
a fixed variant, an oracle, and a canonical state function.

**Workloads** (`MICRO_WORKLOADS` registry):

| Name | Bug Class | Isolation | Workers | Buggy Pattern | Fixed Pattern |
|------|-----------|-----------|---------|---------------|---------------|
| `job_queue_claim` | Double-claim TOCTOU | READ COMMITTED | 4 | `SELECT status; UPDATE` (no WHERE guard) | `UPDATE … WHERE status='ready'` with rowcount check |
| `bank_overdraft` | Overdraft TOCTOU | READ COMMITTED | 4 | `SELECT balance; UPDATE balance-=80` | `UPDATE … WHERE balance >= 80` with rowcount check |
| `list_append` | Lost update (RMW) | READ COMMITTED | 4 | Read val → concat → write back | Atomic `UPDATE val = val \|\| token` |

Each workload has:

| Function | Signature | Description |
|----------|-----------|-------------|
| `*_setup_sql(db)` | `(str) → List[str]` | DDL/DML to create tables and seed data.  DB-aware syntax (e.g. `AUTO_INCREMENT` vs `AUTOINCREMENT`). |
| `*_buggy(adapter, worker_id)` | `(DBAdapter, int) → Dict` | Runs the racy (buggy) transaction.  Returns `{"outcome": ..., "worker": ...}`. |
| `*_fixed(adapter, worker_id)` | `(DBAdapter, int) → Dict` | Runs the safe (fixed) transaction.  Used as control — must produce 0 bugs. |
| `*_oracle(outcomes, ...)` | `(List[Dict], ...) → (bool, str)` | Returns `(bug_found, reason)`.  For `bank_overdraft` and `list_append`, also receives the canonical state dict. |
| `*_canonical(adapter, outcomes)` | `(DBAdapter, Optional[List]) → Dict` | Reads DB to produce a deterministic logical state.  Includes `error_categories` and `outcome_kinds` for coverage diversity. |

**`MicroWorkload` dataclass** fields: `name`, `setup_sql`, `run_buggy`,
`run_fixed`, `oracle`, `canonical`, `needs_state_for_oracle`,
`canonical_hash(state)`.

---

### `scripts/experiments/micro_runner.py`

**Purpose**: Runs micro workloads with concurrent worker threads, strategy-based
delay injection, and coverage-over-time sampling.

**Key exports**:

| Symbol | Description |
|--------|-------------|
| `MicroRunner` | Per-trial runner.  Creates `n_workers` threads synced via `Barrier`, injects delays per strategy, reads canonical state, checks oracle. |
| `run_coverage_session(...)` | Main loop: runs `MicroRunner.run_trial()` for `duration_s` seconds, samples canonical state diversity every `sample_interval_s` (default 0.5s).  Returns `(List[CoveragePoint], List[TrialResult])`. |
| `write_coverage_csv(points, path)` | Write CSV: columns `time_s, db, workload, strategy, rep, unique_states, txns_done, bugs_found`. |
| `plot_coverage(points, output_dir, db)` | Generates per-workload PNG plots of coverage-over-time curves (one line per strategy). |
| `CoveragePoint` | Dataclass for time-series points. |
| `TrialResult` | Dataclass for individual trial: `outcomes`, `canonical_state`, `canonical_hash`, `bug_found`, `bug_reason`, `elapsed_ms`, `error_categories`. |

**`MicroRunner.__init__` parameters**:

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `workload` | `MicroWorkload` | — | Workload to run |
| `db` | `str` | — | `"postgres"`, `"mysql"`, or `"sqlite"` |
| `dsn` | `str` | — | Connection string |
| `n_workers` | `int` | `4` | Concurrency level |
| `variant` | `str` | `"buggy"` | `"buggy"` or `"fixed"` |
| `strategy` | `str` | `"stress"` | `"stress"`, `"random"`, or `"heuristic"` |
| `delay_ms` | `float` | `0.0` | Max delay in ms (for random/heuristic) |
| `heuristic_k` | `int` | `3` | Deterministic rotation modulus |

---

### `scripts/experiments/case_studies.py`

**Purpose**: Time-to-exposure benchmarks for one known bug per DB engine.
Each case study runs `stress` / `random` / `heuristic` and includes a
**control** run that must yield 0 false positives.

**Postgres — `update_conflict_out`** (PG 12.3 vs 12.4):

- **Bug**: SSI conflict-out check misses an rw-antidependency cycle when a
  "trouble" session's aborted UPDATE creates a dead physical tuple version.
  3 sessions: `foo`, `bar`, `trouble`.
- **Oracle**: foo and bar both commit with mutual invisibility (cycle present)
  but no `40001` raised → bug.
- **Control**: PG 12.4 (where the bug was fixed) → must be clean.
- **Implementation**: Delegates to the proven `scripts/discover_update_conflict_out.py`
  engine.  The `heuristic` strategy uses anti-dependency analysis + weighted
  lottery to select delay points (30–200ms range).  `random` uses uniform
  0–100ms on all 11 session events.

**MySQL — `nonrepeatable_read_rr`**:

- **Bug**: Under REPEATABLE READ, Txn R reads `name`, updates `gender` (forces
  row re-evaluation), reads `name` again — sees a different value because
  Txn W updated `name` between the two reads.
- **Oracle**: `name1 != name2` within the same transaction → bug.
- **Control**: `SELECT … FOR UPDATE` / `SERIALIZABLE` → must prevent the anomaly.

**SQLite — `dirty_read_shared_cache`**:

- **Bug**: With `shared_cache=true` and `PRAGMA read_uncommitted=ON`, a reader
  sees uncommitted `'dirty'` value while writer holds an uncommitted update.
- **Oracle**: reader sees `'dirty'` before writer's `ROLLBACK` → bug.
- **Control**: `shared_cache=false`, `read_uncommitted=false` → must be clean.

**Key export**: `CaseResult` dataclass — `db`, `case_name`, `strategy`,
`rep`, `bug_found`, `time_to_first_s`, `trials_to_first`, `total_trials`,
`total_time_s`, `control`.

---

### `scripts/experiments/overhead_bench.py`

**Purpose**: Measures the performance overhead of the scheduling fuzzer.

**Three modes per (workload × DB)**:

| Mode | Strategy | Delay | What it measures |
|------|----------|-------|------------------|
| `baseline` | `stress` | 0ms | Raw throughput — no analysis, no delays |
| `analysis` | `heuristic` | 0ms | Cost of running delay-target selection without actually injecting delays |
| `full` | `heuristic` | real (5ms) | Full fuzzing cost including injected delays |

**Key exports**: `run_overhead_benchmark(...)`, `OverheadRow` dataclass
(`db, workload, mode, txns_per_sec, median_latency_ms, p95_latency_ms,
total_txns, total_time_s`), `write_overhead_csv`, `write_overhead_md`.

**Expected results**: `analysis ≈ baseline` (no measurable overhead from
heuristic selection alone); `full` shows 25–35% slowdown due to injected delays.

---

### `scripts/experiments/run_multidb.py`

**Purpose**: Main orchestrator that ties everything together.  Runs all three
sections (micro coverage, case studies, overhead) and generates reports.

**CLI**:

```
python3 -m scripts.experiments.run_multidb [OPTIONS]

Options:
  --suite {quick,long}       Suite configuration (default: quick)
  --smoke-only               Minimal reps/duration for CI smoke test
  --db [DB ...]              Specific DB(s): postgres, mysql, sqlite
  --output-dir PATH          Output directory
  --pg-12-3-dsn DSN          Override PG 12.3 DSN
  --pg-12-4-dsn DSN          Override PG 12.4 DSN
  --mysql-dsn DSN            Override MySQL DSN
  --sqlite-dsn DSN           Override SQLite path
  --skip-micro               Skip micro workload coverage section
  --skip-cases               Skip case studies section
  --skip-overhead            Skip overhead benchmark section
```

**Suite configurations**:

| Parameter | Quick | Long |
|-----------|-------|------|
| Coverage duration (per workload/strategy/rep) | 20s | 300s (5 min) |
| Repetitions | 5 | 5 |
| Workers | 4 | 4 |
| Delay | 5ms | 5ms |
| Case study duration | 30s | 600s (10 min) |
| Case study max trials | 100 | 1000 |
| Overhead duration | 10s | 120s (2 min) |

**Smoke mode** (`--smoke-only`) overrides: 1 rep, 5s coverage duration, 10s
case study duration, 20 trial max, 5s overhead duration.

---

### `scripts/run_multidb_quick.sh`

Shell entry point for the quick suite.  Sets DSN environment variables from
`docker-compose-multidb.yml` ports and invokes `run_multidb.py --suite quick`.

### `scripts/run_multidb_smoke.sh`

Shell entry point for the smoke test.  SQLite-only, no Docker needed.
Invokes `run_multidb.py --suite quick --smoke-only --db sqlite`.

### `docker-compose-multidb.yml`

Docker Compose file for the multi-DB test infrastructure.  Pinned image
versions, health checks, `tmpfs` data dirs, and relaxed durability settings
for fast benchmarking.

---

## 4. Running Tests

### Smoke Test (SQLite only)

**Duration**: ~60–90 seconds  
**Docker required**: No  
**What it validates**: End-to-end framework correctness — all three sections
(micro coverage, case studies, overhead) run and produce outputs.

```bash
# Option A: shell script
bash scripts/run_multidb_smoke.sh

# Option B: direct invocation
python3 -m scripts.experiments.run_multidb \
    --suite quick --smoke-only --db sqlite \
    --sqlite-dsn /tmp/tx_fuzzer_smoke.sqlite \
    --output-dir results/multidb_smoke
```

**Expected output**: All sections print `=== DONE ===`.  Results directory
contains `summary.md`, `coverage_timeseries.csv`, `case_studies.csv`,
`overhead.csv`, `overhead.md`, `report.md`, and PNG plots in `plots/`.

### Quick Suite (all DBs)

**Duration**: ~15–25 minutes (depends on hardware and DB availability)  
**Docker required**: Yes (for Postgres + MySQL)  
**What it validates**: Full evaluation with 5 reps, 20s coverage windows, 30s
case studies, 10s overhead.

```bash
# Start DBs
docker compose -f docker-compose-multidb.yml up -d

# Wait for health (all should show "healthy")
docker compose -f docker-compose-multidb.yml ps

# Run
bash scripts/run_multidb_quick.sh
# or:
python3 -m scripts.experiments.run_multidb --suite quick
```

**Breakdown of time (approx.)**:

| Section | Per-item | Items | Total |
|---------|----------|-------|-------|
| Micro coverage (buggy) | 20s × 3 strategies × 5 reps | × 3 workloads × 3 DBs | ~45 min worst-case; parallel per-trial ≈ 15 min |
| Micro coverage (control) | 10s × 3 strategies | × 3 workloads × 3 DBs | ~4.5 min |
| Case studies | 30s × 3 strategies × 5 reps | × 3 DBs (2 configs each) | ~15 min |
| Overhead | 10s × 3 modes | × 3 workloads × 3 DBs | ~4.5 min |

**Note**: If a DB is unavailable, it is skipped with a warning.

### Long Suite (all DBs)

**Duration**: ~2.5–3.5 hours  
**Docker required**: Yes  
**What it validates**: Statistically robust evaluation with 300s coverage
windows, 600s case studies, 120s overhead.

```bash
docker compose -f docker-compose-multidb.yml up -d
python3 -m scripts.experiments.run_multidb --suite long
```

### Single-DB Runs

```bash
# SQLite only (no Docker)
python3 -m scripts.experiments.run_multidb --suite quick --db sqlite

# Postgres only
python3 -m scripts.experiments.run_multidb --suite quick --db postgres \
    --pg-12-3-dsn "host=localhost port=54323 dbname=postgres user=postgres password=postgres" \
    --pg-12-4-dsn "host=localhost port=54324 dbname=postgres user=postgres password=postgres"

# MySQL only
python3 -m scripts.experiments.run_multidb --suite quick --db mysql \
    --mysql-dsn "mysql://root:root@localhost:33080/test"
```

### Individual Sections Only

```bash
# Only case studies
python3 -m scripts.experiments.run_multidb --suite quick --skip-micro --skip-overhead

# Only overhead
python3 -m scripts.experiments.run_multidb --suite quick --skip-micro --skip-cases

# Only micro coverage
python3 -m scripts.experiments.run_multidb --suite quick --skip-cases --skip-overhead
```

### Custom DSNs via Environment Variables

```bash
export PG_12_3_DSN="postgres://myuser:mypass@10.0.0.1:5432/mydb"
export PG_12_4_DSN="postgres://myuser:mypass@10.0.0.2:5432/mydb"
export MYSQL_DSN="mysql://root:secret@10.0.0.3:3306/test"
export SQLITE_DSN="/data/fuzzer/eval.sqlite"
python3 -m scripts.experiments.run_multidb --suite quick
```

---

## 5. Strategies

Three scheduling strategies are compared across all experiments:

| Strategy | Description | Delay | Purpose |
|----------|-------------|-------|---------|
| `stress` | OS scheduler only, no injected delays | 0ms | Baseline — what you get without fuzzing |
| `random` | Uniform random delay `U(0, K)` ms before each worker's transaction | 0–K ms | Naive scheduling exploration |
| `heuristic` | Deterministic rotation: worker `(rotation % N)` gets delay `K` ms; others get 0 | K ms on 1 worker | Systematic exploration of interleaving space |

For the Postgres case study, `heuristic` uses the full anti-dependency
analysis + weighted lottery from `discover_update_conflict_out.py` (5-tier
ranking with 30–200ms delay ranges).

---

## 6. Micro Workloads

### `job_queue_claim`

Two workers race to claim a single job (id=1, status='ready').

- **Buggy**: `SELECT status` → check → `UPDATE SET status='claimed'`.
  The TOCTOU gap allows both workers to see `status='ready'` and both claim.
- **Fixed**: `UPDATE … WHERE status='ready'` — only one worker's UPDATE
  matches; the other gets `rowcount=0`.
- **Oracle**: `>1 distinct worker in outcomes with outcome='claimed'` → bug.
- **Coverage**: Typical final states: 4–23 depending on strategy.

### `bank_overdraft`

Two workers try to withdraw 80 from a balance of 100.

- **Buggy**: `SELECT balance` → check `≥ 80` → `UPDATE balance -= 80`.
  Both can read balance=100 and both withdraw → balance goes to −60.
- **Fixed**: `UPDATE … WHERE balance >= 80` — atomic check.
- **Oracle**: `final_balance < 0` OR `successful_withdrawals > 1` → bug.
- **Coverage**: Typical final states: 1–4 depending on strategy.

### `list_append`

Four workers append tokens (`t0`, `t1`, …) to a shared string (key-value row).

- **Buggy**: Read val → concat token → write back.  Read-modify-write race
  causes lost appends.
- **Fixed**: `UPDATE val = val || ',' || token` — atomic single statement.
- **Oracle**: Any appended token missing from final val → bug.
- **Coverage**: Typical final states: 4–60+ depending on strategy.

---

## 7. Case Studies

### Postgres: `update_conflict_out` (SSI Bug)

| Aspect | Detail |
|--------|--------|
| PG Versions | 12.3 (buggy), 12.4 (fixed/control) |
| Sessions | 3: `foo`, `bar`, `trouble` |
| Isolation | SERIALIZABLE (SSI) |
| Bug mechanism | `trouble` UPDATEs then ABORTs, leaving a dead physical tuple version that confuses PG 12.3's conflict-out check |
| Hit rate (heuristic) | ~3% per trial on PG 12.3 |
| Hit rate (stress) | ~0% (OS hides the bug) |
| Expected TTF (heuristic, quick) | 2–35s |
| Control (PG 12.4) | Must be 0 bugs across all trials |
| Quick suite | 30s / 100 trials per strategy |
| Long suite | 600s / 1000 trials per strategy |

### MySQL: Non-Repeatable Read at RR

| Aspect | Detail |
|--------|--------|
| Version | MySQL 8.0.34 |
| Sessions | 2: reader (`Txn R`), writer (`Txn W`) |
| Isolation | REPEATABLE READ |
| Bug mechanism | After `Txn R` updates gender, MySQL re-evaluates the row and picks up `Txn W`'s concurrent name change |
| Control | `SELECT … FOR UPDATE` or `SERIALIZABLE` → must prevent the anomaly |
| Quick suite | 30s / 100 trials per strategy |

### SQLite: Dirty Read (Shared Cache)

| Aspect | Detail |
|--------|--------|
| Sessions | 2: writer, reader |
| Config (buggy) | `shared_cache=true`, `read_uncommitted=ON` |
| Bug mechanism | Reader sees uncommitted `'dirty'` value during writer's uncommitted UPDATE |
| Control | `shared_cache=false`, `read_uncommitted=false` → must be clean |
| Hit rate (heuristic) | Very high (~100% per trial with correct timing) |
| Quick suite | 10s / 20 trials per strategy |

---

## 8. Overhead Benchmark

Three modes measure increasing levels of fuzzer engagement:

```
baseline  ──►  analysis  ──►  full
(0 cost)      (compute only)    (compute + inject delays)
```

**Key metric**: `analysis ≈ baseline` confirms zero overhead from the
heuristic selection logic alone.  `full` quantifies the cost of actually
injecting delays.

**Typical results** (SQLite on modern hardware):

| Mode | Txns/sec | Median latency |
|------|----------|----------------|
| baseline | 108–147 | 6.8–9.2ms |
| analysis | 107–146 | 6.8–9.2ms |
| full | 75–91 | 11.0–13.1ms |

**Quick suite**: 10s per (workload × mode × DB) = ~4.5 min total.  
**Long suite**: 120s per item = ~54 min total.

---

## 9. Output Artifacts

All outputs go to `--output-dir` (default `results/multidb_<suite>`).

| File | Description |
|------|-------------|
| `coverage_timeseries.csv` | Time-series: `time_s, db, workload, strategy, rep, unique_states, txns_done, bugs_found`.  One row per 0.5s sample. |
| `case_studies.csv` | Per-case-study-run: `db, case_name, strategy, rep, bug_found, time_to_first_s, trials_to_first, total_trials, total_time_s, control`. |
| `overhead.csv` | Per-mode: `db, workload, mode, txns_per_sec, median_latency_ms, p95_latency_ms, total_txns, total_time_s`. |
| `overhead.md` | Markdown table of overhead results. |
| `summary.md` | Full summary: coverage table, case study table, Fisher test comparisons, 0-false-positive check, overhead table. |
| `report.md` | Same as `summary.md` (duplicate for different downstream consumers). |
| `plots/coverage_<db>_<workload>.png` | Coverage-over-time curve per workload.  One line per strategy, averaged across reps. |

---

## 10. Troubleshooting

### "Cannot import discover_update_conflict_out"

The Postgres case study requires `psycopg2-binary` and the project root on
`PYTHONPATH`.  Run from the repo root:

```bash
cd /path/to/tx-fuzzer
pip install psycopg2-binary
python3 -m scripts.experiments.run_multidb --db postgres
```

### "database is locked" (SQLite)

SQLite's single-writer model causes `BUSY` errors under concurrency.
The adapter sets `PRAGMA busy_timeout = 5000` (5s) and uses WAL mode.
If you see persistent lock contention, increase the timeout:

```python
a = make_adapter("sqlite", "/tmp/test.db", sqlite_busy_timeout_ms=10000)
```

### Docker containers not healthy

```bash
docker compose -f docker-compose-multidb.yml logs pg12_3
docker compose -f docker-compose-multidb.yml restart
```

### PG case study not finding bugs in smoke mode

The `update_conflict_out` bug has a ~3% per-trial hit rate with the heuristic.
In smoke mode (20 trials), P(finding ≥1 bug) ≈ 46%.  This is expected.  Use
the quick suite (100 trials, 30s) for reliable detection:

```bash
python3 -m scripts.experiments.run_multidb --suite quick --db postgres --skip-micro --skip-overhead
```

### matplotlib warnings

If `matplotlib` is not installed, plots are skipped with a warning.  All
CSV data is still generated.  Install with:

```bash
pip install matplotlib
```

### Running with custom PG versions

```bash
python3 -m scripts.experiments.run_multidb --suite quick --db postgres \
    --pg-12-3-dsn "host=my-pg-12.3-host port=5432 dbname=postgres user=admin password=secret" \
    --pg-12-4-dsn "host=my-pg-12.4-host port=5432 dbname=postgres user=admin password=secret"
```

---

## Appendix: Duration Reference

| Test Type | Command | Time Estimate | Docker |
|-----------|---------|---------------|--------|
| Smoke (SQLite) | `bash scripts/run_multidb_smoke.sh` | **~60–90s** | No |
| Quick, SQLite only | `--suite quick --db sqlite` | **~10–12 min** | No |
| Quick, Postgres only | `--suite quick --db postgres` | **~12–15 min** | Yes |
| Quick, all DBs | `bash scripts/run_multidb_quick.sh` | **~15–25 min** | Yes |
| Long, all DBs | `--suite long` | **~2.5–3.5 hours** | Yes |
| Case studies only (PG) | `--suite quick --db postgres --skip-micro --skip-overhead` | **~5 min** | Yes |
| Overhead only (SQLite) | `--suite quick --db sqlite --skip-micro --skip-cases` | **~2–3 min** | No |
