# Multi-DB Evaluation Runbook

## 1. Scope

This runbook covers everything added for the multi-DB evaluation framework:

1. Postgres + MySQL + SQLite micro-suite coverage growth over time.
2. DB-specific case studies with time-to-first-exposure vs stress baseline.
3. Overhead measurement: baseline vs analysis-only vs full fuzzing.
4. One-command end-to-end execution and validation.

It also includes test procedures for all new pieces: adapters, replay CLI, workloads, suite runner, and generated artifacts.

## 2. What Was Added

Core implementation files:

- `scripts/experiments/multidb_db.py`
- `scripts/experiments/multidb_workloads.py`
- `scripts/experiments/run_multidb_suite.py`
- `scripts/experiments/plot_coverage.py`
- `docker-compose-multidb.yml`
- `scripts/experiments/run_multidb_quick.sh`
- `scripts/repro_all.sh`
- `tests/test_multidb_canonical_hash.py`

Compatibility updates:

- `scripts/replay_litmus.py`
  - New: `--db {postgres|mysql|sqlite}`, `--sqlite-path`.
  - Backward compatible: old `--target` still works.
- `scripts/adapters.py`
  - New SQLite adapter for litmus/replay path.
- `scripts/run_litmus.py`
  - SQLite target enabled in CLI.

Planning note:

- `docs/multidb_plan.md`

## 3. Prerequisites

1. Python 3.10+.
2. Docker + Docker Compose v2 (`docker compose`).
3. Python dependencies installed in your environment:

```bash
cd /home/jay/tx-fuzzer
python3 -m pip install -r requirements.txt
```

Notes:

- `matplotlib` is optional for plotting quality. If missing, `plot_coverage.py` still generates PNG plots using a built-in fallback renderer.
- MySQL runs require `pymysql` from `requirements.txt`.

## 4. Container Versions and Ports

`docker-compose-multidb.yml` pins:

1. PostgreSQL
   - 12.3 on `localhost:54323`
   - 12.4 on `localhost:54324`
   - 16.2 on `localhost:54326`
2. MySQL
   - 5.7.8 on `localhost:33078`
   - 5.7.9 on `localhost:33079`
   - 8.0.34 on `localhost:33080` (`mysql_native_password`)
3. SQLite
   - Host file-backed DB (no container).

## 5. One-Command End-to-End (Quick Suite)

Run everything (bring up containers, wait for health, run quick suite):

```bash
cd /home/jay/tx-fuzzer
./scripts/repro_all.sh
```

Outputs go to:

- `results/multidb_quick/`

Main artifacts:

1. `coverage_timeseries.csv`
2. `plots/coverage_*.png`
3. `case_studies.csv`
4. `case_studies_summary.csv`
5. `replay_summary.csv`
6. `overhead.csv`
7. `overhead.md`
8. `summary.csv`
9. `summary.jsonl`
10. `summary.md`
11. `report.md`

## 6. Direct Suite Commands

Quick:

```bash
cd /home/jay/tx-fuzzer
./scripts/experiments/run_multidb_quick.sh
```

Long:

```bash
cd /home/jay/tx-fuzzer
python3 scripts/experiments/run_multidb_suite.py --profile long --output-dir results/multidb_long
```

Smoke:

```bash
cd /home/jay/tx-fuzzer
python3 scripts/experiments/run_multidb_suite.py --profile quick --smoke --output-dir results/multidb_quick_smoke
```

Custom seed/output:

```bash
python3 scripts/experiments/run_multidb_suite.py \
  --profile quick \
  --base-seed 123 \
  --output-dir results/multidb_quick_seed123
```

## 7. DSN Overrides

You can override any DB target DSN using env vars before running the suite:

- `PG_12_3_DSN`
- `PG_12_4_DSN`
- `PG_16_2_DSN`
- `MYSQL_5_7_8_DSN`
- `MYSQL_5_7_9_DSN`
- `MYSQL_8_0_34_DSN`
- `SQLITE_DSN`

Example:

```bash
export MYSQL_8_0_34_DSN="mysql://root:root@127.0.0.1:33080/test"
./scripts/experiments/run_multidb_quick.sh
```

## 8. Testing Everything That Was Implemented

### 8.1 Static and Import Checks

```bash
cd /home/jay/tx-fuzzer
python3 -m py_compile \
  scripts/adapters.py \
  scripts/run_litmus.py \
  scripts/replay_litmus.py \
  scripts/experiments/multidb_db.py \
  scripts/experiments/multidb_workloads.py \
  scripts/experiments/plot_coverage.py \
  scripts/experiments/run_multidb_suite.py
```

### 8.2 Unit-ish Canonical Hash Tests

```bash
cd /home/jay/tx-fuzzer
python3 -m pytest -q tests/test_multidb_canonical_hash.py
```

This validates:

1. Canonical hash stability.
2. Canonical state hash exists for all micro workloads.
3. `list_append_micro` yields multiple states on SQLite.

### 8.3 Replay CLI Compatibility Tests

Check new CLI options:

```bash
cd /home/jay/tx-fuzzer
python3 scripts/replay_litmus.py --help
```

You should see:

- `--db {postgres,mysql,sqlite}`
- `--sqlite-path`

Quick SQLite replay smoke:

```bash
cd /home/jay/tx-fuzzer
cat > /tmp/sqlite_trace.json <<'JSON'
[
  {"event_id":"c0_begin","client_id":0,"type":"begin","sql":"BEGIN"},
  {"event_id":"c0_update","client_id":0,"type":"sql","sql":"UPDATE kv SET v=$1 WHERE k=$2","args":[1,1]},
  {"event_id":"c0_commit","client_id":0,"type":"commit","sql":"COMMIT"}
]
JSON

python3 scripts/replay_litmus.py \
  --db sqlite \
  --sqlite-path /tmp/replay_litmus.sqlite \
  --trace /tmp/sqlite_trace.json
```

### 8.4 Litmus Adapter Routing (SQLite)

```bash
cd /home/jay/tx-fuzzer
python3 scripts/run_litmus.py --target sqlite --test lost_update --dsn /tmp/litmus.sqlite
```

### 8.5 End-to-End Smoke Suite

```bash
cd /home/jay/tx-fuzzer
python3 scripts/experiments/run_multidb_suite.py --profile quick --smoke --output-dir results/multidb_quick_smoke
```

Validate outputs:

```bash
test -f results/multidb_quick_smoke/coverage_timeseries.csv
test -f results/multidb_quick_smoke/summary.md
test -f results/multidb_quick_smoke/report.md
ls results/multidb_quick_smoke/plots/coverage_*.png
```

### 8.6 Full Quick E2E

```bash
cd /home/jay/tx-fuzzer
time ./scripts/repro_all.sh
```

### 8.7 Existing Postgres Reproducer Safety Check

This confirms the pre-existing `update_conflict_out` flow still works:

```bash
cd /home/jay/tx-fuzzer
./scripts/repro_update_conflict_out.sh --compare
```

Expected behavior:

1. Vulnerable path triggers on 12.3.
2. Fixed path on 12.4 does not reproduce and returns serialization failures (`40001`).

## 9. Acceptance/Regression Checks (Post-Run)

Use these checks after quick or long suite.

### 9.1 Coverage CSV and Plot Presence

```bash
cd /home/jay/tx-fuzzer
test -f results/multidb_quick/coverage_timeseries.csv
ls results/multidb_quick/plots/coverage_*.png | head -n 1
```

### 9.2 Control Variants Must Stay Clean

```bash
cd /home/jay/tx-fuzzer
python3 - <<'PY'
import csv, sys
path='results/multidb_quick/case_studies_summary.csv'
bad=[]
with open(path) as f:
    for r in csv.DictReader(f):
        if r['control']=='yes' and float(r['mean_anomaly_rate'])>0.0:
            bad.append(r)
print('control_rows_with_nonzero_anomaly:', len(bad))
if bad:
    for r in bad: print(r)
    sys.exit(1)
PY
```

### 9.3 Replay Success (Quick: 3/3 per Stored Artifact)

```bash
cd /home/jay/tx-fuzzer
python3 - <<'PY'
import csv, sys
path='results/multidb_quick/replay_summary.csv'
bad=[]
with open(path) as f:
    for r in csv.DictReader(f):
        if int(r['replays'])!=3 or int(r['successes'])!=3:
            bad.append(r)
print('non_3of3_rows:', len(bad))
if bad:
    for r in bad: print(r)
    sys.exit(1)
PY
```

### 9.4 Micro Workloads Reach Multiple States

```bash
cd /home/jay/tx-fuzzer
python3 - <<'PY'
import csv
from collections import defaultdict
path='results/multidb_quick/coverage_timeseries.csv'
best=defaultdict(int)
with open(path) as f:
    for r in csv.DictReader(f):
        k=(r['db'], r['workload'], r['strategy'], r['repetition'])
        best[k]=max(best[k], int(r['cumulative_unique_states']))
by_workload=defaultdict(int)
for (db,w,_,_),v in best.items():
    by_workload[w]=max(by_workload[w], v)
for w,v in sorted(by_workload.items()):
    print(w, v)
assert all(v>=2 for v in by_workload.values())
PY
```

### 9.5 Overhead Table Exists

```bash
cd /home/jay/tx-fuzzer
test -f results/multidb_quick/overhead.csv
test -f results/multidb_quick/overhead.md
```

## 10. Troubleshooting

### 10.1 `pymysql` missing

Install requirements:

```bash
python3 -m pip install -r requirements.txt
```

### 10.2 `matplotlib` missing

Plots still generate using fallback renderer. If you want matplotlib plots:

```bash
python3 -m pip install matplotlib
```

### 10.3 Docker image tag pull issues

If any pinned MySQL tag is unavailable in your environment, update only the affected image tag in `docker-compose-multidb.yml` and keep the target version pairing documented in your run report.

### 10.4 Containers are up but suite says unavailable

Check health and port mapping:

```bash
docker compose -f docker-compose-multidb.yml ps
docker logs txf_pg16_2 --tail 50
docker logs txf_mysql8_0_34 --tail 50
```

## 11. Cleanup

Stop and remove multi-DB containers:

```bash
cd /home/jay/tx-fuzzer
docker compose -f docker-compose-multidb.yml down -v
```
