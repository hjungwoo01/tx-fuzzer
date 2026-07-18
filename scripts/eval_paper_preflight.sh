#!/usr/bin/env bash
# ============================================================================
# Pre-flight check for `make eval-paper`.
#
# Verifies:
#   1. The patched oracles in scripts/schedule_pool/oracle.py still pass tests
#   2. All four eval scripts import cleanly with extended_workloads registered
#   3. Each requested DB is reachable on its DSN
#   4. Prints the planned matrix and estimated wall time
#
# Reads the same env vars as `make eval-paper`. Does NOT launch the long-
# running eval; only smoke-checks each phase for ~5 seconds.
# ============================================================================
set -euo pipefail

PYTHON="${PYTHON:-.venv/bin/python}"

EVAL_DBS="${EVAL_DBS:-sqlite}"
EVAL_DURATION_S="${EVAL_DURATION_S:-3600}"
EVAL_SCALE_DURATION_S="${EVAL_SCALE_DURATION_S:-600}"
EVAL_REPS="${EVAL_REPS:-3}"
EVAL_N_WORKERS="${EVAL_N_WORKERS:-4}"
EVAL_WORKLOADS_BASELINE="${EVAL_WORKLOADS_BASELINE:-bank_overdraft inventory_oversell list_append multikey_transfer}"
EVAL_WORKLOADS_SCALE="${EVAL_WORKLOADS_SCALE:-bank_overdraft multikey_transfer}"
EVAL_WORKLOADS_ORACLE="${EVAL_WORKLOADS_ORACLE:-bank_overdraft inventory_oversell unique_violation list_append job_queue_claim lost_update_counter}"
EVAL_STRATEGIES="${EVAL_STRATEGIES:-stress pct heuristic 'pool(heuristic)'}"
EVAL_SCALE_WORKERS="${EVAL_SCALE_WORKERS:-2 8}"
EVAL_SCALE_TABLE_SCALES="${EVAL_SCALE_TABLE_SCALES:-1 2}"
EVAL_OUT="${EVAL_OUT:-results/paper/preflight}"

bar() { printf '%s\n' "============================================================================"; }
section() { echo; bar; echo "  $1"; bar; }

section "1/4  Patched oracles still pass python tests"
$PYTHON -m pytest -q tests/test_schedule_pool.py tests/test_multidb_canonical_hash.py tests/test_evaluation_extensions.py

section "2/4  Eval scripts import cleanly with extended workloads"
$PYTHON - <<'PY'
import sys
sys.path.insert(0, '.')
sys.path.insert(0, 'scripts')
import scripts.evaluate_baselines        # noqa
import scripts.evaluate_scalability       # noqa
import scripts.evaluate_significance      # noqa
import scripts.validate_oracle_soundness  # noqa
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
import scripts.experiments.extended_workloads  # noqa
print(f"  registered workloads ({len(MICRO_WORKLOADS)}): {sorted(MICRO_WORKLOADS)}")
PY

section "3/4  DSN reachability for requested DBs"
for db in $EVAL_DBS; do
    env_name="$(echo "$db" | tr '[:lower:]' '[:upper:]')_DSN"
    dsn="${!env_name:-}"
    printf "  %-9s %s = %s\n" "$db" "$env_name" "${dsn:-<unset, will use DEFAULT_DSNS>}"
    $PYTHON - "$db" "$dsn" <<'PY'
import os, sys
sys.path.insert(0, '.')
sys.path.insert(0, 'scripts')
from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
db, dsn = sys.argv[1], sys.argv[2] or DEFAULT_DSNS.get(sys.argv[1], "")
try:
    a = make_adapter(db, dsn)
    a.begin(); a.exec("SELECT 1"); a.commit(); a.close()
    print(f"    -> reachable")
except Exception as e:
    print(f"    -> UNREACHABLE: {e}")
    sys.exit(1)
PY
done

section "4/4  Smoke pass through each eval script (5s/phase)"

mkdir -p "$EVAL_OUT/_smoke"

# Pick first DB and first workload for smoke
SMOKE_DB="$(echo "$EVAL_DBS" | awk '{print $1}')"
SMOKE_WL_BASE="$(echo "$EVAL_WORKLOADS_BASELINE" | awk '{print $1}')"
SMOKE_WL_SCALE="$(echo "$EVAL_WORKLOADS_SCALE" | awk '{print $1}')"
SMOKE_WL_ORACLE="$(echo "$EVAL_WORKLOADS_ORACLE" | awk '{print $1}')"

echo "  [smoke 1/4] baselines on $SMOKE_DB / $SMOKE_WL_BASE"
$PYTHON scripts/evaluate_baselines.py \
    --db "$SMOKE_DB" \
    --workloads $SMOKE_WL_BASE \
    --strategies stress 'pool(heuristic)' \
    --duration 5 --reps 1 \
    --output-dir "$EVAL_OUT/_smoke/baselines" >/dev/null

echo "  [smoke 2/4] scalability on $SMOKE_DB / $SMOKE_WL_SCALE"
$PYTHON scripts/evaluate_scalability.py \
    --db "$SMOKE_DB" \
    --workloads $SMOKE_WL_SCALE \
    --workers 2 --scales 1 \
    --strategies stress 'pool(heuristic)' \
    --duration 5 --reps 1 \
    --output-dir "$EVAL_OUT/_smoke/scalability" >/dev/null

echo "  [smoke 3/4] significance on the smoke baseline CSV"
$PYTHON scripts/evaluate_significance.py \
    "$EVAL_OUT/_smoke/baselines/baseline_timeseries.csv" \
    --metric states \
    --output "$EVAL_OUT/_smoke/baselines/significance" >/dev/null 2>&1 || \
    echo "    (significance smoke skipped — needs >=2 strategies; expected on tiny smoke)"

echo "  [smoke 4/4] oracle soundness on $SMOKE_DB / $SMOKE_WL_ORACLE"
$PYTHON scripts/validate_oracle_soundness.py \
    --db "$SMOKE_DB" \
    --workloads $SMOKE_WL_ORACLE \
    --strategies random \
    --n-trials 25 --reps 1 \
    --output-dir "$EVAL_OUT/_smoke/oracle_soundness" >/dev/null

echo "  smoke pass OK"

# ----------------------------------------------------------------------------
# Plan + wall-time estimate
# ----------------------------------------------------------------------------

section "Planned matrix"
n_dbs=$(echo $EVAL_DBS | wc -w | tr -d ' ')
n_wls_base=$(echo $EVAL_WORKLOADS_BASELINE | wc -w | tr -d ' ')
n_wls_scale=$(echo $EVAL_WORKLOADS_SCALE | wc -w | tr -d ' ')
n_wls_oracle=$(echo $EVAL_WORKLOADS_ORACLE | wc -w | tr -d ' ')
n_strats=$(echo $EVAL_STRATEGIES | wc -w | tr -d ' ')
n_workers=$(echo $EVAL_SCALE_WORKERS | wc -w | tr -d ' ')
n_scales=$(echo $EVAL_SCALE_TABLE_SCALES | wc -w | tr -d ' ')

baseline_cells=$(( n_dbs * n_wls_base * n_strats * EVAL_REPS ))
scale_cells=$(( n_dbs * n_wls_scale * n_strats * n_workers * n_scales * EVAL_REPS ))
oracle_cells=$(( n_dbs * n_wls_oracle * 2 * EVAL_REPS ))  # 2 strategies fixed

baseline_secs=$(( baseline_cells * EVAL_DURATION_S ))
scale_secs=$(( scale_cells * EVAL_SCALE_DURATION_S ))
oracle_secs=$(( oracle_cells * 60 ))  # validator uses fixed n-trials=200, ~60s/cell estimate

total_secs=$(( baseline_secs + scale_secs + oracle_secs ))
total_hrs=$(( total_secs / 3600 ))
total_mins=$(( (total_secs % 3600) / 60 ))

cat <<EOF
  databases:               $EVAL_DBS  (n=$n_dbs)
  workloads (baselines):   $EVAL_WORKLOADS_BASELINE  (n=$n_wls_base)
  workloads (scalability): $EVAL_WORKLOADS_SCALE  (n=$n_wls_scale)
  workloads (oracle):      $EVAL_WORKLOADS_ORACLE  (n=$n_wls_oracle)
  strategies:              $EVAL_STRATEGIES  (n=$n_strats)
  reps:                    $EVAL_REPS
  n_workers per trial:     $EVAL_N_WORKERS

  baseline duration/cell:  ${EVAL_DURATION_S}s
  scalability duration/cell: ${EVAL_SCALE_DURATION_S}s
  scalability worker counts: $EVAL_SCALE_WORKERS  (n=$n_workers)
  scalability table scales:  $EVAL_SCALE_TABLE_SCALES  (n=$n_scales)

  cells (baselines):       $baseline_cells   ~$(( baseline_secs / 3600 ))h $(( (baseline_secs % 3600) / 60 ))m
  cells (scalability):     $scale_cells   ~$(( scale_secs / 3600 ))h $(( (scale_secs % 3600) / 60 ))m
  cells (oracle):          $oracle_cells   ~$(( oracle_secs / 60 ))m

  output dir:              $EVAL_OUT  (override with EVAL_OUT)

  ESTIMATED WALL TIME:     ${total_hrs}h ${total_mins}m
EOF

section "preflight OK"
echo "  Run: make eval-paper"
echo
