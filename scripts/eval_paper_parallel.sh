#!/usr/bin/env bash
# ============================================================================
# Parallel paper-eval runner: one process per DB, isolated EVAL_OUT per
# process, isolated SQLITE_DSN per process. Concurrency-safe.
#
# Reads EVAL_DBS from env (default: "sqlite postgres mysql") and spawns one
# `make eval-paper` per DB, each writing to a sub-directory under EVAL_OUT.
# Waits for all to complete; prints per-DB exit status; exits non-zero if any
# child failed.
#
# Usage:
#   EVAL_DBS_PARALLEL="sqlite postgres mysql" \
#     POSTGRES_DSN="..." MYSQL_DSN="..." \
#     bash scripts/eval_paper_parallel.sh
#
# Or via Make:
#   make eval-paper-parallel
# ============================================================================
set -euo pipefail

PYTHON="${PYTHON:-.venv/bin/python}"

# Use EVAL_DBS_PARALLEL to avoid colliding with EVAL_DBS read by the per-DB
# `make eval-paper` invocation.
EVAL_DBS_PARALLEL="${EVAL_DBS_PARALLEL:-${EVAL_DBS:-sqlite}}"

EVAL_OUT_ROOT="${EVAL_OUT_ROOT:-results/paper/$(date +%Y%m%d_%H%M%S)_parallel}"
mkdir -p "$EVAL_OUT_ROOT"

bar() { printf '%s\n' "============================================================================"; }

echo
bar
echo "  PARALLEL paper-eval — one process per DB"
bar
echo "  EVAL_DBS_PARALLEL = $EVAL_DBS_PARALLEL"
echo "  EVAL_OUT_ROOT     = $EVAL_OUT_ROOT"
echo "  PYTHON            = $PYTHON"
echo

# ----------------------------------------------------------------------------
# Pre-flight: confirm each DB is reachable BEFORE spending 24h on it.
# ----------------------------------------------------------------------------
for db in $EVAL_DBS_PARALLEL; do
    env_name="$(echo "$db" | tr '[:lower:]' '[:upper:]')_DSN"
    case "$db" in
        sqlite)
            # Each parallel sqlite needs its own file; auto-namespace if user
            # didn't set one explicitly.
            if [ -z "${SQLITE_DSN:-}" ]; then
                export "PER_DB_SQLITE_DSN_$db=$EVAL_OUT_ROOT/$db.sqlite"
            fi
            ;;
    esac
    dsn="${!env_name:-}"
    $PYTHON - "$db" "$dsn" <<'PY' || { echo "  FAIL: $db unreachable"; exit 1; }
import os, sys
sys.path.insert(0, '.')
sys.path.insert(0, 'scripts')
from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
db, dsn = sys.argv[1], sys.argv[2] or DEFAULT_DSNS.get(sys.argv[1], "")
a = make_adapter(db, dsn)
a.begin(); a.exec("SELECT 1"); a.commit(); a.close()
PY
    echo "  $db reachable"
done
echo

# ----------------------------------------------------------------------------
# Spawn one `make eval-paper` per DB, each isolated.
# ----------------------------------------------------------------------------
# bash 3.2 (macOS) doesn't have associative arrays, so we keep parallel
# indexed arrays of (db, pid).
db_list=()
pid_list=()
for db in $EVAL_DBS_PARALLEL; do
    db_out="$EVAL_OUT_ROOT/$db"
    db_log="$EVAL_OUT_ROOT/$db.log"
    mkdir -p "$db_out"

    # Per-process SQLITE_DSN so two sqlite runs don't race on the default
    # /tmp file. For pg/mysql, each DB has its own DSN already.
    if [ "$db" = "sqlite" ] && [ -z "${SQLITE_DSN:-}" ]; then
        per_dsn="$EVAL_OUT_ROOT/$db.sqlite"
        export_dsn="SQLITE_DSN=$per_dsn"
    else
        export_dsn=""
    fi

    echo "  launching: $db -> $db_out  (log: $db_log)"
    (
        if [ -n "$export_dsn" ]; then export $export_dsn; fi
        EVAL_DBS="$db" \
        EVAL_OUT="$db_out" \
        make eval-paper > "$db_log" 2>&1
    ) &
    db_list+=("$db")
    pid_list+=($!)
done

echo
echo "  spawned ${#db_list[@]} processes; waiting..."
echo

# ----------------------------------------------------------------------------
# Wait, collecting per-DB exit status.
# ----------------------------------------------------------------------------
overall=0
for i in "${!db_list[@]}"; do
    db="${db_list[$i]}"
    pid="${pid_list[$i]}"
    if wait "$pid"; then
        echo "  [OK]   $db (pid=$pid)"
    else
        rc=$?
        echo "  [FAIL] $db (pid=$pid, exit=$rc) — see $EVAL_OUT_ROOT/$db.log"
        overall=1
    fi
done

echo
bar
if [ $overall -eq 0 ]; then
    echo "  Parallel eval complete -> $EVAL_OUT_ROOT"
else
    echo "  Parallel eval finished with errors -> $EVAL_OUT_ROOT"
fi
bar

exit $overall
