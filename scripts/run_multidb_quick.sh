#!/usr/bin/env bash
#
# Run the multi-DB quick evaluation suite.
# Prerequisites:
#   docker compose -f docker-compose-multidb.yml up -d
#   pip install -r requirements.txt
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

echo "=== Multi-DB Quick Suite ==="
echo "Starting at $(date -u)"
echo ""

export MPLCONFIGDIR="${MPLCONFIGDIR:-$ROOT/.mplconfig}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$ROOT/.cache}"

# Default DSNs matching docker-compose-multidb.yml
export PG_12_3_DSN="${PG_12_3_DSN:-postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable}"
export PG_12_4_DSN="${PG_12_4_DSN:-postgres://postgres:postgres@localhost:54324/postgres?sslmode=disable}"
export MYSQL_DSN="${MYSQL_DSN:-mysql://root:root@localhost:33080/test}"
if [ -z "${SQLITE_DSN:-}" ]; then
  export SQLITE_DSN="$(mktemp /tmp/tx_fuzzer_multidb_quick.XXXXXX)"
fi

# Print SQLite version
python3 -c "import sqlite3; print(f'SQLite version: {sqlite3.sqlite_version}')" || true

# Run quick suite
python3 -m scripts.experiments.run_multidb \
    --suite quick \
    --pg-12-3-dsn "$PG_12_3_DSN" \
    --pg-12-4-dsn "$PG_12_4_DSN" \
    --mysql-dsn "$MYSQL_DSN" \
    --sqlite-dsn "$SQLITE_DSN" \
    --output-dir results/multidb_quick

echo ""
echo "=== Quick Suite Complete ==="
echo "Results in: results/multidb_quick/"
echo "  - coverage_timeseries.csv"
echo "  - case_studies.csv"
echo "  - overhead.csv / overhead.md"
echo "  - summary.md / report.md"
echo "  - plots/*.png"
