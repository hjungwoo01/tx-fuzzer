#!/usr/bin/env bash
#
# Smoke test the multi-DB suite (minimal duration, 1 rep).
# Validates that the framework runs end-to-end without errors.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

echo "=== Multi-DB Smoke Test ==="

export MPLCONFIGDIR="${MPLCONFIGDIR:-$ROOT/.mplconfig}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$ROOT/.cache}"

if [ -z "${SQLITE_DSN:-}" ]; then
  export SQLITE_DSN="$(mktemp /tmp/tx_fuzzer_smoke.XXXXXX)"
fi

# Run smoke test (skip PG/MySQL if not available; always test SQLite)
python3 -m scripts.experiments.run_multidb \
    --suite quick \
    --smoke-only \
    --db sqlite \
    --sqlite-dsn "$SQLITE_DSN" \
    --output-dir results/multidb_smoke

echo ""
echo "=== Smoke Test Complete ==="
echo "Results in: results/multidb_smoke/"
