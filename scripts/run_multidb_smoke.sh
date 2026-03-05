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

export SQLITE_DSN="${SQLITE_DSN:-/tmp/tx_fuzzer_smoke.sqlite}"

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
