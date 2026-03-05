#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/results/multidb_quick}"

python3 scripts/experiments/run_multidb_suite.py \
  --profile quick \
  --output-dir "$OUT_DIR" \
  "$@"

