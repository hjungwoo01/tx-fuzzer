#!/usr/bin/env bash
# repro_update_conflict_out.sh — One-liner reproduction of the PostgreSQL SSI
# "update-conflict-out" bug across postgres:12.3 (vulnerable) and 12.4 (fixed).
#
# Usage:
#   ./scripts/repro_update_conflict_out.sh          # full comparison
#   ./scripts/repro_update_conflict_out.sh --pg 12.3 # single version
#   ./scripts/repro_update_conflict_out.sh --k-bounded 2  # schedule search
#
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_FILE="docker-compose-repro.yml"
SCRIPT="scripts/repro_update_conflict_out.py"

# Detect python
PYTHON="${PYTHON:-python3}"
if command -v "${PYTHON}" &>/dev/null; then
    :
elif command -v python &>/dev/null; then
    PYTHON=python
else
    echo "ERROR: python3 not found" >&2
    exit 1
fi

# If running in compare mode (default when no args), start docker containers.
if [[ $# -eq 0 ]] || [[ "${1:-}" == "--compare" ]]; then
    echo "=== Starting PostgreSQL 12.3 + 12.4 containers ==="
    docker compose -f "$COMPOSE_FILE" up -d --wait 2>/dev/null || \
        docker-compose -f "$COMPOSE_FILE" up -d 2>/dev/null || \
        echo "WARNING: could not start containers; assuming they are already running."
    echo ""
    echo "=== Running SSI update-conflict-out reproduction ==="
    exec "$PYTHON" "$SCRIPT" --compare --wait 60 "$@"
else
    # Pass all arguments through to the Python script
    exec "$PYTHON" "$SCRIPT" "$@"
fi
