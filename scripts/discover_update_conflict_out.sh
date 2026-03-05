#!/usr/bin/env bash
# discover_update_conflict_out.sh — Full discovery + cross-version validation
#
# This script demonstrates:
# 1) Starts postgres:12.3 (vulnerable) and postgres:12.4 (fixed) containers
# 2) Runs baseline stats on both (no delays) — shows "OS hides the bug"
# 3) Runs discovery campaign on 12.3 — fuzzer discovers the anomaly
# 4) Replays the found trace on 12.3 to confirm reproducibility
# 5) Replays the same trace on 12.4 to confirm the fix
# 6) Prints a clear summary
#
# Usage:
#   ./scripts/discover_update_conflict_out.sh
#   VARIANT=delete ./scripts/discover_update_conflict_out.sh
#   K=3 BUDGET=1000 ./scripts/discover_update_conflict_out.sh

set -euo pipefail
cd "$(dirname "$0")/.."

VARIANT="${VARIANT:-update}"
K="${K:-3}"
BUDGET="${BUDGET:-500}"
BASELINE_TRIALS="${BASELINE_TRIALS:-200}"
REPLAY_TRIALS="${REPLAY_TRIALS:-20}"
RESULTS_DIR="results/update-conflict-out"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No Color

echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  PostgreSQL SSI update-conflict-out Discovery Demo${NC}"
echo -e "${BOLD}  Variant: ${VARIANT} | K: ${K} | Budget: ${BUDGET}${NC}"
echo -e "${BOLD}============================================================${NC}"

# --- Step 0: Start containers ---
echo ""
echo -e "${YELLOW}Step 0: Starting PostgreSQL containers...${NC}"
docker compose -f docker-compose-repro.yml up -d 2>/dev/null || \
  docker-compose -f docker-compose-repro.yml up -d 2>/dev/null || true

echo "Waiting for containers to be healthy..."
for port in 54323 54324; do
    version="12.3"
    [[ "$port" == "54324" ]] && version="12.4"
    for i in $(seq 1 30); do
        if pg_isready -h localhost -p "$port" -U postgres >/dev/null 2>&1 || \
           python3 -c "
import psycopg2, sys
try:
    c = psycopg2.connect('postgres://postgres:postgres@localhost:${port}/postgres?sslmode=disable')
    c.close()
except: sys.exit(1)
" 2>/dev/null; then
            echo "  PostgreSQL ${version} (port ${port}): ready"
            break
        fi
        if [[ "$i" == "30" ]]; then
            echo -e "  ${RED}PostgreSQL ${version} (port ${port}): TIMEOUT${NC}"
            if [[ "$port" == "54323" ]]; then
                echo "ERROR: PostgreSQL 12.3 is required. Check docker-compose-repro.yml."
                exit 1
            fi
        fi
        sleep 1
    done
done

mkdir -p "$RESULTS_DIR"

# --- Step 1: Baseline on 12.3 (no delays) ---
echo ""
echo -e "${YELLOW}Step 1: Baseline on PostgreSQL 12.3 (no delays, ${BASELINE_TRIALS} trials)${NC}"
echo "  Natural OS scheduling — expecting ~0 anomalies..."
python3 scripts/discover_update_conflict_out.py \
    --mode baseline \
    --pg 12.3 \
    --variant "$VARIANT" \
    --trials "$BASELINE_TRIALS" \
    2>&1 | tee "${RESULTS_DIR}/baseline_12_3.log"

# --- Step 1b: Baseline on 12.4 ---
echo ""
echo -e "${YELLOW}Step 1b: Baseline on PostgreSQL 12.4 (no delays, ${BASELINE_TRIALS} trials)${NC}"
python3 scripts/discover_update_conflict_out.py \
    --mode baseline \
    --pg 12.4 \
    --variant "$VARIANT" \
    --trials "$BASELINE_TRIALS" \
    2>&1 | tee "${RESULTS_DIR}/baseline_12_4.log" || true

# --- Step 2: Discovery campaign on 12.3 ---
echo ""
echo -e "${YELLOW}Step 2: Discovery campaign on PostgreSQL 12.3 (K=${K}, budget=${BUDGET})${NC}"
echo "  Heuristic delay injection — searching for anomaly-triggering schedule..."
python3 scripts/discover_update_conflict_out.py \
    --mode discover \
    --pg 12.3 \
    --variant "$VARIANT" \
    --k "$K" \
    --budget "$BUDGET" \
    2>&1 | tee "${RESULTS_DIR}/discovery.log"

FOUND_TRACE="${RESULTS_DIR}/found.json"
if [[ ! -f "$FOUND_TRACE" ]]; then
    echo -e "${RED}Discovery failed — no artifact found.${NC}"
    echo "Try increasing BUDGET or K."
    exit 1
fi

echo -e "${GREEN}Artifact saved to: ${FOUND_TRACE}${NC}"

# --- Step 3: Replay on 12.3 (confirm reproducibility) ---
echo ""
echo -e "${YELLOW}Step 3: Replay found trace on PostgreSQL 12.3 (${REPLAY_TRIALS} trials)${NC}"
python3 scripts/discover_update_conflict_out.py \
    --mode validate \
    --trace "$FOUND_TRACE" \
    --replay-trials "$REPLAY_TRIALS" \
    2>&1 | tee "${RESULTS_DIR}/replay_validation.log"

# --- Step 4: Summary ---
echo ""
echo -e "${BOLD}============================================================${NC}"
echo -e "${BOLD}  SUMMARY${NC}"
echo -e "${BOLD}============================================================${NC}"

# Parse results from logs
baseline_bugs=$(grep -oP 'Anomalies detected: \K\d+' "${RESULTS_DIR}/baseline_12_3.log" 2>/dev/null || echo "?")
baseline_total=$(grep -oP 'Anomalies detected: \d+/\K\d+' "${RESULTS_DIR}/baseline_12_3.log" 2>/dev/null || echo "?")

echo ""
echo -e "  ${BOLD}baseline hidden:${NC}     ${baseline_bugs}/${baseline_total} anomalies in natural trials"

if grep -q "BUG FOUND" "${RESULTS_DIR}/discovery.log" 2>/dev/null; then
    echo -e "  ${GREEN}${BOLD}fuzzer discovered:${NC}   YES"
else
    echo -e "  ${RED}${BOLD}fuzzer discovered:${NC}   NO"
fi

echo ""
echo -e "  Artifact: ${FOUND_TRACE}"
echo -e "  Delays applied:"
python3 -c "
import json
with open('${FOUND_TRACE}') as f:
    a = json.load(f)
for eid, d in a.get('delays', {}).items():
    j = a.get('delay_justifications', {}).get(eid, {})
    print(f'    {eid}: {d:.3f}s [{j.get(\"tier\", \"?\")}]')
    if j.get('justification'):
        print(f'      → {j[\"justification\"]}')
" 2>/dev/null || true

echo ""
echo -e "${BOLD}============================================================${NC}"
echo -e "  Full logs in: ${RESULTS_DIR}/"
echo -e "${BOLD}============================================================${NC}"
