set -euo pipefail

# Simple experiment loop:
#  1. Run a random workload via the Go runner (or your own Python wrapper),
#     which produces logs/trace_latest.json.
#  2. Replay that trace deterministically with an injected delay.

WORKLOAD="${1:-workloads/example.yaml}"

echo "[run_fuzz_replay] running random workload: ${WORKLOAD}"

python scripts/run_random.py --workload "${WORKLOAD}"

echo "[run_fuzz_replay] replaying latest trace with injected delay..."
python scripts/replay_litmus.py \
  --trace logs/trace_latest.json \
  --inject-delay-at-step 5 \
  --delay-seconds 0.1

echo "[run_fuzz_replay] done."

