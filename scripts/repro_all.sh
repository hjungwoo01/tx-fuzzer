#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE_FILE="$ROOT/docker-compose-multidb.yml"

docker compose -f "$COMPOSE_FILE" up -d

wait_healthy() {
  local cname="$1"
  local retries="${2:-120}"
  local i=0
  while (( i < retries )); do
    local status
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "$cname" 2>/dev/null || true)"
    if [[ "$status" == "healthy" ]]; then
      echo "[$cname] healthy"
      return 0
    fi
    sleep 2
    (( i += 1 ))
  done
  echo "Timed out waiting for container health: $cname" >&2
  return 1
}

wait_healthy txf_pg12_3
wait_healthy txf_pg12_4
wait_healthy txf_pg16_2
wait_healthy txf_mysql5_7_8
wait_healthy txf_mysql5_7_9
wait_healthy txf_mysql8_0_34

scripts/experiments/run_multidb_quick.sh "$@"

