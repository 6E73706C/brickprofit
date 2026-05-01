#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh  –  Build locally and deploy all services to Docker Swarm
#
# Usage:
#   ./scripts/deploy.sh
#   SERVICES="api web" ./scripts/deploy.sh   # deploy subset
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT}/.env"

SERVICES="${SERVICES:-nginx api web worker}"   # space-separated list

echo "▶  Building images locally"
for SVC in $SERVICES; do
    echo "   Building ${SVC}..."
    docker build \
        --tag "brickprofit-${SVC}:latest" \
        "${ROOT}/${SVC}"
done

echo "▶  Deploying stack to Swarm"
docker stack deploy \
    --compose-file "${ROOT}/docker-stack.yml" \
    --resolve-image never \
    brickprofit

echo "✔  Deploy complete"
