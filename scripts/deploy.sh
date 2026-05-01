#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# deploy.sh  –  Build, push, and deploy all services to Docker Swarm
#
# Usage:
#   TAG=v1.2.3 ./scripts/deploy.sh
#   TAG=v1.2.3 SERVICES="api web" ./scripts/deploy.sh   # deploy subset
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT}/.env"

TAG="${TAG:-latest}"
SERVICES="${SERVICES:-nginx api web worker}"   # space-separated list

echo "▶  Building images (tag=${TAG})"
for SVC in $SERVICES; do
    echo "   Building ${SVC}..."
    docker build \
        --tag "${REGISTRY}/brickprofit-${SVC}:${TAG}" \
        --tag "${REGISTRY}/brickprofit-${SVC}:latest" \
        "${ROOT}/${SVC}"
done

echo "▶  Pushing images"
for SVC in $SERVICES; do
    docker push "${REGISTRY}/brickprofit-${SVC}:${TAG}"
    docker push "${REGISTRY}/brickprofit-${SVC}:latest"
done

echo "▶  Deploying stack to Swarm"
TAG="${TAG}" docker stack deploy \
    --compose-file "${ROOT}/docker-stack.yml" \
    --with-registry-auth \
    brickprofit

echo "✔  Deploy complete  (tag=${TAG})"
