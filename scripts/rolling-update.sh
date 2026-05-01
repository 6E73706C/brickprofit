#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# rolling-update.sh  –  Update one or more services with zero downtime
#
# Each service is updated one replica at a time (parallelism=1, order=start-first).
# The old container is only removed after the new one passes its healthcheck.
#
# Usage:
#   TAG=v1.2.3 SERVICE=api ./scripts/rolling-update.sh
#   TAG=v1.2.3 SERVICE="api web" ./scripts/rolling-update.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "${ROOT}/.env"

TAG="${TAG:-latest}"
SERVICE="${SERVICE:-api}"

for SVC in $SERVICE; do
    IMAGE="${REGISTRY}/brickprofit-${SVC}:${TAG}"
    echo "▶  Rolling update: brickprofit_${SVC}  →  ${IMAGE}"

    docker service update \
        --image "${IMAGE}" \
        --update-parallelism 1 \
        --update-delay 15s \
        --update-order start-first \
        --update-failure-action rollback \
        "brickprofit_${SVC}"

    echo "✔  brickprofit_${SVC} updated"
done
