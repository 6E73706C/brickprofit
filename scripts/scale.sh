#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scale.sh  –  Scale a service up or down
#
# Usage:
#   SERVICE=api REPLICAS=6 ./scripts/scale.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SERVICE="${SERVICE:-api}"
REPLICAS="${REPLICAS:-3}"

docker service scale "brickprofit_${SERVICE}=${REPLICAS}"
echo "✔  brickprofit_${SERVICE} scaled to ${REPLICAS} replicas"
