#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# drain-node.sh  –  Safely drain a node for maintenance / OS updates
#
# Workflow:
#   1. Drain the node  → Swarm reschedules its containers on other nodes
#   2. (you) do your maintenance / reboot the server
#   3. Re-activate the node when ready
#
# Usage:
#   NODE=worker-2 ./scripts/drain-node.sh
#   NODE=worker-2 ./scripts/drain-node.sh --reactivate
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

NODE="${NODE:?'Set NODE=<node-hostname>'}"

if [[ "${1:-}" == "--reactivate" ]]; then
    echo "▶  Reactivating node: ${NODE}"
    docker node update --availability active "${NODE}"
    echo "✔  ${NODE} is active again"
else
    echo "▶  Draining node: ${NODE}  (containers will be rescheduled)"
    docker node update --availability drain "${NODE}"
    echo ""
    echo "   Node is draining.  Wait for containers to reschedule, then"
    echo "   perform your maintenance and run:"
    echo "   NODE=${NODE} ./scripts/drain-node.sh --reactivate"
fi
