#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# init-swarm.sh  –  Bootstrap a new Docker Swarm cluster
#
# Run this ONCE on the first manager node.
# Then copy the printed join-token commands and run them on worker nodes.
#
# Node labels are used to pin Cassandra nodes to specific servers so that
# each Cassandra instance has its own dedicated disk volume.
#
# Usage:
#   ADVERTISE_ADDR=192.168.1.10 ./scripts/init-swarm.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

ADVERTISE_ADDR="${ADVERTISE_ADDR:?'Set ADVERTISE_ADDR=<manager-IP>'}"

echo "▶  Initialising Docker Swarm"
docker swarm init --advertise-addr "${ADVERTISE_ADDR}"

echo ""
echo "▶  Labelling THIS node as cassandra=node1"
SELF=$(docker node inspect self --format '{{ .ID }}')
docker node update --label-add cassandra=node1 "${SELF}"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Next steps:"
echo ""
echo "  1. Join other nodes as MANAGERS (all nodes are managers AND workers):"
docker swarm join-token manager
echo ""
echo "  2. After nodes join, promote them (if they joined as worker accidentally):"
echo "     docker node promote <node-id>"
echo ""
echo "  NOTE: Use an odd number of managers (3, 5, 7) for Raft quorum."
echo "        With 3 managers you can lose 1; with 5 you can lose 2."
echo ""
echo "  3. After managers join, label them for Cassandra placement:"
echo "     docker node update --label-add cassandra=node2 <node-2-id>"
echo "     docker node update --label-add cassandra=node3 <node-3-id>"
echo ""
echo "  3. Copy .env.example to .env and fill in secrets"
echo ""
echo "  4. Deploy:"
echo "     TAG=v1.0.0 ./scripts/deploy.sh"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
