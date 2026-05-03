#!/bin/sh
# Resolve ${NODE0_HOST}, ${NODE1_HOST}, ${NODE2_HOST} placeholders in the
# Prometheus config template at container startup.  This ensures the correct
# physical IPs are always used regardless of which Swarm node the container
# is scheduled on.
set -e

NODE0="${NODE0_HOST:-}"
NODE1="${NODE1_HOST:-}"
NODE2="${NODE2_HOST:-}"

sed \
  "s|\${NODE0_HOST}|${NODE0}|g;\
   s|\${NODE1_HOST}|${NODE1}|g;\
   s|\${NODE2_HOST}|${NODE2}|g" \
  /etc/prometheus/prometheus.yml.tmpl > /tmp/prometheus.yml

exec /bin/prometheus "$@"
