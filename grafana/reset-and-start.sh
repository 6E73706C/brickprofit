#!/bin/sh
set -e

# ─────────────────────────────────────────────────────────────────────────────
# This is the SOLE mechanism that syncs the admin password before Grafana
# starts.  It must complete successfully before exec /run.sh is called.
#
# GF_SECURITY_ADMIN_PASSWORD MUST be set — the container will refuse to start
# (and Swarm will restart it) rather than silently use a wrong password.
# ─────────────────────────────────────────────────────────────────────────────

: "${GF_SECURITY_ADMIN_PASSWORD:?GF_SECURITY_ADMIN_PASSWORD env var must be set in the service spec}"

DB_PATH="${GF_PATHS_DATA:-/var/lib/grafana}/grafana.db"

if [ -f "$DB_PATH" ]; then
    echo "[grafana-wrapper] Existing DB found – resetting admin password from GF_SECURITY_ADMIN_PASSWORD"
    i=1
    while [ "$i" -le 5 ]; do
        if grafana cli \
            --homepath /usr/share/grafana \
            admin reset-admin-password \
            --direct \
            "$GF_SECURITY_ADMIN_PASSWORD" 2>&1; then
            echo "[grafana-wrapper] Password reset successful (attempt $i)."
            break
        fi
        echo "[grafana-wrapper] Password reset attempt $i/5 failed, retrying in 3s..."
        sleep 3
        i=$((i + 1))
    done
    if [ "$i" -gt 5 ]; then
        echo "[grafana-wrapper] ERROR: All 5 password reset attempts failed – exiting so Swarm can restart."
        exit 1
    fi
else
    echo "[grafana-wrapper] No existing DB – Grafana will initialise with GF_SECURITY_ADMIN_PASSWORD on first start."
fi

exec /run.sh "$@"
