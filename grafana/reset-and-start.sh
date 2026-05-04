#!/bin/sh
set -e

# ─────────────────────────────────────────────────────────────────────────────
# Force the admin password to always match GF_SECURITY_ADMIN_PASSWORD, even
# when Grafana already has an existing database.  Without this, every deploy
# that recreates the container leaves the stored password hash unchanged while
# the env-var value may differ, causing "Invalid username or password".
# ─────────────────────────────────────────────────────────────────────────────
DB_PATH="${GF_PATHS_DATA:-/var/lib/grafana}/grafana.db"

if [ -f "$DB_PATH" ]; then
    echo "[grafana-wrapper] Existing DB found – resetting admin password from GF_SECURITY_ADMIN_PASSWORD"
    grafana cli \
        --homepath /usr/share/grafana \
        admin reset-admin-password \
        "${GF_SECURITY_ADMIN_PASSWORD:-admin}" 2>&1 || true
fi

exec /run.sh "$@"
