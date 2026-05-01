-- ─────────────────────────────────────────────────────────────────────────────
-- BrickProfit – Cassandra Schema (CQL – SQL-compatible syntax)
-- Run once on first boot via cassandra1 init script
-- ─────────────────────────────────────────────────────────────────────────────

-- Create application user (run as superuser)
CREATE ROLE IF NOT EXISTS brickprofit WITH PASSWORD = 'pXxgsHRxfTQLPdRQn3VMFEZ5Y97qjFKW7UKjgNgXi7w' AND LOGIN = true;

-- Keyspace: NetworkTopologyStrategy gives per-datacenter replication
-- RF=3 means every row is stored on 3 nodes → survive 1 node failure
CREATE KEYSPACE IF NOT EXISTS brickprofit
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'dc1': 3
    }
    AND durable_writes = true;

GRANT ALL PERMISSIONS ON KEYSPACE brickprofit TO brickprofit;

USE brickprofit;

-- ─── Users ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id          UUID,
    email       TEXT,
    username    TEXT,
    password_hash TEXT,
    is_active   BOOLEAN,
    created_at  TIMESTAMP,
    PRIMARY KEY (id)
);

-- Secondary-index style: look up by email
CREATE TABLE IF NOT EXISTS users_by_email (
    email       TEXT,
    id          UUID,
    PRIMARY KEY (email)
);

-- ─── Items ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS items (
    id          UUID,
    name        TEXT,
    description TEXT,
    owner_id    UUID,
    created_at  TIMESTAMP,
    PRIMARY KEY (id)
);

-- Partition by owner for efficient "items by user" queries
CREATE TABLE IF NOT EXISTS items_by_owner (
    owner_id    UUID,
    created_at  TIMESTAMP,
    id          UUID,
    name        TEXT,
    PRIMARY KEY (owner_id, created_at, id)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- ─── Sessions / tokens ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    token       TEXT,
    user_id     UUID,
    expires_at  TIMESTAMP,
    PRIMARY KEY (token)
) WITH default_time_to_live = 86400;   -- auto-expire after 24 h

-- ─── Audit log (append-only, time-series pattern) ────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    year_month  TEXT,           -- partition key keeps partitions bounded
    created_at  TIMESTAMP,
    id          UUID,
    user_id     UUID,
    action      TEXT,
    resource    TEXT,
    details     TEXT,
    PRIMARY KEY (year_month, created_at, id)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 7
  };
