-- ─────────────────────────────────────────────────────────────────────────────
-- BrickProfit – Cassandra Schema
-- Safe to run multiple times (IF NOT EXISTS guards on everything)
-- ─────────────────────────────────────────────────────────────────────────────

-- Keyspace: DC name must match Cassandra's reported DC (default: "datacenter1")
CREATE KEYSPACE IF NOT EXISTS brickprofit
    WITH replication = {
        'class': 'NetworkTopologyStrategy',
        'datacenter1': 3
    }
    AND durable_writes = true;

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

-- ─── Proxies ─────────────────────────────────────────────────────────────────
-- Partition by protocol so queries like "give me 100 HTTP proxies" are fast.
CREATE TABLE IF NOT EXISTS proxies (
    protocol   TEXT,
    ip         TEXT,
    port       INT,
    id         UUID,
    source     TEXT,
    first_seen TIMESTAMP,
    last_seen  TIMESTAMP,
    is_active  BOOLEAN,
    PRIMARY KEY (protocol, ip, port)
);

-- ─── Golden Proxies ────────────────────────────────────────────────────────────
-- Verified working proxies promoted from `proxies` by the proxy-tester service.
CREATE TABLE IF NOT EXISTS golden_proxies (
    protocol   TEXT,
    ip         TEXT,
    port       INT,
    id         UUID,
    source     TEXT,
    first_seen TIMESTAMP,
    last_seen  TIMESTAMP,
    is_active  BOOLEAN,
    PRIMARY KEY (protocol, ip, port)
);

-- ─── Sessions / tokens ───────────────────────────────────────────────────────
-- "token" is a reserved CQL keyword – must be double-quoted
CREATE TABLE IF NOT EXISTS sessions (
    "token"    TEXT,
    user_id    UUID,
    expires_at TIMESTAMP,
    PRIMARY KEY ("token")
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
