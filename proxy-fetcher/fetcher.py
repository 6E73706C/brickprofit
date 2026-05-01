"""
proxy-fetcher – periodically pulls free proxy lists from GitHub and upserts
them into the Cassandra `proxies` table.

Sources (raw GitHub content, updated frequently):
  - TheSpeedX/PROXY-List   → HTTP, SOCKS4, SOCKS5
  - monosans/proxy-list    → HTTP, SOCKS4, SOCKS5
  - clarketm/proxy-list    → HTTP
  - hookzof/socks5_list    → SOCKS5
  - mmpx12/proxy-list      → HTTP, SOCKS4, SOCKS5
"""

import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone

import requests
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3").split(",")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "brickprofit")
CASSANDRA_USER = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL_SECONDS", "1800"))  # default: 30 min

# Resolve JSON-encoded list (matches config.py pattern in the rest of the stack)
if CASSANDRA_HOSTS and CASSANDRA_HOSTS[0].startswith("["):
    import json
    CASSANDRA_HOSTS = json.loads(",".join(CASSANDRA_HOSTS))

# ── Proxy sources ─────────────────────────────────────────────────────────────
SOURCES = [
    # (url, protocol, source_name)
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",   "http",   "TheSpeedX"),
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt", "socks4", "TheSpeedX"),
    ("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt", "socks5", "TheSpeedX"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",   "http",   "monosans"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt", "socks4", "monosans"),
    ("https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt", "socks5", "monosans"),
    ("https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt", "http", "clarketm"),
    ("https://raw.githubusercontent.com/hookzof/socks5_list/master/list.txt",    "socks5", "hookzof"),
    ("https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",   "http",   "mmpx12"),
    ("https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt", "socks4", "mmpx12"),
    ("https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt", "socks5", "mmpx12"),
]

PROXY_RE = re.compile(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$")


# ── Cassandra helpers ─────────────────────────────────────────────────────────
def connect(retries: int = 20, delay: int = 10):
    auth = PlainTextAuthProvider(CASSANDRA_USER, CASSANDRA_PASSWORD)
    for attempt in range(1, retries + 1):
        try:
            cluster = Cluster(
                CASSANDRA_HOSTS,
                auth_provider=auth,
                load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
                protocol_version=4,
            )
            session = cluster.connect(CASSANDRA_KEYSPACE)
            log.info("Connected to Cassandra.")
            return session
        except NoHostAvailable as exc:
            log.warning("Cassandra not ready (attempt %d/%d): %s", attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(delay)


def prepare_statements(session):
    upsert = session.prepare("""
        UPDATE proxies
        SET    last_seen  = ?,
               source     = ?,
               is_active  = true
        WHERE  protocol   = ?
        AND    ip         = ?
        AND    port       = ?
    """)
    insert_new = session.prepare("""
        INSERT INTO proxies (id, ip, port, protocol, source, first_seen, last_seen, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, true)
        IF NOT EXISTS
    """)
    return upsert, insert_new


# ── Fetch / parse ─────────────────────────────────────────────────────────────
def fetch_proxies(url: str, protocol: str, source: str) -> list[tuple]:
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Failed to fetch %s: %s", url, exc)
        return []

    results = []
    for line in resp.text.splitlines():
        line = line.strip()
        m = PROXY_RE.match(line)
        if m:
            ip, port = m.group(1), int(m.group(2))
            results.append((ip, port, protocol, source))
    log.info("  %s / %-6s → %d proxies", source, protocol, len(results))
    return results


# ── Main loop ─────────────────────────────────────────────────────────────────
def run_once(session, upsert_stmt, insert_stmt):
    now = datetime.now(timezone.utc)
    total = 0
    for url, protocol, source in SOURCES:
        for ip, port, proto, src in fetch_proxies(url, protocol, source):
            try:
                # Try lightweight insert first (IF NOT EXISTS)
                result = session.execute(insert_stmt, (
                    uuid.uuid4(), ip, port, proto, src, now, now,
                ))
                if not result.one().applied:
                    # Row exists → update last_seen
                    session.execute(upsert_stmt, (now, src, proto, ip, port))
                total += 1
            except Exception as exc:
                log.debug("Insert error for %s:%s – %s", ip, port, exc)
    log.info("Fetch cycle complete. %d proxies upserted.", total)


def main():
    session = connect()
    upsert_stmt, insert_stmt = prepare_statements(session)

    while True:
        log.info("Starting fetch cycle…")
        try:
            run_once(session, upsert_stmt, insert_stmt)
        except Exception as exc:
            log.error("Fetch cycle failed: %s", exc)
        log.info("Sleeping %d seconds until next cycle.", FETCH_INTERVAL)
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
