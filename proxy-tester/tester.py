"""
proxy-tester – tests every proxy in the `proxies` table by routing a request
through it to https://ifconfig.me/

Rules:
  • proxy connects AND returns a different IP than the server's own IP → GOLDEN
    – row is inserted into `golden_proxies` and deleted from `proxies`
  • proxy fails or leaks the real IP → DELETED from `proxies`

Golden proxies are re-validated on every cycle:
  • still passes  → last_seen updated, stays in `golden_proxies`
  • fails         → deleted from `golden_proxies`
"""

import concurrent.futures
import ipaddress
import logging
import os
import random
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
CASSANDRA_HOSTS   = os.environ.get("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3").split(",")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "brickprofit")
CASSANDRA_USER    = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")
TEST_INTERVAL     = int(os.environ.get("TEST_INTERVAL_SECONDS", "1800"))   # 30 min
TEST_TIMEOUT      = int(os.environ.get("TEST_TIMEOUT_SECONDS", "12"))
MAX_WORKERS       = int(os.environ.get("TEST_MAX_WORKERS", "150"))

# Plain-text IP echo services — rotated randomly to avoid rate-limits.
# All return just the raw IP address with no HTML.
TEST_URLS = [
    "https://ifconfig.me/ip",
    "https://api.ipify.org",
    "https://ipinfo.io/ip",
    "https://icanhazip.com",
    "https://checkip.amazonaws.com",
    "https://api4.my-ip.io/ip.txt",
]

# Comma-separated list of this server's known public IPs (all swarm nodes).
# Set NODE_IPS in the environment for each node so we can detect proxy leakage
# without making any direct outbound connection.
NODE_IPS: set[str] = {
    ip.strip() for ip in os.environ.get("NODE_IPS", "").split(",") if ip.strip()
}
if not NODE_IPS:
    raise RuntimeError(
        "NODE_IPS env var is required (comma-separated public IPs of all swarm nodes). "
        "Example: NODE_IPS=23.88.123.22,178.105.72.175,178.105.76.42"
    )

if CASSANDRA_HOSTS and CASSANDRA_HOSTS[0].startswith("["):
    import json
    CASSANDRA_HOSTS = json.loads(",".join(CASSANDRA_HOSTS))


# ── Cassandra ─────────────────────────────────────────────────────────────────
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
            log.warning("Cassandra not ready (%d/%d): %s", attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(delay)


# ── Helpers ───────────────────────────────────────────────────────────────────
def is_valid_ip(text: str) -> bool:
    """Return True if text is a valid IPv4 or IPv6 address."""
    try:
        ipaddress.ip_address(text.strip())
        return True
    except ValueError:
        return False


# ── Proxy test ────────────────────────────────────────────────────────────────
def test_proxy(protocol: str, ip: str, port: int, own_ip: str) -> bool:
    """
    Returns True only when:
      1. The request through the proxy succeeds; AND
      2. The returned IP is a valid IP and NOT one of this server's known IPs.

    Rotates randomly across TEST_URLS to distribute load and avoid rate-limits.
    Falls back to a second URL if the first returns a non-IP response.
    All connections are made through the proxy under test — no direct connections.
    """
    if protocol == "http":
        proxy_url = f"http://{ip}:{port}"
    elif protocol == "socks4":
        proxy_url = f"socks4://{ip}:{port}"
    elif protocol == "socks5":
        proxy_url = f"socks5://{ip}:{port}"
    else:
        return False

    proxies = {"http": proxy_url, "https": proxy_url}
    headers = {"User-Agent": "curl/7.68.0"}

    # Pick two different URLs so we have a fallback if the first is rate-limited
    candidates = random.sample(TEST_URLS, k=min(2, len(TEST_URLS)))
    for url in candidates:
        try:
            resp = requests.get(
                url,
                proxies=proxies,
                timeout=TEST_TIMEOUT,
                headers=headers,
            )
            returned_ip = resp.text.strip()
            if not is_valid_ip(returned_ip):
                # Service returned HTML/error — try next candidate
                continue
            # Proxy is valid only if the returned IP is not any of our own node IPs
            return returned_ip not in NODE_IPS
        except Exception:
            # Connection/timeout — proxy is bad, no point trying another URL
            return False
    return False


# ── Test cycle ────────────────────────────────────────────────────────────────
def run_once(session) -> None:
    now = datetime.now(timezone.utc)

    # Prepare statements
    insert_golden = session.prepare("""
        INSERT INTO golden_proxies (id, ip, port, protocol, source, first_seen, last_seen, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, true)
    """)
    update_golden_ts = session.prepare("""
        UPDATE golden_proxies SET last_seen = ?
        WHERE protocol = ? AND ip = ? AND port = ?
    """)
    delete_fresh = session.prepare("""
        DELETE FROM proxies WHERE protocol = ? AND ip = ? AND port = ?
    """)
    delete_golden = session.prepare("""
        DELETE FROM golden_proxies WHERE protocol = ? AND ip = ? AND port = ?
    """)

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Test all fresh proxies
    # ──────────────────────────────────────────────────────────────────────────
    fresh = list(session.execute(
        "SELECT protocol, ip, port, id, source, first_seen FROM proxies"
    ))
    log.info("Testing %d fresh proxies (workers=%d, timeout=%ds)…",
             len(fresh), MAX_WORKERS, TEST_TIMEOUT)

    promoted  = 0
    discarded = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(test_proxy, r.protocol, r.ip, r.port): r
            for r in fresh
        }
        for future in concurrent.futures.as_completed(futures):
            row = futures[future]
            try:
                ok = future.result()
            except Exception:
                ok = False

            if ok:
                # Move to golden_proxies
                try:
                    session.execute(insert_golden, (
                        row.id or uuid.uuid4(),
                        row.ip, row.port, row.protocol,
                        row.source or "unknown",
                        row.first_seen or now, now,
                    ))
                    session.execute(delete_fresh, (row.protocol, row.ip, row.port))
                    promoted += 1
                except Exception as exc:
                    log.debug("Promote %s:%s error: %s", row.ip, row.port, exc)
            else:
                # Delete from fresh table
                try:
                    session.execute(delete_fresh, (row.protocol, row.ip, row.port))
                    discarded += 1
                except Exception as exc:
                    log.debug("Delete fresh %s:%s error: %s", row.ip, row.port, exc)

    log.info("Fresh proxies → promoted: %d, discarded: %d", promoted, discarded)

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Re-validate golden proxies
    # ──────────────────────────────────────────────────────────────────────────
    golden = list(session.execute(
        "SELECT protocol, ip, port FROM golden_proxies"
    ))
    log.info("Re-validating %d golden proxies…", len(golden))

    kept    = 0
    removed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(test_proxy, r.protocol, r.ip, r.port): r
            for r in golden
        }
        for future in concurrent.futures.as_completed(futures):
            row = futures[future]
            try:
                ok = future.result()
            except Exception:
                ok = False

            if ok:
                try:
                    session.execute(update_golden_ts, (now, row.protocol, row.ip, row.port))
                    kept += 1
                except Exception as exc:
                    log.debug("Update golden %s:%s error: %s", row.ip, row.port, exc)
            else:
                try:
                    session.execute(delete_golden, (row.protocol, row.ip, row.port))
                    removed += 1
                except Exception as exc:
                    log.debug("Delete golden %s:%s error: %s", row.ip, row.port, exc)

    log.info("Golden proxies → kept: %d, removed: %d", kept, removed)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    log.info("Known node IPs (proxy leak detection): %s", NODE_IPS)
    session = connect()

    while True:
        log.info("═══ Starting test cycle ═══")
        try:
            run_once(session)
        except Exception as exc:
            log.error("Test cycle failed: %s", exc, exc_info=True)
        log.info("Sleeping %ds until next cycle.", TEST_INTERVAL)
        time.sleep(TEST_INTERVAL)


if __name__ == "__main__":
    main()
