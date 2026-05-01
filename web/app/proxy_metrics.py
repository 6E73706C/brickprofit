"""
Background thread that queries Cassandra every 60 s and updates
Prometheus gauges for proxy counts.  Starts automatically when the
Flask app initialises via start_collector().
"""

import logging
import threading
import time

from prometheus_client import Gauge

log = logging.getLogger(__name__)

# ── Prometheus gauges ────────────────────────────────────────────────────────
FRESH_COUNT = Gauge(
    "brickprofit_proxy_fresh_count",
    "Number of fresh (untested) proxies in the proxies table",
    ["protocol"],
)
GOLDEN_COUNT = Gauge(
    "brickprofit_proxy_golden_count",
    "Number of verified working proxies in the golden_proxies table",
    ["protocol"],
)
FRESH_TOTAL = Gauge(
    "brickprofit_proxy_fresh_total",
    "Total number of fresh proxies across all protocols",
)
GOLDEN_TOTAL = Gauge(
    "brickprofit_proxy_golden_total",
    "Total number of golden proxies across all protocols",
)

_PROTOCOLS = ["http", "socks4", "socks5"]
_INTERVAL  = 60   # seconds between Cassandra queries
_started   = False
_lock      = threading.Lock()


def _collect_once() -> None:
    try:
        from app.db import get_session
        session = get_session()

        # ── Fresh proxies ────────────────────────────────────────────────────
        fresh_rows = session.execute(
            "SELECT protocol, COUNT(*) AS cnt FROM proxies GROUP BY protocol"
        )
        fresh_by_proto = {r.protocol: int(r.cnt) for r in fresh_rows}

        # ── Golden proxies ───────────────────────────────────────────────────
        try:
            golden_rows = session.execute(
                "SELECT protocol, COUNT(*) AS cnt FROM golden_proxies GROUP BY protocol"
            )
            golden_by_proto = {r.protocol: int(r.cnt) for r in golden_rows}
        except Exception:
            golden_by_proto = {}

        # Update per-protocol gauges
        for proto in _PROTOCOLS:
            FRESH_COUNT.labels(protocol=proto).set(fresh_by_proto.get(proto, 0))
            GOLDEN_COUNT.labels(protocol=proto).set(golden_by_proto.get(proto, 0))

        # Update totals
        FRESH_TOTAL.set(sum(fresh_by_proto.values()))
        GOLDEN_TOTAL.set(sum(golden_by_proto.values()))

    except Exception as exc:
        log.warning("proxy_metrics: collection failed: %s", exc)


def _loop() -> None:
    while True:
        _collect_once()
        time.sleep(_INTERVAL)


def start_collector() -> None:
    """Start the background collector thread (idempotent)."""
    global _started
    with _lock:
        if _started:
            return
        _started = True
    t = threading.Thread(target=_loop, name="proxy-metrics-collector", daemon=True)
    t.start()
    log.info("proxy_metrics: collector started (interval=%ds)", _INTERVAL)
