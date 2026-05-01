"""
Background thread that queries Cassandra every 60 s and updates
Prometheus gauges for scraped LEGO set counts (total + per year).
Starts automatically when the Flask app initialises via start_collector().
"""

import logging
import threading
import time

from prometheus_client import Gauge

log = logging.getLogger(__name__)

# ── Prometheus gauges ────────────────────────────────────────────────────────
SETS_TOTAL = Gauge(
    "brickprofit_lego_sets_total",
    "Total number of LEGO sets scraped from BrickLink",
)
SETS_BY_YEAR = Gauge(
    "brickprofit_lego_sets_count",
    "Number of LEGO sets scraped for a given year",
    ["year"],
)
YEARS_COVERED = Gauge(
    "brickprofit_lego_sets_years_covered",
    "Number of distinct years that have at least one scraped LEGO set",
)

_INTERVAL = 60   # seconds between Cassandra queries
_started  = False
_lock     = threading.Lock()


def _collect_once() -> None:
    try:
        from app.db import get_session
        session = get_session()

        rows = session.execute(
            "SELECT year, COUNT(*) AS cnt FROM lego_sets GROUP BY year"
        )

        total = 0
        years = 0
        for r in rows:
            count = int(r.cnt)
            SETS_BY_YEAR.labels(year=str(r.year)).set(count)
            total += count
            years += 1

        SETS_TOTAL.set(total)
        YEARS_COVERED.set(years)

    except Exception as exc:
        log.warning("lego_metrics: collection failed: %s", exc)


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
    t = threading.Thread(target=_loop, name="lego-metrics-collector", daemon=True)
    t.start()
    log.info("lego_metrics: collector started (interval=%ds)", _INTERVAL)
