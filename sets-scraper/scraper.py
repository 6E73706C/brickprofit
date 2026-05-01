"""
sets-scraper – scrapes BrickLink catalog for LEGO sets by year.

For each year in the range  [current_year - 5 … current_year + 1]  it fetches:
  https://www.bricklink.com/catalogList.asp?catType=S&itemYear=YEAR

All HTTP requests are routed through a randomly-chosen golden proxy from
Cassandra.  Scraped rows (item_no, description, image_url, year) are upserted
into the `lego_sets` table.

Cycle behaviour:
  - One full pass over all years = one cycle.
  - After a complete cycle the scraper sleeps SCRAPE_INTERVAL_SECONDS before
    starting the next one (default 3 600 s / 1 hour) to avoid hammering BrickLink.
  - Items already in the DB just get their last_seen timestamp refreshed.
"""

import logging
import os
import random
import re
import time
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
CASSANDRA_HOSTS    = os.environ.get("CASSANDRA_HOSTS", "cassandra1,cassandra2,cassandra3").split(",")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "brickprofit")
CASSANDRA_USER     = os.environ.get("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")

SCRAPE_INTERVAL    = int(os.environ.get("SCRAPE_INTERVAL_SECONDS", "3600"))   # 1 h between full cycles
REQUEST_TIMEOUT    = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "30"))
DELAY_BETWEEN_PAGES = float(os.environ.get("DELAY_BETWEEN_PAGES_SECONDS", "3"))  # polite crawl delay

BRICKLINK_BASE = "https://www.bricklink.com/catalogList.asp"

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


def get_golden_proxy(session) -> dict | None:
    """Pick a random golden proxy from Cassandra. Returns a requests proxies dict or None."""
    try:
        rows = list(session.execute(
            "SELECT protocol, ip, port FROM golden_proxies"
        ))
        if not rows:
            log.warning("No golden proxies available – requests will go direct.")
            return None
        row = random.choice(rows)
        if row.protocol == "http":
            proxy_url = f"http://{row.ip}:{row.port}"
        elif row.protocol == "socks4":
            proxy_url = f"socks4://{row.ip}:{row.port}"
        elif row.protocol == "socks5":
            proxy_url = f"socks5://{row.ip}:{row.port}"
        else:
            return None
        log.debug("Using proxy %s (%s)", proxy_url, row.protocol)
        return {"http": proxy_url, "https": proxy_url}
    except Exception as exc:
        log.warning("Failed to fetch golden proxy: %s", exc)
        return None


# ── Year range ────────────────────────────────────────────────────────────────
def target_years() -> list[int]:
    now = datetime.now(timezone.utc).year
    return list(range(now - 5, now + 2))   # last 5 years + current + next


# ── Scraping ──────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def fetch_page(year: int, pg: int, proxies: dict | None, session) -> str | None:
    """Fetch one catalog page.  Retries up to 3 times, rotating proxy on each retry."""
    params = {
        "catType": "S",
        "itemYear": str(year),
        "pg": str(pg),
    }
    for attempt in range(1, 4):
        try:
            resp = requests.get(
                BRICKLINK_BASE,
                params=params,
                headers=HEADERS,
                proxies=proxies,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            log.warning("Fetch year=%d pg=%d attempt %d/3 failed: %s", year, pg, attempt, exc)
            # Rotate to a fresh proxy on retry
            proxies = get_golden_proxy(session)
            time.sleep(2)
    return None


def parse_sets(html: str) -> list[dict]:
    """
    Parse catalog list HTML and return a list of dicts:
      { item_no, description, image_url }

    BrickLink's catalog table has rows like:
      <TR> <TD> <IMG src="//img.bricklink.com/..."> </TD>
           <TD> <B><A href="...">ITEM-NO</A></B><BR>Description text </TD>
      </TR>
    We handle both the legacy table layout and the newer div-based layout.
    """
    soup = BeautifulSoup(html, "lxml")
    results = []

    # ── Strategy 1: table rows with catalog item links ────────────────────
    # BrickLink item links follow: /v2/catalog/catalogitem.page?S=ITEMNO
    item_links = soup.find_all("a", href=re.compile(r"catalogitem\.page\?S=", re.I))
    seen = set()
    for link in item_links:
        item_no = link.get_text(strip=True)
        if not item_no or item_no in seen:
            continue
        seen.add(item_no)

        # Description: sibling text after the <A> or in the parent <TD>
        parent_td = link.find_parent("td")
        if parent_td:
            # Strip the item_no part; remaining text is description
            full_text = parent_td.get_text(separator=" ", strip=True)
            description = full_text.replace(item_no, "", 1).strip(" -:")
        else:
            description = ""

        # Image: look in adjacent <TD> or parent <TR>
        parent_row = link.find_parent("tr")
        image_url = ""
        if parent_row:
            img = parent_row.find("img")
            if img:
                src = img.get("src", "")
                if src.startswith("//"):
                    src = "https:" + src
                image_url = src

        results.append({
            "item_no": item_no,
            "description": description,
            "image_url": image_url,
        })

    return results


def has_next_page(html: str, current_pg: int) -> bool:
    """Return True if there is a next page link on the catalog list page."""
    soup = BeautifulSoup(html, "lxml")
    # BrickLink uses "pg=N" links; look for a link to pg+1
    next_pg = str(current_pg + 1)
    for a in soup.find_all("a", href=re.compile(r"pg=" + re.escape(next_pg))):
        return True
    return False


# ── Cassandra writes ──────────────────────────────────────────────────────────
def prepare_statements(session):
    insert = session.prepare("""
        INSERT INTO lego_sets (year, item_no, description, image_url, first_seen, last_seen)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    update_ts = session.prepare("""
        UPDATE lego_sets SET last_seen = ?, description = ?, image_url = ?
        WHERE year = ? AND item_no = ?
    """)
    return insert, update_ts


def upsert_set(session, insert_stmt, update_stmt, year: int, item: dict, now: datetime):
    """Insert or update a lego_set row."""
    try:
        # Try insert with IF NOT EXISTS semantics via Cassandra LWT
        result = session.execute(
            """
            INSERT INTO lego_sets (year, item_no, description, image_url, first_seen, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (year, item["item_no"], item["description"], item["image_url"], now, now),
        )
        if not result.one().applied:
            # Row already exists, just refresh last_seen + possibly improved data
            session.execute(
                update_stmt,
                (now, item["description"], item["image_url"], year, item["item_no"]),
            )
    except Exception as exc:
        log.debug("Upsert error %s/%s: %s", year, item["item_no"], exc)


# ── Main scrape cycle ─────────────────────────────────────────────────────────
def scrape_year(session, update_stmt, year: int) -> int:
    """Scrape all pages for a given year. Returns number of sets processed."""
    proxy = get_golden_proxy(session)
    total = 0
    pg = 1

    while True:
        log.info("  Fetching year=%d page=%d …", year, pg)
        html = fetch_page(year, pg, proxy, session)
        if not html:
            log.warning("  Failed to fetch year=%d page=%d – skipping.", year, pg)
            break

        sets = parse_sets(html)
        log.info("  year=%d page=%d → %d sets found", year, pg, len(sets))

        if not sets:
            break

        now = datetime.now(timezone.utc)
        for item in sets:
            upsert_set(session, None, update_stmt, year, item, now)
            total += 1

        if not has_next_page(html, pg):
            break

        pg += 1
        time.sleep(DELAY_BETWEEN_PAGES)  # polite delay before next page

    return total


def run_once(session, update_stmt) -> None:
    years = target_years()
    log.info("Starting scrape cycle for years: %s", years)
    grand_total = 0
    for year in years:
        count = scrape_year(session, update_stmt, year)
        log.info("  year=%d → %d sets upserted", year, count)
        grand_total += count
        time.sleep(DELAY_BETWEEN_PAGES)
    log.info("Cycle complete. %d total set records upserted.", grand_total)


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    session = connect()
    _, update_stmt = prepare_statements(session)

    while True:
        log.info("═══ Starting LEGO scrape cycle ═══")
        try:
            run_once(session, update_stmt)
        except Exception as exc:
            log.error("Scrape cycle failed: %s", exc, exc_info=True)
        log.info("Sleeping %ds until next cycle.", SCRAPE_INTERVAL)
        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
