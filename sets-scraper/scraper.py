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
IMAGE_DIR          = os.environ.get("LEGO_IMAGES_DIR", "/data/lego-images")

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
      { item_no, description, image_url, large_image_url }

    BrickLink catalog table structure per row (3 <TD> siblings in a <TR>):
      TD[0] – thumbnail <IMG SRC='https://img.bricklink.com/ItemImage/ST/0/ITEM.t1.png'>
      TD[1] – item_no link + optional (Inv) link
      TD[2] – <strong>Set name</strong><FONT ...>parts/year/category</FONT>

    Large image URL: replace /ST/ with /SL/ and strip the .t1 suffix:
      ST/0/10463-1.t1.png  →  SL/0/10463-1.png
    """
    soup = BeautifulSoup(html, "lxml")
    results = []
    seen = set()

    for row in soup.find_all("tr"):
        tds = row.find_all("td", recursive=False)
        if len(tds) < 3:
            continue

        # TD[1]: item_no from catalogitem.page link
        link = tds[1].find("a", href=re.compile(r"catalogitem\.page\?S=", re.I))
        if not link:
            continue
        item_no = link.get_text(strip=True)
        if not item_no or item_no in seen:
            continue
        seen.add(item_no)

        # TD[2]: description from <strong> tag (the set name headline)
        strong = tds[2].find("strong")
        description = strong.get_text(strip=True) if strong else tds[2].get_text(strip=True).split("\n")[0].strip()

        # TD[0]: thumbnail image
        img = tds[0].find("img")
        image_url = ""
        large_image_url = ""
        if img:
            src = img.get("src", "")
            if src.startswith("//"):
                src = "https:" + src
            image_url = src
            # Derive large image: ST/0/ITEM.t1.png → SL/0/ITEM.png
            large_image_url = src.replace("/ItemImage/ST/", "/ItemImage/SL/").replace(".t1.png", ".png")

        results.append({
            "item_no": item_no,
            "description": description,
            "image_url": image_url,
            "large_image_url": large_image_url,
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


# ── Image download ───────────────────────────────────────────────────────────
def download_image(item: dict) -> None:
    """Download the large image for a set to IMAGE_DIR if not already cached."""
    large_url = item.get("large_image_url", "")
    item_no   = item.get("item_no", "")
    if not large_url or not item_no:
        return
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", item_no) + ".png"
    dest = os.path.join(IMAGE_DIR, safe_name)
    if os.path.exists(dest):
        return  # already downloaded
    os.makedirs(IMAGE_DIR, exist_ok=True)
    tmp = dest + ".tmp"
    try:
        resp = requests.get(large_url, headers=HEADERS, timeout=15, stream=True)
        resp.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        os.replace(tmp, dest)
        log.debug("Downloaded image for %s", item_no)
    except Exception as exc:
        log.warning("Failed to download image for %s: %s", item_no, exc)
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass


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
        result = session.execute(
            """
            INSERT INTO lego_sets (year, item_no, description, image_url, first_seen, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (year, item["item_no"], item["description"], item["image_url"], now, now),
        )
        if not result.one().applied:
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
            download_image(item)
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
