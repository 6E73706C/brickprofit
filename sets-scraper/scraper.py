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
DELAY_BETWEEN_PAGES = float(os.environ.get("DELAY_BETWEEN_PAGES_SECONDS", "5"))  # polite crawl delay
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


def pick_proxy(session) -> tuple[object, dict]:
    """
    Pick a random golden proxy from Cassandra.
    Returns (row, proxies_dict).  Raises RuntimeError if the table is empty.
    """
    rows = list(session.execute("SELECT protocol, ip, port FROM golden_proxies"))
    if not rows:
        raise RuntimeError("No golden proxies available – refusing to go direct.")
    row = random.choice(rows)
    if row.protocol == "http":
        proxy_url = f"http://{row.ip}:{row.port}"
    elif row.protocol == "socks4":
        proxy_url = f"socks4://{row.ip}:{row.port}"
    elif row.protocol == "socks5":
        proxy_url = f"socks5://{row.ip}:{row.port}"
    else:
        raise RuntimeError(f"Unknown proxy protocol: {row.protocol}")
    log.debug("Using proxy %s:%s (%s)", row.ip, row.port, row.protocol)
    return row, {"http": proxy_url, "https": proxy_url}


def drop_proxy(session, row) -> None:
    """Remove a bad proxy from Cassandra so it is never used again."""
    try:
        session.execute(
            "DELETE FROM golden_proxies WHERE protocol = %s AND ip = %s AND port = %s",
            (row.protocol, row.ip, row.port),
        )
        log.info("Dropped dead proxy %s:%s from golden_proxies.", row.ip, row.port)
    except Exception as exc:
        log.warning("Failed to drop proxy %s:%s: %s", row.ip, row.port, exc)


def fetch_with_proxy(url: str, session, *, params: dict | None = None,
                     stream: bool = False, retries: int = 5) -> requests.Response:
    """
    GET *url* through a golden proxy.
    Each attempt uses a freshly-picked proxy (rotates every try).
    On any failure the offending proxy is deleted from the DB and the next
    proxy is tried.  Raises RuntimeError if all retries fail or no proxies
    are left.  Never falls back to a direct connection.
    """
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        proxy_row, proxies = pick_proxy(session)  # raises if table empty
        try:
            resp = requests.get(
                url,
                params=params,
                headers=HEADERS,
                proxies=proxies,
                timeout=REQUEST_TIMEOUT,
                stream=stream,
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            log.warning("Request attempt %d/%d via %s:%s failed: %s",
                        attempt, retries, proxy_row.ip, proxy_row.port, exc)
            drop_proxy(session, proxy_row)
            last_exc = exc
            time.sleep(3 * attempt)  # back-off: 3s, 6s, 9s …
    raise RuntimeError(f"All {retries} proxy attempts failed for {url}") from last_exc


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


def fetch_page(year: int, pg: int, session) -> str | None:
    """Fetch one catalog page via a golden proxy.  Returns HTML or None on failure."""
    params = {
        "catType": "S",
        "itemYear": str(year),
        "pg": str(pg),
    }
    try:
        resp = fetch_with_proxy(BRICKLINK_BASE, session, params=params, retries=3)
        return resp.text
    except RuntimeError as exc:
        log.warning("fetch_page year=%d pg=%d: %s", year, pg, exc)
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


def get_total_pages(html: str) -> int:
    """
    Parse the highest page number from BrickLink's pagination links.
    Returns 1 if no pagination is found (single page).
    """
    soup = BeautifulSoup(html, "lxml")
    max_pg = 1
    for a in soup.find_all("a", href=re.compile(r"[?&]pg=(\d+)")):
        m = re.search(r"[?&]pg=(\d+)", a.get("href", ""))
        if m:
            pg = int(m.group(1))
            if pg > max_pg:
                max_pg = pg
    return max_pg


# ── Image download ───────────────────────────────────────────────────────────
def download_image(item: dict, session) -> None:
    """Download the large image for a set to IMAGE_DIR via a golden proxy."""
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
        resp = fetch_with_proxy(large_url, session, stream=True, retries=3)
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        os.replace(tmp, dest)
        log.debug("Downloaded image for %s", item_no)
    except RuntimeError as exc:
        log.warning("Failed to download image for %s: %s", item_no, exc)
    finally:
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
    total = 0

    # Fetch page 1 first to learn the total number of pages
    log.info("  Fetching year=%d page=1 (discovering total pages) …", year)
    html = fetch_page(year, 1, session)
    if not html:
        log.warning("  Failed to fetch year=%d page=1 – skipping year.", year)
        return 0

    total_pages = get_total_pages(html)
    log.info("  year=%d → %d total pages", year, total_pages)

    for pg in range(1, total_pages + 1):
        if pg > 1:
            log.info("  Fetching year=%d page=%d/%d …", year, pg, total_pages)
            html = fetch_page(year, pg, session)
            if not html:
                log.warning("  Failed to fetch year=%d page=%d – skipping page.", year, pg)
                time.sleep(DELAY_BETWEEN_PAGES)
                continue  # skip this page but keep going with the rest

        sets = parse_sets(html)
        log.info("  year=%d page=%d/%d → %d sets found", year, pg, total_pages, len(sets))

        now = datetime.now(timezone.utc)
        for item in sets:
            upsert_set(session, None, update_stmt, year, item, now)
            download_image(item, session)
            total += 1

        if pg < total_pages:
            time.sleep(DELAY_BETWEEN_PAGES)

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
