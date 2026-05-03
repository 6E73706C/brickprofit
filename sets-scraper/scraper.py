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


def is_valid_catalog_html(resp) -> bool:
    """
    Return False if BrickLink returned an error/captcha page instead of real
    catalog data.  Called inside fetch_with_proxy before the response is
    accepted, so a bad proxy can be dropped and a fresh one retried.
    """
    # Only inspect text responses (not streaming image downloads)
    content_type = resp.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        return True
    snippet = resp.text[:4096]
    if "<title>BrickLink Error</title>" in snippet:
        log.warning("BrickLink returned error page (proxy probably blocked).")
        return False
    if "catalogitem" not in snippet and "catType" not in snippet and "pg=" not in snippet:
        # No catalog content at all – likely a redirect/login/block page
        log.warning("BrickLink response contains no catalog content (possible block).")
        return False
    return True


def fetch_with_proxy(url: str, session, *, params: dict | None = None,
                     stream: bool = False, retries: int = 5,
                     validate: bool = False) -> requests.Response:
    """
    GET *url* through a golden proxy.
    Each attempt uses a freshly-picked proxy (rotates every try).
    On any failure OR failed validation the offending proxy is deleted from the
    DB and the next proxy is tried.  Raises RuntimeError if all retries fail or
    no proxies are left.  Never falls back to a direct connection.
    Set validate=True for BrickLink catalog pages to detect error pages.
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
            if validate and not is_valid_catalog_html(resp):
                raise ValueError("Proxy returned a blocked/error page from BrickLink")
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.ProxyError,
                requests.exceptions.SSLError) as exc:
            # Connection-level failure – proxy is dead, drop it and retry
            log.warning("Request attempt %d/%d via %s:%s failed (connection): %s",
                        attempt, retries, proxy_row.ip, proxy_row.port, exc)
            drop_proxy(session, proxy_row)
            last_exc = exc
            time.sleep(3 * attempt)  # back-off: 3s, 6s, 9s …
        except Exception as exc:
            # HTTP error (4xx/5xx) or other – proxy is fine, resource issue – stop retrying
            log.warning("Request attempt %d/%d via %s:%s failed (HTTP): %s",
                        attempt, retries, proxy_row.ip, proxy_row.port, exc)
            last_exc = exc
            break
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
        resp = fetch_with_proxy(BRICKLINK_BASE, session, params=params,
                                retries=5, validate=True)
        return resp.text
    except RuntimeError as exc:
        log.warning("fetch_page year=%d pg=%d: %s", year, pg, exc)
        return None


_INV_RE = re.compile(r'^\s*\(\s*Inv\s*\)\s*$', re.I)


def parse_sets(html: str) -> list[dict]:
    """
    Parse catalog list HTML and return a list of dicts:
      { item_no, description, image_url, large_image_url }

    BrickLink catalog structure per row (≥3 <TD> siblings in a <TR>):
      One TD has the item_no via a catalogitem.page?S= link (not the "(Inv)" link).
      One TD has <strong> or <b> with the set name as description.
      One TD has the thumbnail <IMG>.

    We search all TDs rather than assuming fixed column positions, since
    BrickLink has been observed with varying column order.

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

        # Find item_no: first catalogitem.page?S= link whose text is NOT "(Inv)"
        item_no = None
        for td in tds:
            for a in td.find_all("a", href=re.compile(r"catalogitem\.page\?S=", re.I)):
                text = a.get_text(strip=True)
                if text and not _INV_RE.match(text):
                    item_no = text
                    break
            if item_no:
                break

        if not item_no or item_no in seen:
            continue
        seen.add(item_no)

        # Find description: first <strong> or <b> whose text is not "(Inv)"
        description = ""
        for td in tds:
            for tag in td.find_all(["strong", "b"]):
                t = tag.get_text(strip=True)
                if t and not _INV_RE.match(t):
                    description = t
                    break
            if description:
                break

        # Find image: first <img> in any TD
        image_url = ""
        large_image_url = ""
        for td in tds:
            img = td.find("img")
            if img:
                src = img.get("src", "")
                if src.startswith("//"):
                    src = "https:" + src
                image_url = src
                large_image_url = src.replace("/ItemImage/ST/", "/ItemImage/SL/").replace(".t1.png", ".png")
                break

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
    description = item["description"]
    # Never write a parsing artifact "(Inv)" as the description
    if _INV_RE.match(description or ""):
        log.debug("Skipping (Inv) pseudo-row: year=%d item_no=%s", year, item.get("item_no", "?"))
        return
    try:
        result = session.execute(
            """
            INSERT INTO lego_sets (year, item_no, description, image_url, first_seen, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """,
            (year, item["item_no"], description, item["image_url"], now, now),
        )
        if not result.one().applied:
            session.execute(
                update_stmt,
                (now, description, item["image_url"], year, item["item_no"]),
            )
    except Exception as exc:
        log.debug("Upsert error %s/%s: %s", year, item["item_no"], exc)


# ── Main scrape cycle ─────────────────────────────────────────────────────────
def discover_pages(session, years: list[int]) -> list[tuple[int, int]]:
    """
    Fetch page 1 of every year to learn total page counts.
    Returns a flat list of (year, page) tuples for every page that exists,
    in randomised order so scraping never follows a predictable pattern.
    """
    all_pairs: list[tuple[int, int]] = []
    random.shuffle(years)  # even discovery order is randomised
    for year in years:
        log.info("Discovering pages for year=%d …", year)
        html = fetch_page(year, 1, session)
        if not html:
            log.warning("  Could not reach year=%d during discovery – will retry next cycle.", year)
            continue
        total = get_total_pages(html)
        log.info("  year=%d → %d pages", year, total)
        # page 1 HTML is already in hand; tag it so the scraper can reuse it
        all_pairs.append((year, 1))
        for pg in range(2, total + 1):
            all_pairs.append((year, pg))
        time.sleep(DELAY_BETWEEN_PAGES)

    random.shuffle(all_pairs)
    return all_pairs


def scrape_page(session, update_stmt, year: int, pg: int, prefetched_html: str | None = None) -> int:
    """Scrape a single (year, page) pair. Returns number of sets upserted."""
    html = prefetched_html if prefetched_html else fetch_page(year, pg, session)
    if not html:
        log.warning("  Skipping year=%d page=%d (fetch failed).", year, pg)
        return 0
    sets = parse_sets(html)
    log.info("  year=%d page=%d → %d sets", year, pg, len(sets))
    now = datetime.now(timezone.utc)
    for item in sets:
        upsert_set(session, None, update_stmt, year, item, now)
        download_image(item, session)
    return len(sets)


def purge_inv_rows(session) -> int:
    """
    Delete all lego_sets rows whose description is the '(Inv)' parsing artefact.
    Called before each scrape cycle so those rows are re-scraped with correct data.
    Scans the full target_years range.
    """
    years = target_years()
    deleted = 0
    for y in years:
        try:
            rows = list(session.execute(
                "SELECT year, item_no, description FROM lego_sets WHERE year = %s", (y,)
            ))
            for r in rows:
                if r.description and _INV_RE.match(r.description):
                    session.execute(
                        "DELETE FROM lego_sets WHERE year = %s AND item_no = %s",
                        (r.year, r.item_no),
                    )
                    deleted += 1
        except Exception as exc:
            log.warning("purge_inv_rows year=%d: %s", y, exc)
    if deleted:
        log.info("Purged %d (Inv) rows — they will be re-scraped this cycle.", deleted)
    return deleted


def backfill_missing_images(session) -> int:
    """
    After each scrape cycle, scan every row in the target_years range that has
    an image_url but no local image file, and download it via a golden proxy.
    This ensures images are downloaded even if they were missed in a previous cycle.
    """
    years = target_years()
    downloaded = 0
    skipped = 0
    for y in years:
        try:
            rows = list(session.execute(
                "SELECT item_no, image_url FROM lego_sets WHERE year = %s", (y,)
            ))
            for r in rows:
                if not r.item_no or not r.image_url:
                    continue
                safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", r.item_no) + ".png"
                dest = os.path.join(IMAGE_DIR, safe_name)
                if os.path.exists(dest):
                    skipped += 1
                    continue
                large_url = (r.image_url
                             .replace("/ItemImage/ST/", "/ItemImage/SL/")
                             .replace(".t1.png", ".png"))
                item = {"item_no": r.item_no, "large_image_url": large_url}
                download_image(item, session)
                if os.path.exists(dest):
                    downloaded += 1
        except Exception as exc:
            log.warning("backfill_missing_images year=%d: %s", y, exc)
    log.info("Image backfill: %d downloaded, %d already present.", downloaded, skipped)
    return downloaded


def run_once(session, update_stmt) -> None:
    years = target_years()
    log.info("═══ Discovery pass for years: %s ═══", years)

    # 1. Purge stale (Inv) rows so they get re-scraped with correct descriptions
    purge_inv_rows(session)

    # 2. Scrape all pages
    all_pairs = discover_pages(session, years)
    log.info("Total (year, page) pairs to scrape: %d – order randomised.", len(all_pairs))

    grand_total = 0
    for i, (year, pg) in enumerate(all_pairs, 1):
        log.info("[%d/%d] Scraping year=%d page=%d …", i, len(all_pairs), year, pg)
        count = scrape_page(session, update_stmt, year, pg)
        grand_total += count
        time.sleep(DELAY_BETWEEN_PAGES)

    log.info("Cycle complete. %d total set records upserted.", grand_total)

    # 3. Download any images that are still missing after the scrape pass
    backfill_missing_images(session)


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
