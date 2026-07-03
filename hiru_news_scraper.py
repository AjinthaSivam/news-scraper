"""
Hiru News (Tamil) Scraper
--------------------------------
Production scraper meant to run on a schedule (e.g. GitHub Actions, every 15 min).

Design: instead of walking a *fixed* nid range, it walks FORWARD from the last
known-good numeric id (nid) and stops when it either (a) saves
MAX_ARTICLES_PER_RUN articles, or (b) hits MAX_CONSECUTIVE_MISSES in a row
(a run of 404s / parse failures, which usually means "no newer articles yet").
This lets the same script both catch up on a backlog and pick up newly
published articles on every run, without ever needing to bump a hardcoded
upper bound by hand.
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import re
import time
import logging
import unicodedata
from datetime import datetime, timezone

# -------------------------------
# Configuration (env-overridable so GitHub Actions can tune it without code changes)
# -------------------------------
BASE_URL = os.environ.get("BASE_URL", "https://hirunews.lk/tm/{}")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "./hiru_tamil")
STATUS_FILE = os.environ.get("STATUS_FILE", "./status.json")
FAILED_LOG = os.environ.get("FAILED_LOG", "./failed_ids.txt")

MAX_ARTICLES_PER_RUN = int(os.environ.get("MAX_ARTICLES_PER_RUN", 60))
MAX_CONSECUTIVE_MISSES = int(os.environ.get("MAX_CONSECUTIVE_MISSES", 6))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", 10))
RETRY_COUNT = int(os.environ.get("RETRY_COUNT", 3))
SLEEP_BETWEEN_REQUESTS = float(os.environ.get("SLEEP_BETWEEN_REQUESTS", 2.5))

HEADERS = {
    # Identify the bot honestly - hirunews.lk's admins can then allowlist/contact you
    # instead of silently rate-limiting or blocking a generic browser UA.
    "User-Agent": os.environ.get(
        "SCRAPER_USER_AGENT",
        "Mozilla/5.0 (compatible; TamilNLPResearchBot/1.0; research use)",
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ta,en-US;q=0.7,en;q=0.3",
    "Referer": "https://hirunews.lk/tm/",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("hiru_scraper")

session = requests.Session()
session.headers.update(HEADERS)


# -------------------------------
# Status persistence (atomic writes so a killed GH Actions job can't corrupt it)
# -------------------------------
def load_status():
    if not os.path.exists(STATUS_FILE):
        raise FileNotFoundError(
            f"{STATUS_FILE} not found. Seed it once with a starting nid, e.g.\n"
            '{"initial_working_nid": 242133, "last_working_nid": 242133}'
        )
    try:
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        # Don't silently fall back to defaults - that risks re-scraping or
        # skipping large ranges. Fail loudly so the workflow run is marked red.
        logger.error(f"{STATUS_FILE} is unreadable ({e}); aborting rather than guessing.")
        raise


def save_status(status):
    tmp_path = STATUS_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=4, ensure_ascii=False)
    os.replace(tmp_path, STATUS_FILE)  # atomic replace on POSIX and Windows


# -------------------------------
# Networking
# -------------------------------
def fetch_with_retry(url, retries=RETRY_COUNT):
    """Returns (response_or_None, reason_or_None)."""
    for attempt in range(retries):
        try:
            res = session.get(url, timeout=REQUEST_TIMEOUT)

            if res.status_code == 200:
                return res, None
            if res.status_code == 404:
                # Article id doesn't exist (yet, or ever) - retrying won't help,
                # and treating this as a normal "miss" is what lets the loop
                # detect it has caught up to the newest published article.
                return None, "not_found"

            logger.warning(f"[Retry] Status {res.status_code} for {url}")

        except requests.RequestException as e:
            logger.warning(f"[Retry] Attempt {attempt + 1}/{retries} failed: {e}")

        time.sleep(3 + attempt * 2)  # exponential backoff

    return None, "error"


# -------------------------------
# Parsing
# -------------------------------
def normalize_text(text):
    if text is None:
        return text
    return unicodedata.normalize("NFC", text)


def parse_article(html, url):
    soup = BeautifulSoup(html, "html.parser")

    try:
        headline_tag = soup.select_one("h1.head-title")
        content_container = soup.select_one("#this-article")
        category_tag = soup.select_one(".update-category")
        timestamp_tag = soup.select_one(".update-category + span")

        if not (headline_tag and content_container and category_tag and timestamp_tag):
            return None

        for tag in content_container.find_all(["iframe", "script", "style"]):
            tag.decompose()

        headline = normalize_text(headline_tag.get_text(strip=True))
        content = normalize_text(content_container.get_text(separator="\n", strip=True))
        category = normalize_text(category_tag.get_text(strip=True))
        timestamp = normalize_text(timestamp_tag.get_text(strip=True))

        if not headline or not content:
            return None

        return {
            "Source": "hirunews",
            "Timestamp": timestamp,
            "Headline": headline,
            "News Content": content,
            "URL": url,
            "Category": category,
            "Parent URL": "https://hirunews.lk/tm/",
        }

    except Exception as e:
        logger.error(f"Parse error for {url}: {e}")
        return None


# -------------------------------
# Date handling
# -------------------------------
# NOTE: the original format string ("%d %B %Y") assumes English month names
# (e.g. "April"). If hirunews.lk's Tamil section ever renders the timestamp
# with a Tamil month name, every article will silently land in unknown-date/.
# Spot-check a live page's timestamp markup before trusting this in production;
# add a Tamil-month lookup table here if needed.
DATE_FORMATS = [
    "%d %B %Y",   # 17 April 2026
    "%B %d, %Y",  # April 17, 2026
    "%Y-%m-%d",
    "%d-%m-%Y",
]


def format_date(date_str):
    if not date_str:
        return "unknown-date"

    date_part = date_str.split("-")[0].strip() if "-" in date_str else date_str.strip()
    # Strip a trailing time component like "10:30 AM" if it wasn't already
    # separated by a dash.
    date_part = re.sub(r"\d{1,2}:\d{2}\s*[APap][Mm]$", "", date_part).strip()

    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_part, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    logger.warning(f"Could not parse date: '{date_str}'")
    return "unknown-date"


# -------------------------------
# Storage
# -------------------------------
def save_article(nid, data):
    date_folder = format_date(data["Timestamp"])
    folder_path = os.path.join(OUTPUT_DIR, date_folder)
    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f"{nid}.json")
    tmp_path = file_path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    os.replace(tmp_path, file_path)  # atomic write


def log_failed(nid, reason=""):
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{nid}\t{reason}\n")


# -------------------------------
# Main scraper
# -------------------------------
def scrape():
    status = load_status()

    nid = status["last_working_nid"] + 1
    consecutive_misses = 0
    articles_saved = 0

    logger.info(
        f"Starting scrape from nid={nid} (cap: {MAX_ARTICLES_PER_RUN} articles, "
        f"stop after {MAX_CONSECUTIVE_MISSES} consecutive misses)"
    )

    while articles_saved < MAX_ARTICLES_PER_RUN and consecutive_misses < MAX_CONSECUTIVE_MISSES:
        url = BASE_URL.format(nid)
        res, err = fetch_with_retry(url)

        if res is None:
            consecutive_misses += 1
            reason = err or "unknown"
            logger.info(f"nid={nid}: {reason} (consecutive misses: {consecutive_misses})")
            log_failed(nid, reason)
            nid += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        article = parse_article(res.text, url)

        if article:
            article["article_id"] = nid
            article["scraped_at"] = datetime.now(timezone.utc).isoformat()

            save_article(nid, article)

            status["last_working_nid"] = nid
            save_status(status)

            articles_saved += 1
            consecutive_misses = 0
            logger.info(f"Saved nid={nid} -> {article['Headline'][:60]}")
        else:
            consecutive_misses += 1
            logger.warning(
                f"nid={nid}: page loaded but parsing failed "
                f"(consecutive misses: {consecutive_misses})"
            )
            log_failed(nid, "parse_failed")

        nid += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    stop_reason = (
        "hit MAX_ARTICLES_PER_RUN cap"
        if articles_saved >= MAX_ARTICLES_PER_RUN
        else "hit MAX_CONSECUTIVE_MISSES (likely caught up to newest article)"
    )
    logger.info(
        f"Run complete. Saved {articles_saved} article(s), last nid tried={nid - 1}. "
        f"Stopped because: {stop_reason}."
    )


if __name__ == "__main__":
    try:
        scrape()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        raise SystemExit(1)
