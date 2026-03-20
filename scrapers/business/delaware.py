"""
Delaware Division of Corporations — business entity scraper.

Search URL : https://icis.corp.delaware.gov/Ecorp/EntitySearch/NameSearch.aspx
Detail URL : https://icis.corp.delaware.gov/Ecorp/EntitySearch/EntityInformation.aspx

The site uses ASP.NET WebForms.  Every navigation is a POST-based postback;
EntityInformation.aspx does not respond to plain GET requests.

Detail page flow:
  1. Click the lnkbtnEntityName link (triggers __doPostBack → POST → detail page).
  2. Parse the detail page HTML.
  3. Re-navigate by goto(SEARCH_URL) + re-fill + re-submit to get back to the
     results table.  go_back() is avoided because browsers refuse to re-POST
     without user confirmation, making it unreliable in headless mode.

NOTE: The Delaware SOS explicitly prohibits data mining and excessive automated
searches.  Use this scraper responsibly — add delays between requests and only
query what you genuinely need for verification purposes.
"""

import time
import warnings
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout, sync_playwright
from sqlalchemy.orm import Session

from api.database import SessionLocal
from api.models.business import Business
from scrapers.business.base import BusinessRecord, BusinessScraper

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://icis.corp.delaware.gov"
SEARCH_URL = f"{BASE_URL}/Ecorp/EntitySearch/NameSearch.aspx"

# ASP.NET WebForms element IDs
INPUT_ENTITY_NAME = "#ctl00_ContentPlaceHolder1_frmEntityName"
BTN_SEARCH = "#ctl00_ContentPlaceHolder1_btnSubmit"

# Results table and entity-name link selectors
RESULTS_TABLE_CSS = "#tblResults"
ENTITY_LINK_CSS = "a[id*='lnkbtnEntityName']"

# Canonical detail page URL (form action discovered from live HTML)
# ASP.NET postback keeps the browser URL at NameSearch.aspx, so we
# construct source_url from this pattern using the file number.
DETAIL_PAGE_URL = f"{BASE_URL}/Ecorp/EntitySearch/SearchDetailsPage.aspx"

# How many results to process per search (test limit)
MAX_RESULTS = 3

# Polite delay between entity clicks (seconds)
CLICK_DELAY = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse MM/DD/YYYY or YYYY-MM-DD strings; return None on failure."""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class DelawareScraper(BusinessScraper):
    """Scraper for the Delaware Division of Corporations entity search."""

    state_code = "DE"

    def search(self, name: str) -> list[BusinessRecord]:
        """
        Search Delaware SOS for *name* and return enriched BusinessRecord list.

        Flow:
          1. Load the search page and submit the entity name.
          2. Parse the results table to collect (entity_name, file_number) rows.
          3. For each row (up to MAX_RESULTS):
             a. Click the nth lnkbtnEntityName link (ASP.NET postback → detail page).
             b. Capture page.url as source_url.
             c. Parse detail fields with BeautifulSoup.
             d. If more entities remain, re-navigate to SEARCH_URL and re-submit
                the search to restore the results table for the next click.
                (go_back() is avoided — browsers block re-POST in headless mode.)
        """
        records: list[BusinessRecord] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            try:
                # --------------------------------------------------------
                # 1. Load search page and submit
                # --------------------------------------------------------
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30_000)
                page.fill(INPUT_ENTITY_NAME, name)

                with page.expect_navigation(wait_until="domcontentloaded", timeout=30_000):
                    try:
                        page.click(BTN_SEARCH, timeout=5_000)
                    except PlaywrightTimeout:
                        page.click("input[type='submit']", timeout=5_000)

                # --------------------------------------------------------
                # 2. Debug: print raw results table HTML
                # --------------------------------------------------------
                soup = BeautifulSoup(page.content(), "lxml")
                _debug_dump_results_table(soup)

                # --------------------------------------------------------
                # 3. Parse summary rows (name + file number only)
                # --------------------------------------------------------
                summary_rows = _parse_results_table(soup)
                if not summary_rows:
                    print("[delaware] No results found.")
                    return records

                to_process = summary_rows[:MAX_RESULTS]
                print(f"[delaware] Found {len(summary_rows)} results; processing first {len(to_process)}.")

                # --------------------------------------------------------
                # 4. Click each entity link → parse detail → re-search to reset
                # --------------------------------------------------------
                for i, row in enumerate(to_process):
                    time.sleep(CLICK_DELAY)

                    # Re-locate links fresh every iteration — re-searching the
                    # form re-renders the DOM so old handles are always stale.
                    entity_links = page.locator(ENTITY_LINK_CSS)
                    link_count = entity_links.count()

                    if i >= link_count:
                        warnings.warn(
                            f"[delaware] Link index {i} out of range "
                            f"(only {link_count} found). Skipping '{row['name']}'.",
                            stacklevel=2,
                        )
                        continue

                    print(f"[delaware] Clicking entity {i + 1}/{len(to_process)}: {row['name']}")

                    # Click triggers __doPostBack → server responds with detail page
                    try:
                        with page.expect_navigation(
                            wait_until="domcontentloaded", timeout=30_000
                        ):
                            entity_links.nth(i).click()
                    except PlaywrightTimeout:
                        warnings.warn(
                            f"[delaware] Navigation timed out for '{row['name']}'. Skipping.",
                            stacklevel=2,
                        )
                        continue

                    # ASP.NET postback never changes page.url — construct the
                    # canonical source URL from the file number instead.
                    source_url = f"{DETAIL_PAGE_URL}?i={row['file_number']}"
                    detail_soup = BeautifulSoup(page.content(), "lxml")

                    # Debug: dump full detail page HTML for first entity only
                    if i == 0:
                        print("\n[delaware DEBUG] Detail page HTML (first entity):")
                        print(detail_soup.prettify())
                        print("[delaware DEBUG] End of detail page HTML\n")

                    detail = _parse_detail_page(detail_soup)

                    records.append(
                        BusinessRecord(
                            name=row["name"],
                            entity_type=detail.get("entity_type", ""),
                            status=detail.get("status", ""),
                            state="DE",
                            source_url=source_url,
                            entity_number=row.get("file_number"),
                            registered_agent=detail.get("registered_agent"),
                            incorporation_date=detail.get("incorporation_date"),
                            last_updated=datetime.utcnow(),
                        )
                    )

                    # Restore results page for the next iteration by re-submitting
                    # the original search.  go_back() is unreliable for POST-based
                    # ASP.NET postback pages in headless Chromium.
                    if i < len(to_process) - 1:
                        try:
                            page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30_000)
                            page.fill(INPUT_ENTITY_NAME, name)
                            with page.expect_navigation(
                                wait_until="domcontentloaded", timeout=30_000
                            ):
                                try:
                                    page.click(BTN_SEARCH, timeout=5_000)
                                except PlaywrightTimeout:
                                    page.click("input[type='submit']", timeout=5_000)
                            page.wait_for_selector(RESULTS_TABLE_CSS, timeout=15_000)
                        except PlaywrightTimeout:
                            warnings.warn(
                                "[delaware] Could not restore results page; stopping early.",
                                stacklevel=2,
                            )
                            break

            finally:
                browser.close()

        return records


# ---------------------------------------------------------------------------
# HTML parsers (pure BeautifulSoup — no Playwright dependency)
# ---------------------------------------------------------------------------

def _debug_dump_results_table(soup: BeautifulSoup) -> None:
    """
    Print the raw HTML of the results table for debugging.

    Tries #tblResults first, then falls back to any table containing
    'entity name' or 'file number' in its text.
    """
    table = soup.find("table", id="tblResults")
    if table is None:
        for t in soup.find_all("table"):
            text = t.get_text(" ").lower()
            if "entity name" in text or "file number" in text:
                table = t
                break

    print("\n[delaware DEBUG] Results table HTML:")
    if table:
        print(table.prettify())
    else:
        print("  <no results table found — dumping full page text snippet>")
        print(soup.get_text(" ", strip=True)[:2000])
    print("[delaware DEBUG] End of table dump\n")


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract (name, file_number) pairs from the results table.

    Expected structure inside #tblResults:
      - Each data row has an <a id="...lnkbtnEntityName..."> with the entity name.
      - The file number is in an adjacent cell.

    Returns rows in DOM order so indices align with lnkbtnEntityName link order.
    """
    rows: list[dict] = []

    table = soup.find("table", id="tblResults")
    if table is None:
        # Fallback: any table with entity-name links
        for t in soup.find_all("table"):
            if t.find("a", id=lambda v: v and "lnkbtnEntityName" in v):
                table = t
                break

    if table is None:
        return rows

    header_row = table.find("tr")
    if header_row is None:
        return rows

    headers = [
        th.get_text(strip=True).lower()
        for th in header_row.find_all(["th", "td"])
    ]
    col_file = _col_index(headers, ("file number", "file no", "entity number"))

    for tr in table.find_all("tr")[1:]:  # skip header
        cells = tr.find_all("td")
        if not cells:
            continue

        # Entity name comes from the lnkbtnEntityName anchor text
        link = tr.find("a", id=lambda v: v and "lnkbtnEntityName" in v)
        if link is None:
            continue

        entity_name = link.get_text(strip=True)
        if not entity_name:
            continue

        file_number = ""
        if col_file is not None and col_file < len(cells):
            file_number = cells[col_file].get_text(strip=True)

        rows.append({"name": entity_name, "file_number": file_number})

    return rows


def _parse_detail_page(soup: BeautifulSoup) -> dict:
    """
    Extract fields from a Delaware SOS entity detail page.

    The page renders every data point in a <span> with a deterministic
    ASP.NET control ID.  We read those spans directly rather than scanning
    generic label/value pairs.

    Field → span id mapping (discovered from live HTML):
      Entity Kind      → ctl00_ContentPlaceHolder1_lblEntityKind   e.g. "Limited Partnership"
      Incorporation Date → ctl00_ContentPlaceHolder1_lblIncDate     e.g. "12/8/2015"
      Registered Agent → ctl00_ContentPlaceHolder1_lblAgentName    e.g. "THE CORPORATION TRUST COMPANY"

    Status is behind a $10 paywall on the free search page and is not
    extractable from the public HTML.
    """
    def _span(span_id: str) -> str:
        tag = soup.find("span", id=span_id)
        return tag.get_text(strip=True) if tag else ""

    raw_date = _span("ctl00_ContentPlaceHolder1_lblIncDate")
    agent_name = _span("ctl00_ContentPlaceHolder1_lblAgentName")

    return {
        "entity_type": _span("ctl00_ContentPlaceHolder1_lblEntityKind"),
        "status": "",  # requires $10 fee — not in free public search
        "registered_agent": agent_name if agent_name else None,
        "incorporation_date": _parse_date(raw_date) if raw_date else None,
    }


def _col_index(headers: list[str], candidates: tuple) -> Optional[int]:
    """Return the first index whose header text contains any candidate string."""
    for i, h in enumerate(headers):
        for c in candidates:
            if c in h:
                return i
    return None


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

def save_records(records: list[BusinessRecord], db: Session) -> list[Business]:
    """
    Upsert BusinessRecord list into the Business table.

    Match on (name, state) — update if found, insert if not.

    Matching on (name, state) instead of (name, state, source_url) prevents
    duplicate rows when early scraping runs stored source_url as the generic
    NameSearch.aspx / EntityInformation.aspx page rather than the canonical
    per-entity URL.  Any existing row for a given entity is updated in place;
    its source_url is upgraded to the canonical detail URL on each run.
    """
    saved: list[Business] = []
    for rec in records:
        existing = (
            db.query(Business)
            .filter(
                Business.name == rec.name,
                Business.state == rec.state,
            )
            .first()
        )
        if existing:
            existing.entity_number = rec.entity_number or existing.entity_number
            existing.entity_type = rec.entity_type or existing.entity_type
            existing.status = rec.status or existing.status
            existing.registered_agent = rec.registered_agent or existing.registered_agent
            existing.incorporation_date = rec.incorporation_date or existing.incorporation_date
            existing.source_url = rec.source_url or existing.source_url
            existing.last_updated = datetime.utcnow()
            saved.append(existing)
        else:
            business = Business(
                name=rec.name,
                entity_number=rec.entity_number,
                entity_type=rec.entity_type or "",
                status=rec.status or "",
                state=rec.state,
                registered_agent=rec.registered_agent,
                incorporation_date=rec.incorporation_date,
                last_updated=rec.last_updated,
                source_url=rec.source_url,
            )
            db.add(business)
            saved.append(business)

    db.commit()
    return saved


# ---------------------------------------------------------------------------
# Quick test — run directly: python -m scrapers.business.delaware
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pprint

    print("Searching Delaware SOS for 'Apple' ...")
    scraper = DelawareScraper()
    results = scraper.search("Apple")

    print(f"\nTotal results returned: {len(results)}")
    print("\n--- First 3 results ---\n")
    for rec in results[:3]:
        pprint.pprint({
            "name": rec.name,
            "entity_type": rec.entity_type,
            "status": rec.status,
            "state": rec.state,
            "entity_number": rec.entity_number,
            "registered_agent": rec.registered_agent,
            "incorporation_date": str(rec.incorporation_date) if rec.incorporation_date else None,
            "source_url": rec.source_url,
        })
        print()

    print("Saving to database ...")
    db = SessionLocal()
    try:
        saved = save_records(results, db)
        print(f"Saved / updated {len(saved)} records.")
    finally:
        db.close()
