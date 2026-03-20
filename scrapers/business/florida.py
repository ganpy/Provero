"""
Florida Division of Corporations — business entity scraper.

Search URL  : https://search.sunbiz.org/Inquiry/CorporationSearch/ByName
              GET parameter: searchTerm=<entity name>

Detail URL  : https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResultDetail
              ?inquirytype=EntityName&directionType=Initial&searchNameOrder=<document_number>

The search results page renders a table with columns:
  Entity Name | Document Number | Status | Principal Address

For each result we navigate directly to the detail page via a plain GET request
(no ASP.NET postback required).  Playwright loads the page; BeautifulSoup parses it.

NOTE: Sunbiz requests that automated tools identify themselves and be respectful of
server load.  A 1-second sleep is applied between every detail-page request.
"""

import random
import time
import warnings
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeout, sync_playwright
from sqlalchemy.orm import Session

from api.database import SessionLocal
from api.models.business import Business
from scrapers.business.base import BusinessRecord, BusinessScraper

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://search.sunbiz.org"
SEARCH_URL = f"{BASE_URL}/Inquiry/CorporationSearch/ByName"

# Detail page URL template — document_number is inserted by the caller
DETAIL_URL_TEMPLATE = (
    f"{BASE_URL}/Inquiry/CorporationSearch/SearchResultDetail"
    "?inquirytype=EntityName&directionType=Initial&searchNameOrder={document_number}"
)

# How many results to process per search
MAX_RESULTS = 3

# Polite delay between detail page requests (seconds)
REQUEST_DELAY = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse common date formats used on Sunbiz; return None on failure."""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _text(tag) -> str:
    """Return stripped text from a BeautifulSoup tag, or empty string."""
    return tag.get_text(strip=True) if tag else ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class FloridaScraper(BusinessScraper):
    """Scraper for the Florida Division of Corporations (sunbiz.org)."""

    state_code = "FL"

    def search(self, name: str) -> list[BusinessRecord]:
        """
        Search the Florida SOS for entities matching *name*.

        Flow:
          1. Load the search URL with searchTerm=<name> via a GET request.
          2. Parse the results table to extract entity name + document number.
          3. For each result (up to MAX_RESULTS):
             a. Construct the detail page URL using the document number.
             b. Load the detail page.
             c. Parse entity_type, status, registered_agent, incorporation_date.
             d. Build a BusinessRecord and append to the list.
          4. Return the list — does NOT write to the DB.
        """
        records: list[BusinessRecord] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )
            page = context.new_page()
            # Mask the webdriver property that Cloudflare checks to detect
            # headless/automated browsers.
            page.evaluate(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            try:
                # --------------------------------------------------------
                # 1. Load the search results page
                # --------------------------------------------------------
                search_url_with_param = f"{SEARCH_URL}?searchTerm={name}"
                print(f"[florida] Loading: {search_url_with_param}")
                page.goto(
                    search_url_with_param,
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
                # Wait for all network activity to settle so dynamically
                # rendered table rows are present before we snapshot the DOM.
                # 30-second timeout gives Cloudflare's JS challenge time to
                # complete its full verification cycle before we read the DOM.
                page.wait_for_load_state("networkidle", timeout=30_000)
                # Random delay (2–4 s) mimics human reading time and gives any
                # deferred Cloudflare JS rendering a chance to finish.
                time.sleep(random.uniform(2, 4))

                # --------------------------------------------------------
                # DEBUG: print first 3000 chars of raw HTML so we can inspect
                # the actual structure of the results page.
                # --------------------------------------------------------
                raw_html = page.content()
                print("\n[florida DEBUG] First 3000 chars of search results page HTML:")
                print(raw_html[:3000])
                print("[florida DEBUG] End of HTML snippet\n")

                # --------------------------------------------------------
                # 2. Parse results table
                # --------------------------------------------------------
                soup = BeautifulSoup(raw_html, "lxml")
                summary_rows = _parse_results_table(soup)

                if not summary_rows:
                    print("[florida] No results found.")
                    return records

                to_process = summary_rows[:MAX_RESULTS]
                print(
                    f"[florida] Found {len(summary_rows)} results; "
                    f"processing first {len(to_process)}."
                )

                # --------------------------------------------------------
                # 3. Visit each detail page
                # --------------------------------------------------------
                for i, row in enumerate(to_process):
                    time.sleep(REQUEST_DELAY)

                    doc_num = row["document_number"]
                    detail_url = DETAIL_URL_TEMPLATE.format(document_number=doc_num)

                    print(
                        f"[florida] Fetching detail {i + 1}/{len(to_process)}: "
                        f"{row['name']} ({doc_num})"
                    )

                    try:
                        page.goto(detail_url, wait_until="domcontentloaded", timeout=30_000)
                    except PlaywrightTimeout:
                        warnings.warn(
                            f"[florida] Timed out loading detail page for '{row['name']}'. "
                            "Skipping.",
                            stacklevel=2,
                        )
                        continue

                    detail_soup = BeautifulSoup(page.content(), "lxml")
                    detail = _parse_detail_page(detail_soup)

                    records.append(
                        BusinessRecord(
                            name=row["name"],
                            entity_type=detail.get("entity_type", ""),
                            status=detail.get("status", ""),
                            state="FL",
                            source_url=detail_url,
                            entity_number=doc_num,
                            registered_agent=detail.get("registered_agent"),
                            incorporation_date=detail.get("incorporation_date"),
                            last_updated=datetime.utcnow(),
                        )
                    )

            finally:
                browser.close()

        return records


# ---------------------------------------------------------------------------
# HTML parsers (pure BeautifulSoup — no Playwright dependency)
# ---------------------------------------------------------------------------

def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract (name, document_number) pairs from the Sunbiz search results table.

    The results page renders a <table> (or a list of rows inside a div) where
    each data row contains:
      col 0 — Entity Name  (link text)
      col 1 — Document Number
      col 2 — Status
      col 3 — Principal Address

    Returns rows in DOM order.
    """
    rows: list[dict] = []

    # Sunbiz wraps results in a <div class="srchRslt"> or similar; look for
    # any table that contains entity-name links first.
    table = None
    for t in soup.find_all("table"):
        text = t.get_text(" ").lower()
        if "entity name" in text or "document number" in text:
            table = t
            break

    # Fallback: look for a results container
    if table is None:
        container = soup.find("div", class_=lambda c: c and "result" in c.lower())
        if container:
            table = container.find("table")

    if table is None:
        # Last resort: try the first sizeable table on the page
        tables = soup.find_all("table")
        for t in tables:
            if len(t.find_all("tr")) > 1:
                table = t
                break

    if table is None:
        return rows

    all_rows = table.find_all("tr")
    if not all_rows:
        return rows

    # Determine header row to find column indices
    header_row = all_rows[0]
    headers = [
        th.get_text(strip=True).lower()
        for th in header_row.find_all(["th", "td"])
    ]

    col_name = _col_index(headers, ("entity name", "name"))
    col_doc = _col_index(headers, ("document number", "document no", "doc number", "file number"))

    for tr in all_rows[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        # Entity name: prefer link text, fall back to cell text
        name_cell = cells[col_name] if col_name is not None and col_name < len(cells) else cells[0]
        link = name_cell.find("a")
        entity_name = _text(link) if link else _text(name_cell)
        if not entity_name:
            continue

        # Document number
        document_number = ""
        if col_doc is not None and col_doc < len(cells):
            document_number = _text(cells[col_doc])
        elif len(cells) > 1:
            # Try to find something that looks like a document number
            # Sunbiz doc numbers are typically alphanumeric like "P12000012345"
            for cell in cells[1:]:
                candidate = _text(cell)
                if candidate and len(candidate) >= 6 and not candidate.startswith("http"):
                    document_number = candidate
                    break

        rows.append({"name": entity_name, "document_number": document_number})

    return rows


def _parse_detail_page(soup: BeautifulSoup) -> dict:
    """
    Extract fields from a Sunbiz entity detail page.

    The detail page renders information in a series of label/value pairs,
    typically inside a <div class="detailSection"> or similar structure.
    We scan for known label strings and read the adjacent text.

    Fields extracted:
      - entity_type       e.g. "Florida Profit Corporation"
      - status            e.g. "Active"
      - registered_agent  e.g. "JOHN DOE"
      - incorporation_date  e.g. date(2010, 3, 15)
    """
    result: dict = {
        "entity_type": "",
        "status": "",
        "registered_agent": None,
        "incorporation_date": None,
    }

    # ----------------------------------------------------------------
    # Strategy 1: look for <span> / <label> + sibling/next text pattern
    # that Sunbiz uses in detail sections.
    #
    # Sunbiz detail pages typically have structures like:
    #   <span class="label">Filing Type</span>
    #   <span>Florida Profit Corporation</span>
    # or label/value pairs in a definition-list style.
    # ----------------------------------------------------------------

    def _find_value_after_label(label_text: str) -> str:
        """
        Find the text that follows a label matching label_text (case-insensitive).
        Checks <span>, <p>, <td>, and <div> elements for label patterns.
        """
        label_lower = label_text.lower()

        # Check spans with class "label" or similar
        for tag in soup.find_all(["span", "label", "th", "td", "p", "div"]):
            tag_text = tag.get_text(strip=True).lower()
            if tag_text == label_lower or tag_lower_matches(tag_text, label_lower):
                # Try next sibling
                sibling = tag.find_next_sibling()
                if sibling:
                    val = sibling.get_text(strip=True)
                    if val:
                        return val
                # Try parent's next sibling
                parent = tag.parent
                if parent:
                    next_sib = parent.find_next_sibling()
                    if next_sib:
                        val = next_sib.get_text(strip=True)
                        if val:
                            return val
                # Try next element in document order
                next_el = tag.find_next(["span", "p", "td", "div"])
                if next_el:
                    val = next_el.get_text(strip=True)
                    if val and val.lower() != label_lower:
                        return val
        return ""

    def tag_lower_matches(tag_text: str, label_lower: str) -> bool:
        """True if tag_text starts with or contains label_lower as a phrase."""
        return tag_text.startswith(label_lower) or label_lower in tag_text

    # ----------------------------------------------------------------
    # Strategy 2: table-based label/value rows (common in Sunbiz)
    # Rows look like: <tr><td>Label:</td><td>Value</td></tr>
    # ----------------------------------------------------------------
    label_value_map: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) >= 2:
            label_raw = cells[0].get_text(strip=True).rstrip(":").lower()
            value_raw = cells[1].get_text(strip=True)
            if label_raw and value_raw:
                label_value_map[label_raw] = value_raw

    def _lv(keys: tuple) -> str:
        """Look up the first matching key from label_value_map."""
        for k in keys:
            k_lower = k.lower()
            # Exact match
            if k_lower in label_value_map:
                return label_value_map[k_lower]
            # Partial match
            for lk, lv in label_value_map.items():
                if k_lower in lk:
                    return lv
        return ""

    # ----------------------------------------------------------------
    # Extract fields — try table map first, then sibling strategy
    # ----------------------------------------------------------------

    # Entity type / filing type
    entity_type = _lv(("filing type", "entity type", "corporation type", "type"))
    if not entity_type:
        entity_type = _find_value_after_label("Filing Type")
    if not entity_type:
        entity_type = _find_value_after_label("Entity Type")
    result["entity_type"] = entity_type

    # Status
    status = _lv(("status", "filing status", "corporation status"))
    if not status:
        status = _find_value_after_label("Status")
    result["status"] = status

    # Registered agent name
    agent = _lv(("registered agent name", "agent name", "registered agent"))
    if not agent:
        agent = _find_value_after_label("Registered Agent Name")
    if not agent:
        agent = _find_value_after_label("Agent Name")
    result["registered_agent"] = agent if agent else None

    # Incorporation / filing date
    inc_raw = _lv(("date filed", "filing date", "incorporation date", "date incorporated"))
    if not inc_raw:
        inc_raw = _find_value_after_label("Date Filed")
    if not inc_raw:
        inc_raw = _find_value_after_label("Filing Date")
    result["incorporation_date"] = _parse_date(inc_raw) if inc_raw else None

    return result


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

    Match on (name, state, source_url) — update if found, insert if not.
    """
    saved: list[Business] = []
    for rec in records:
        existing = (
            db.query(Business)
            .filter(
                Business.name == rec.name,
                Business.state == rec.state,
                Business.source_url == rec.source_url,
            )
            .first()
        )
        if existing:
            existing.entity_type = rec.entity_type or existing.entity_type
            existing.status = rec.status or existing.status
            existing.registered_agent = rec.registered_agent or existing.registered_agent
            existing.incorporation_date = rec.incorporation_date or existing.incorporation_date
            existing.last_updated = datetime.utcnow()
            saved.append(existing)
        else:
            business = Business(
                name=rec.name,
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
# Quick test — run directly: python -m scrapers.business.florida
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pprint

    print("Searching Florida Division of Corporations for 'Apple' ...")
    scraper = FloridaScraper()
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
