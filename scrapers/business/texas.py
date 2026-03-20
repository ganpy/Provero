"""
Texas Comptroller of Public Accounts — franchise tax account status search.

The original Texas SOS URL (https://mycpa.cpa.state.tx.us/coa/Index.html) redirects
to the Texas Comptroller franchise tax account status search at:
  https://comptroller.texas.gov/taxes/franchise/account-status/search

This page is JavaScript-driven: form submission fires an AJAX request that
injects results into #resultTable — there is NO full-page navigation on submit.
Playwright waits for rows to appear inside the table body before snapshotting
the DOM for BeautifulSoup to parse.

Search form elements (confirmed from live page source):
  Name input    : #name         (text, 2–50 chars)
  Submit button : #submitBtn
  Results table : #resultTable
    col 0 — Name            (link → detail page)
    col 1 — Taxpayer Number (11-digit Comptroller ID)
    col 2 — Zip Code

Detail page URL:
  https://comptroller.texas.gov/taxes/franchise/account-status/search/{taxpayerId}

Fields extracted from the detail page (confirmed from texas_detail_debug.html):
  - entity_type        (e.g. "FOREIGN FOR-PROFIT CORPORATION")
  - status             (e.g. "Active")
  - registered_agent   (e.g. "CT CORPORATION SYSTEM")
  - incorporation_date (e.g. date(1977, 1, 3))
"""

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

BASE_URL = "https://comptroller.texas.gov"
SEARCH_URL = f"{BASE_URL}/taxes/franchise/account-status/search"

# Form selectors
INPUT_NAME = "#name"
BTN_SUBMIT = "#submitBtn"

# AJAX results — rows are injected here; no navigation event fires
RESULTS_TABLE_SEL = "#resultTable"
RESULTS_ROW_SEL   = "#resultTable tbody tr"

# How many results to process per search
MAX_RESULTS = 3

# Polite delay between detail page requests (seconds)
DETAIL_DELAY = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse common date formats used on the Texas Comptroller site."""
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%B %d, %Y"):
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

class TexasScraper(BusinessScraper):
    """Scraper for the Texas Comptroller franchise tax account status search."""

    state_code = "TX"

    def search(self, name: str) -> list[BusinessRecord]:
        """
        Search the Texas Comptroller site for entities matching *name*.

        Flow:
          1. Load SEARCH_URL, fill #name, click #submitBtn.
          2. Wait for AJAX results to render in #resultTable tbody tr.
          3. Parse the results table for name, taxpayer_id, and detail URL.
          4. For each result (up to MAX_RESULTS):
             a. Navigate to the detail page (plain GET — real navigation).
             b. Parse entity_type, status, registered_agent, incorporation_date.
             c. Build a BusinessRecord and append.
          5. Return the list — does NOT write to the DB.

        Key difference from WebForms scrapers: there is no navigation event
        on form submit; expect_navigation() must NOT be used here.  Instead
        we wait for rows to appear inside #resultTable.
        """
        records: list[BusinessRecord] = []

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            try:
                # --------------------------------------------------------
                # 1. Load search page, fill name field, click submit
                # --------------------------------------------------------
                print(f"[texas] Loading: {SEARCH_URL}")
                # Must wait for 'networkidle' — the AJAX submit handler is
                # attached by deferred JavaScript that hasn't run yet at
                # 'domcontentloaded'.  Clicking before networkidle silently
                # does nothing and leaves the results table permanently empty.
                page.goto(SEARCH_URL, wait_until="networkidle", timeout=30_000)
                page.fill(INPUT_NAME, name)
                print(f"[texas] Submitting search for '{name}' ...")
                page.click(BTN_SUBMIT)

                # --------------------------------------------------------
                # 2. Wait for AJAX results — NO navigation occurs
                # --------------------------------------------------------
                try:
                    page.wait_for_selector(RESULTS_ROW_SEL, timeout=30_000)
                except PlaywrightTimeout:
                    print("[texas] Timed out waiting for #resultTable rows.")
                    return records

                # --------------------------------------------------------
                # DEBUG: save full results-page HTML for structure inspection
                # --------------------------------------------------------
                raw_html = page.content()
                debug_path = "texas_debug.html"
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(raw_html)
                print(f"[texas DEBUG] Full results page HTML written to {debug_path}")

                # --------------------------------------------------------
                # 3. Parse the results table
                # --------------------------------------------------------
                soup = BeautifulSoup(raw_html, "lxml")
                _debug_dump_results_table(soup)

                summary_rows = _parse_results_table(soup)
                if not summary_rows:
                    print("[texas] No results parsed from table.")
                    return records

                to_process = summary_rows[:MAX_RESULTS]
                print(
                    f"[texas] Found {len(summary_rows)} results; "
                    f"processing first {len(to_process)}."
                )

                # --------------------------------------------------------
                # 4. Visit each detail page via direct GET
                # --------------------------------------------------------
                for i, row in enumerate(to_process):
                    time.sleep(DETAIL_DELAY)

                    detail_url = row["detail_url"]
                    print(
                        f"[texas] Fetching detail {i + 1}/{len(to_process)}: "
                        f"{row['name']} ({row['taxpayer_id']})"
                    )

                    try:
                        page.goto(detail_url, wait_until="networkidle", timeout=30_000)
                    except PlaywrightTimeout:
                        warnings.warn(
                            f"[texas] Timed out loading detail page for '{row['name']}'. "
                            "Skipping.",
                            stacklevel=2,
                        )
                        continue

                    detail_html = page.content()

                    # Save the first entity's detail page for structure inspection
                    if i == 0:
                        detail_debug_path = "texas_detail_debug.html"
                        with open(detail_debug_path, "w", encoding="utf-8") as f:
                            f.write(detail_html)
                        print(
                            f"[texas DEBUG] Detail page HTML written to {detail_debug_path}"
                        )

                    detail_soup = BeautifulSoup(detail_html, "lxml")
                    detail = _parse_detail_page(detail_soup)

                    records.append(
                        BusinessRecord(
                            name=row["name"],
                            entity_type=detail.get("entity_type", ""),
                            status=detail.get("status", ""),
                            state="TX",
                            source_url=detail_url,
                            entity_number=row["taxpayer_id"],
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

def _debug_dump_results_table(soup: BeautifulSoup) -> None:
    """Print the raw HTML of the results table for debugging."""
    table = soup.find("table", id="resultTable")

    print("\n[texas DEBUG] Results table HTML:")
    if table:
        print(table.prettify()[:3000])
    else:
        print("  <no #resultTable found — dumping first 2000 chars of page text>")
        print(soup.get_text(" ", strip=True)[:2000])
    print("[texas DEBUG] End of table dump\n")


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract rows from the Texas Comptroller AJAX results table.

    #resultTable columns (confirmed from live page):
      col 0 — Name            (link → /taxes/franchise/account-status/search/{taxpayerId})
      col 1 — Taxpayer Number (11-digit Comptroller ID)
      col 2 — Zip Code

    Returns a list of dicts with keys: name, taxpayer_id, detail_url
    """
    rows: list[dict] = []

    table = soup.find("table", id="resultTable")
    if table is None:
        # Fallback: any table that looks like it contains taxpayer data
        for t in soup.find_all("table"):
            text = t.get_text(" ").lower()
            if "taxpayer" in text or "entity name" in text or "file number" in text:
                table = t
                break

    if table is None:
        return rows

    tbody = table.find("tbody")
    trs = (tbody or table).find_all("tr")

    for tr in trs:
        cells = tr.find_all("td")
        if not cells:
            continue

        # ---- Name and detail link (col 0) ----
        name_cell = cells[0]
        link = name_cell.find("a")
        entity_name = _text(link) if link else _text(name_cell)
        if not entity_name:
            continue

        # Build absolute detail URL from the link href
        detail_url = ""
        if link and link.get("href"):
            href = link["href"].strip()
            if href.startswith("http"):
                detail_url = href
            elif href.startswith("/"):
                detail_url = f"{BASE_URL}{href}"
            else:
                detail_url = f"{BASE_URL}/{href.lstrip('/')}"

        # ---- Taxpayer ID (col 1) ----
        taxpayer_id = ""
        if len(cells) > 1:
            taxpayer_id = _text(cells[1])

        # Fallback: construct detail URL from taxpayer ID if href was absent
        if not detail_url and taxpayer_id:
            detail_url = f"{SEARCH_URL}/{taxpayer_id}"

        rows.append({
            "name": entity_name,
            "taxpayer_id": taxpayer_id,
            "detail_url": detail_url,
        })

    return rows


def _parse_detail_page(soup: BeautifulSoup) -> dict:
    """
    Extract fields from a Texas Comptroller entity detail page.

    The detail page (confirmed from texas_detail_debug.html) renders each
    field as a pair of sibling <div> elements inside a Bootstrap .row:
      <div class="... grey-blocks ...">Label text</div>
      <div class="... results-blocks ...">Value text</div>

    Known label/value pairs from the live detail page:
      "Right to Transact Business in Texas"  → status   (e.g. "ACTIVE")
      "Right Organization Type"              → entity_type
      "Taxpayer/Franchise Tax Account Type"  → entity_type (fallback)
      "Registered Agent Name"                → registered_agent
      "Effective SOS Registration Date"      → incorporation_date

    Falls back to scanning <tr> label/value pairs (officer table rows)
    in case the page structure changes.

    Returns a dict with keys: entity_type, status, registered_agent,
    incorporation_date.
    """
    # ----------------------------------------------------------------
    # Strategy 1 (primary): div.grey-blocks / div.results-blocks pairs
    # ----------------------------------------------------------------
    label_value_map: dict[str, str] = {}

    for label_div in soup.find_all(
        "div", class_=lambda c: c and "grey-blocks" in c
    ):
        value_div = label_div.find_next_sibling(
            "div", class_=lambda c: c and "results-blocks" in c
        )
        if value_div:
            label_raw = label_div.get_text(strip=True).rstrip(":").strip().lower()
            value_raw = value_div.get_text(strip=True)
            if label_raw and value_raw:
                label_value_map[label_raw] = value_raw

    # ----------------------------------------------------------------
    # Strategy 2 (fallback): <tr><td/th>Label</td><td>Value</td></tr>
    # ----------------------------------------------------------------
    if not label_value_map:
        for tr in soup.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) >= 2:
                label_raw = cells[0].get_text(strip=True).rstrip(":").strip().lower()
                value_raw = cells[1].get_text(strip=True)
                if label_raw and value_raw:
                    label_value_map[label_raw] = value_raw

    def _lv(*candidates: str) -> str:
        """Return first matching value — exact match first, then partial."""
        for k in candidates:
            k_lower = k.lower()
            if k_lower in label_value_map:
                return label_value_map[k_lower]
        for k in candidates:
            k_lower = k.lower()
            for lk, lv in label_value_map.items():
                if k_lower in lk:
                    return lv
        return ""

    entity_type = _lv(
        "right organization type",
        "taxpayer/franchise tax account type",
        "organization type",
        "taxpayer type",
        "entity type",
        "type of organization",
    )
    status = _lv(
        "right to transact business in texas",
        "right to transact business",
        "right to transact",
        "status",
        "active status",
        "filing status",
    )
    agent = _lv(
        "registered agent name",
        "registered agent",
        "agent name",
        "resident agent",
    )
    inc_raw = _lv(
        "effective sos registration date",
        "sos registration date",
        "effective date",
        "date filed",
        "filing date",
        "incorporation date",
        "in texas since",
    )

    return {
        "entity_type": entity_type,
        "status": status,
        "registered_agent": agent if agent else None,
        "incorporation_date": _parse_date(inc_raw) if inc_raw else None,
    }


def _col_index(headers: list[str], candidates: tuple) -> Optional[int]:
    """Return the first column index whose header contains any candidate string."""
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
# Quick test — run directly: python -m scrapers.business.texas
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pprint

    # NOTE: The Texas Comptroller API rejects searches that return more than
    # ~500 entries with "Please refine search by Entity Name."  Searching for
    # plain 'Apple' triggers this limit (1538 hits); 'Apple Inc' returns a
    # manageable exact match.
    print("Searching Texas Comptroller for 'Apple Inc' ...")
    scraper = TexasScraper()
    results = scraper.search("Apple Inc")

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
