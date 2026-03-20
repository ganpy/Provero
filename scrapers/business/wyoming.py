"""
Wyoming Secretary of State — business entity scraper.

Search URL  : https://wyobiz.wyo.gov/Business/FilingSearch.aspx
              ASP.NET WebForms — form submitted via Playwright click.

Detail URL  : https://wyobiz.wyo.gov/Business/FilingDetails.aspx?eFNum=<filing_id>
              Plain GET request — href extracted directly from result-table links.

The search results are rendered as a <ul> of <li class="rowRegular|rowHighlight">
elements — NOT a <table>.  Each <li> contains one <a> link and the following
child spans:
  .resFile1  — "Entity Name - FilingID (TypeCode)"
  .resFile2  — "Status: <value>"
  .resFile3  — "Standing - Tax: <value>"
  .resFile4  — "Standing - RA: <value>"
  .resFile5  — "Filed On: MM/DD/YYYY"

For each result we follow the link href to the detail page and extract:
  - entity_type        (from "Filing Type" field)
  - status             (from "Status" field, or from .resFile2 directly)
  - registered_agent   (from "Registered Agent" section)
  - incorporation_date (from "Original Filing Date" or "Date Filed" field)

NOTE: The Wyoming SOS site is ASP.NET WebForms.  The search form submission is a
standard POST handled by Playwright clicking the submit button.  Unlike Delaware,
detail pages on wyobiz respond to plain GET requests, so we can navigate directly
using the href values harvested from the results list — no re-search loop needed.

ASP.NET control IDs confirmed from the live page source (wyoming_debug.html):
  Search input  : MainContent_txtFilingName   (text input)
  Search button : MainContent_cmdSearch       (input.wizButton.searchButton)

If the site is updated and IDs change, the scraper falls back to heuristic
selectors so it degrades gracefully rather than crashing.
"""

import time
import warnings
from datetime import date, datetime
from typing import Optional

from bs4 import BeautifulSoup, Tag
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeout, sync_playwright
from sqlalchemy.orm import Session

from api.database import SessionLocal
from api.models.business import Business
from scrapers.business.base import BusinessRecord, BusinessScraper

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://wyobiz.wyo.gov"
SEARCH_URL = f"{BASE_URL}/Business/FilingSearch.aspx"
DETAIL_BASE_URL = f"{BASE_URL}/Business/FilingDetails.aspx"

# ASP.NET control IDs confirmed from live page source (wyoming_debug.html)
INPUT_FILING_NAME = "#MainContent_txtFilingName"
BTN_SEARCH = "#MainContent_cmdSearch"

# Fallback selectors used when primary IDs change
INPUT_FALLBACKS = [
    "input[id*='txtFilingName']",
    "input[id*='FilingName']",
    "input[name*='FilingName']",
    "input[type='text']",
]
BTN_FALLBACKS = [
    "input[id*='cmdSearch']",
    "input[id*='Search'][type='submit']",
    "input[value='Search']",
    "input[type='submit']",
]

# CSS class used on result <li> elements in the search results list
RESULT_LI_CLASSES = ("rowRegular", "rowHighlight")

# How many results to process per search
MAX_RESULTS = 3

# Polite delay between detail page requests (seconds)
DETAIL_DELAY = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse common date formats used on wyobiz; return None on failure."""
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


def _locate_input(page: Page) -> str:
    """
    Return the CSS selector for the filing-name search input.

    Tries the primary ASP.NET control ID first; falls back through a list
    of heuristic selectors so the scraper degrades gracefully if the site
    updates its control IDs.
    """
    for selector in [INPUT_FILING_NAME] + INPUT_FALLBACKS:
        if page.locator(selector).count() > 0:
            return selector
    raise RuntimeError("[wyoming] Cannot find the filing-name search input on the page.")


def _locate_button(page: Page) -> str:
    """
    Return the CSS selector for the search submit button.

    Tries the primary ASP.NET control ID first; falls back through heuristics.
    """
    for selector in [BTN_SEARCH] + BTN_FALLBACKS:
        if page.locator(selector).count() > 0:
            return selector
    raise RuntimeError("[wyoming] Cannot find the search submit button on the page.")


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class WyomingScraper(BusinessScraper):
    """Scraper for the Wyoming Secretary of State business entity search."""

    state_code = "WY"

    def search(self, name: str) -> list[BusinessRecord]:
        """
        Search Wyoming SOS for entities matching *name* and return enriched
        BusinessRecord list.

        Flow:
          1. Load SEARCH_URL, fill the Filing Name field, submit the form.
          2. Parse the results GridView table to collect
             (entity_name, filing_id, entity_type, status, detail_href) rows.
          3. For each row (up to MAX_RESULTS):
             a. Navigate directly to the detail page URL (plain GET).
             b. Parse registered_agent and incorporation_date with BeautifulSoup.
             c. Build a BusinessRecord and append to the list.
          4. Return the list — does NOT write to the DB.

        Unlike Delaware, Wyoming detail pages respond to plain GET requests, so
        we navigate with page.goto() rather than clicking postback links, and
        there is no need to re-submit the search between records.
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
                # 1. Load the search page and submit
                # --------------------------------------------------------
                print(f"[wyoming] Loading search page: {SEARCH_URL}")
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30_000)

                input_sel = _locate_input(page)
                btn_sel = _locate_button(page)

                page.fill(input_sel, name)
                print(f"[wyoming] Submitting search for '{name}' ...")

                with page.expect_navigation(wait_until="domcontentloaded", timeout=30_000):
                    try:
                        page.click(btn_sel, timeout=5_000)
                    except PlaywrightTimeout:
                        # Last-resort: try any visible submit button
                        page.click("input[type='submit']", timeout=5_000)

                # --------------------------------------------------------
                # 2. Dump full page HTML for structure inspection, then parse
                # --------------------------------------------------------
                raw_html = page.content()
                debug_path = "wyoming_debug.html"
                with open(debug_path, "w", encoding="utf-8") as f:
                    f.write(raw_html)
                print(f"[wyoming DEBUG] Full page HTML written to {debug_path}")

                soup = BeautifulSoup(raw_html, "lxml")
                _debug_dump_results_table(soup)

                summary_rows = _parse_results_table(soup)
                if not summary_rows:
                    print("[wyoming] No results found.")
                    return records

                to_process = summary_rows[:MAX_RESULTS]
                print(
                    f"[wyoming] Found {len(summary_rows)} results; "
                    f"processing first {len(to_process)}."
                )

                # --------------------------------------------------------
                # 3. Visit each detail page via direct GET
                # --------------------------------------------------------
                for i, row in enumerate(to_process):
                    time.sleep(DETAIL_DELAY)

                    detail_url = row["detail_url"]
                    print(
                        f"[wyoming] Fetching detail {i + 1}/{len(to_process)}: "
                        f"{row['name']} ({row['filing_id']})"
                    )

                    try:
                        page.goto(detail_url, wait_until="domcontentloaded", timeout=30_000)
                    except PlaywrightTimeout:
                        warnings.warn(
                            f"[wyoming] Timed out loading detail page for '{row['name']}'. "
                            "Skipping.",
                            stacklevel=2,
                        )
                        continue

                    detail_html = page.content()

                    # Save the first entity's detail page for structure inspection
                    if i == 0:
                        detail_debug_path = "wyoming_detail_debug.html"
                        with open(detail_debug_path, "w", encoding="utf-8") as f:
                            f.write(detail_html)
                        print(f"[wyoming DEBUG] Detail page HTML written to {detail_debug_path}")

                    detail_soup = BeautifulSoup(detail_html, "lxml")
                    detail = _parse_detail_page(detail_soup)

                    records.append(
                        BusinessRecord(
                            name=row["name"],
                            entity_type=row.get("entity_type") or detail.get("entity_type", ""),
                            status=row.get("status") or detail.get("status", ""),
                            state="WY",
                            source_url=detail_url,
                            entity_number=row["filing_id"],
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
    """Print the raw HTML of the first few result <li> elements for debugging."""
    lis = soup.find_all(
        "li",
        class_=lambda c: c and any(rc in c for rc in RESULT_LI_CLASSES),
    )
    print(f"\n[wyoming DEBUG] Found {len(lis)} result <li> elements.")
    for li in lis[:3]:
        print(li.prettify())
    print("[wyoming DEBUG] End of result dump\n")


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract rows from the Wyoming SOS search results list.

    The live page renders results as <li class="rowRegular|rowHighlight"> elements
    inside a <ul> — there is no <table>.  Each <li> wraps a single <a> link whose
    child spans carry all field data:

      <li class="rowRegular">
        <a href="FilingDetails.aspx?eFNum=...">
          <span>
            <span class="resFile1 resultField">
              Entity Name -
              <span style="white-space:nowrap;">2021-000997292</span>
              (LLC)
            </span>
            <span class="resFile2"><span class="resultField">Status:</span> Active</span>
            <span class="resFile3"><span class="resultField">Standing - Tax:</span> Good</span>
            <span class="resFile4"><span class="resultField">Standing - RA:</span> Good</span>
            <span class="resFile5"><span class="resultField">Filed On:</span> 04/15/2021</span>
          </span>
        </a>
      </li>

    Returns a list of dicts with keys:
      name, filing_id, entity_type, status, detail_url
    """
    import re

    rows: list[dict] = []

    lis = soup.find_all(
        "li",
        class_=lambda c: c and any(rc in c for rc in RESULT_LI_CLASSES),
    )
    if not lis:
        return rows

    for li in lis:
        link = li.find("a")
        if not link:
            continue

        # ---- Detail URL ----
        href = link.get("href", "").strip()
        if href.startswith("http"):
            detail_url = href
        elif href.startswith("/"):
            detail_url = f"{BASE_URL}{href}"
        else:
            detail_url = f"{BASE_URL}/Business/{href.lstrip('/')}"

        # ---- resFile1: entity name, filing ID, entity type code ----
        res1 = link.find("span", class_=lambda c: c and "resFile1" in c)
        if not res1:
            continue

        # The filing ID sits inside a <span style="white-space:nowrap;"> in resFile1
        id_span = res1.find("span")
        filing_id = id_span.get_text(strip=True) if id_span else ""

        # Entity name: the first NavigableString in res1 before the id_span.
        # e.g. "Apple & Banana LLC -"  → strip trailing " -"
        # Must use isinstance(child, Tag) — NavigableString also has .name (=None)
        # so hasattr() cannot distinguish the two types.
        entity_name = ""
        for child in res1.children:
            if isinstance(child, Tag):          # hit the filing-ID span — stop
                break
            text = str(child).strip()
            if text:
                entity_name = text
                break
        entity_name = entity_name.rstrip(" -").strip()
        if not entity_name:
            continue

        # Entity type: abbreviation in parentheses at end of resFile1 text
        # e.g. "(LLC)", "(CORP)", "(TN)"
        res1_full = res1.get_text(" ", strip=True)
        type_match = re.search(r"\(([^)]+)\)\s*$", res1_full)
        entity_type = type_match.group(1) if type_match else ""

        # ---- resFile2: status ----
        # Structure: <span class="resFile2"><span class="resultField">Status:</span> Active</span>
        status = ""
        res2 = link.find("span", class_=lambda c: c and "resFile2" in c)
        if res2:
            label = res2.find("span", class_="resultField")
            full_text = res2.get_text(" ", strip=True)
            if label:
                label_text = label.get_text(strip=True)
                status = full_text[len(label_text):].strip()
            else:
                status = full_text

        rows.append({
            "name": entity_name,
            "filing_id": filing_id,
            "entity_type": entity_type,
            "status": status,
            "detail_url": detail_url,
        })

    return rows


def _parse_detail_page(soup: BeautifulSoup) -> dict:
    """
    Extract fields from a Wyoming SOS entity detail page.

    The live page exposes every data point in a <span> or <div> with a
    deterministic ID — confirmed from wyoming_detail_debug.html:

      span#txtFilingType   — entity type  e.g. "Limited Liability Company - Domestic"
      span#txtStatus       — status       e.g. "Inactive - Administratively Dissolved (Tax)"
      span#txtInitialDate  — filing date  e.g. "04/15/2021"
      span#txtAgentName    — RA name      e.g. "FBRA LLC"
        (inside div#collapse1, which is the "Additional Details" accordion panel)

    We read these IDs directly rather than scanning label/value pairs.

    Returns a dict with keys: entity_type, status, registered_agent,
    incorporation_date.
    """
    def _sid(span_id: str) -> str:
        """Return stripped text of the element with the given id, or ''."""
        tag = soup.find(id=span_id)
        return tag.get_text(strip=True) if tag else ""

    # Entity type — full human-readable name from the detail page
    # e.g. "Limited Liability Company - Domestic"
    entity_type = _sid("txtFilingType")

    # Status — full status string including sub-status when dissolved
    # e.g. "Inactive - Administratively Dissolved (Tax)"
    status = _sid("txtStatus")

    # Registered agent name — lives inside the accordion's #collapse1 div
    # e.g. "FBRA LLC"
    agent = _sid("txtAgentName")

    # Original filing / incorporation date — "MM/DD/YYYY"
    # span#txtInitialDate holds the date the filing was first submitted
    inc_raw = _sid("txtInitialDate")

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
# Quick test — run directly: python -m scrapers.business.wyoming
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pprint

    print("Searching Wyoming SOS for 'Apple' ...")
    scraper = WyomingScraper()
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
