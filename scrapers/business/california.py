"""
California Secretary of State — business entity scraper.

Uses the legacy public search (no login required):
  https://businesssearch.sos.ca.gov/
"""

import time
import warnings
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from sqlalchemy.orm import Session

from api.database import SessionLocal
from api.models.business import Business
from scrapers.business.base import BusinessRecord, BusinessScraper

SEARCH_URL = "https://businesssearch.sos.ca.gov/CBS/SearchResults"
BASE_URL   = "https://businesssearch.sos.ca.gov"
MAX_RESULTS = 3
REQUEST_DELAY = 1

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://businesssearch.sos.ca.gov/",
}


def _parse_date(raw: str) -> Optional[date]:
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _text(tag) -> str:
    return tag.get_text(strip=True) if tag else ""


class CaliforniaScraper(BusinessScraper):
    state_code = "CA"

    def search(self, name: str) -> list[BusinessRecord]:
        records: list[BusinessRecord] = []
        session = requests.Session()
        session.headers.update(HEADERS)

        # Step 1: GET the search page to grab any hidden form fields / cookies
        print(f"[california] Loading search page: {BASE_URL}/CBS/SearchResults")
        resp = session.get(f"{BASE_URL}/CBS/SearchResults", params={
            "filing_type": "ALL",
            "status": "ACTIVE",
            "number_of_rows": "10",
            "keyword": name,
        }, timeout=30)

        soup = BeautifulSoup(resp.text, "lxml")

        # Step 2: Parse results table
        rows = _parse_results_table(soup)
        if not rows:
            print("[california] No results found.")
            return records

        to_process = rows[:MAX_RESULTS]
        print(f"[california] Found {len(rows)} results; processing first {len(to_process)}.")

        # Step 3: Fetch detail pages
        for i, row in enumerate(to_process):
            time.sleep(REQUEST_DELAY)
            detail_url = row.get("detail_url", "")
            if not detail_url:
                continue

            print(f"[california] Detail {i+1}/{len(to_process)}: {row['name']}")
            try:
                dresp = session.get(detail_url, timeout=30)
                dsoup = BeautifulSoup(dresp.text, "lxml")
                detail = _parse_detail_page(dsoup)
            except Exception as e:
                warnings.warn(f"[california] Detail fetch failed: {e}")
                detail = {}

            records.append(BusinessRecord(
                name=row["name"],
                entity_type=row.get("entity_type") or detail.get("entity_type", ""),
                status=row.get("status") or detail.get("status", ""),
                state="CA",
                source_url=detail_url,
                entity_number=row.get("entity_number") or None,
                registered_agent=detail.get("registered_agent"),
                incorporation_date=detail.get("incorporation_date"),
                last_updated=datetime.utcnow(),
            ))

        return records


def _parse_results_table(soup: BeautifulSoup) -> list[dict]:
    rows = []
    table = soup.find("table", id="SearchResults")
    if table is None:
        # fallback: any table with entity-looking headers
        for t in soup.find_all("table"):
            txt = t.get_text(" ").lower()
            if "entity" in txt or "status" in txt:
                table = t
                break
    if table is None:
        return rows

    all_rows = table.find_all("tr")
    if len(all_rows) < 2:
        return rows

    headers = [_text(th).lower() for th in all_rows[0].find_all(["th", "td"])]

    def col(candidates):
        for i, h in enumerate(headers):
            for c in candidates:
                if c in h:
                    return i
        return None

    c_name   = col(("entity name", "business name", "name"))
    c_num    = col(("entity number", "number", "file"))
    c_type   = col(("entity type", "type"))
    c_status = col(("status",))

    for tr in all_rows[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        name_cell = cells[c_name] if c_name is not None else cells[0]
        link = name_cell.find("a")
        entity_name = _text(link) if link else _text(name_cell)
        if not entity_name:
            continue

        detail_url = ""
        if link and link.get("href"):
            href = link["href"]
            detail_url = href if href.startswith("http") else urljoin(BASE_URL, href)

        entity_number = _text(cells[c_num]) if c_num is not None and c_num < len(cells) else ""
        entity_type   = _text(cells[c_type]) if c_type is not None and c_type < len(cells) else ""
        status        = _text(cells[c_status]) if c_status is not None and c_status < len(cells) else ""

        rows.append({
            "name": entity_name,
            "entity_number": entity_number,
            "entity_type": entity_type,
            "status": status,
            "detail_url": detail_url,
        })

    return rows


def _parse_detail_page(soup: BeautifulSoup) -> dict:
    result = {
        "entity_type": "",
        "status": "",
        "registered_agent": None,
        "incorporation_date": None,
    }

    lv = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) >= 2:
            lbl = _text(cells[0]).rstrip(":").lower()
            val = _text(cells[1])
            if lbl and val:
                lv[lbl] = val

    def get(*keys):
        for k in keys:
            kl = k.lower()
            if kl in lv:
                return lv[kl]
            for lk, lval in lv.items():
                if kl in lk:
                    return lval
        return ""

    result["entity_type"]  = get("entity type", "type")
    result["status"]       = get("status", "standing")
    agent = get("agent for service", "registered agent", "agent name", "agent")
    result["registered_agent"] = agent or None
    inc_raw = get("initial filing date", "filing date", "formation date",
                  "incorporation date", "date filed", "registration date")
    result["incorporation_date"] = _parse_date(inc_raw) if inc_raw else None

    return result


def save_records(records: list[BusinessRecord], db: Session) -> list[Business]:
    saved = []
    for rec in records:
        existing = (
            db.query(Business)
            .filter(Business.name == rec.name, Business.state == rec.state,
                    Business.source_url == rec.source_url)
            .first()
        )
        if existing:
            existing.entity_type      = rec.entity_type or existing.entity_type
            existing.status           = rec.status or existing.status
            existing.entity_number    = rec.entity_number or existing.entity_number
            existing.registered_agent = rec.registered_agent or existing.registered_agent
            existing.incorporation_date = rec.incorporation_date or existing.incorporation_date
            existing.last_updated     = datetime.utcnow()
            saved.append(existing)
        else:
            b = Business(
                name=rec.name, entity_type=rec.entity_type or "",
                status=rec.status or "", state=rec.state,
                entity_number=rec.entity_number,
                registered_agent=rec.registered_agent,
                incorporation_date=rec.incorporation_date,
                last_updated=rec.last_updated, source_url=rec.source_url,
            )
            db.add(b)
            saved.append(b)
    db.commit()
    return saved


if __name__ == "__main__":
    import pprint
    print("Searching California Secretary of State for 'Apple' ...")
    scraper = CaliforniaScraper()
    results = scraper.search("Apple")
    print(f"\nTotal results returned: {len(results)}")
    print("\n--- First 3 results ---\n")
    for rec in results[:3]:
        pprint.pprint({
            "name": rec.name,
            "entity_type": rec.entity_type,
            "status": rec.status,
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
