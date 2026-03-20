"""
Bulk data importer for state SOS business entity files.

Usage:
  python scripts/import_bulk_data.py --state NY   # downloads + imports NY
  python scripts/import_bulk_data.py --state FL --file path/to/file.csv
  python scripts/import_bulk_data.py --state CA --file path/to/file.csv
"""

import argparse
import csv
import io
import sys
import zipfile
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
from sqlalchemy.orm import Session

# Make sure project root is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.database import SessionLocal
from api.models.business import Business

# ---------------------------------------------------------------------------
# Bulk data source URLs
# ---------------------------------------------------------------------------

SOURCES = {
    "NY": {
        "url": "https://data.ny.gov/api/views/n9v6-gdp6/rows.csv?accessType=DOWNLOAD",
        "description": "New York Active Businesses (data.ny.gov)",
        "format": "csv",
    },
    "FL": {
        "url": "https://dos.fl.gov/sunbiz/corporations-tools-and-resources/bulk-data-downloads/",
        "description": "Florida Sunbiz bulk data (manual download required)",
        "format": "manual",
    },
    "CA": {
        "url": "https://bizfileonline.sos.ca.gov/api/Records/businesssearch/exportBulkData",
        "description": "California SOS bulk data (manual download required)",
        "format": "manual",
    },
}

# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%B %d, %Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None

# ---------------------------------------------------------------------------
# State-specific row parsers
# Each returns a dict with keys matching Business model fields (or None to skip)
# ---------------------------------------------------------------------------

def _parse_ny_row(row: dict) -> Optional[dict]:
    """
    New York Open Data actual columns:
    'DOS ID', 'Current Entity Name', 'Initial DOS Filing Date',
    'County', 'Jurisdiction', 'Entity Type', 'Registered Agent Name'
    """
    name = (row.get("Current Entity Name") or "").strip()
    if not name:
        return None

    return {
        "name": name,
        "entity_number": (row.get("DOS ID") or "").strip() or None,
        "entity_type": (row.get("Entity Type") or "").strip(),
        "status": "Active",
        "state": "NY",
        "incorporation_date": _parse_date(row.get("Initial DOS Filing Date", "")),
        "registered_agent": (row.get("Registered Agent Name") or "").strip() or None,
        "source_url": "https://data.ny.gov/Economic-Development/Active-Corporations-Beginning-1800/n9v6-gdp6",
    }


def _parse_fl_row(row: dict) -> Optional[dict]:
    """
    Florida Sunbiz CSV columns vary by file type.
    Common columns: 'Name', 'Document Number', 'FEI/EIN Number',
    'Date Filed', 'State', 'Status', 'Last Event', 'Event Date Filed', 'Event Effective Date'
    """
    name = (row.get("Name") or row.get("CORP NAME") or row.get("Corp Name") or "").strip()
    if not name:
        return None

    return {
        "name": name,
        "entity_number": (row.get("Document Number") or row.get("DOCUMENT NUMBER") or "").strip() or None,
        "entity_type": (row.get("FEI/EIN Number") or "").strip() or "",  # FL doesn't always have type in bulk
        "status": (row.get("Status") or row.get("STATUS") or "").strip(),
        "state": "FL",
        "incorporation_date": _parse_date(row.get("Date Filed") or row.get("DATE FILED") or ""),
        "registered_agent": None,
        "source_url": "https://dos.fl.gov/sunbiz/",
    }


def _parse_ca_row(row: dict) -> Optional[dict]:
    """
    California SOS bulk CSV columns:
    'Entity Name', 'Entity Number', 'Registration Date',
    'Entity Type', 'Entity Status', 'Agent for Service of Process'
    """
    name = (row.get("Entity Name") or "").strip()
    if not name:
        return None

    return {
        "name": name,
        "entity_number": (row.get("Entity Number") or "").strip() or None,
        "entity_type": (row.get("Entity Type") or "").strip(),
        "status": (row.get("Entity Status") or "").strip(),
        "state": "CA",
        "incorporation_date": _parse_date(row.get("Registration Date") or row.get("Formation Date") or ""),
        "registered_agent": (row.get("Agent for Service of Process") or "").strip() or None,
        "source_url": "https://bizfileonline.sos.ca.gov",
    }


PARSERS = {
    "NY": _parse_ny_row,
    "FL": _parse_fl_row,
    "CA": _parse_ca_row,
}

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(url: str, state: str) -> Path:
    print(f"[{state}] Downloading from {url} ...")
    resp = requests.get(url, stream=True, timeout=120, headers={
        "User-Agent": "Mozilla/5.0 (compatible; Provero/1.0)"
    })
    resp.raise_for_status()

    out_path = Path(f"/tmp/provero_{state.lower()}_bulk.csv")

    # Handle ZIP files
    content_type = resp.headers.get("Content-Type", "")
    if "zip" in content_type or url.endswith(".zip"):
        zip_path = Path(f"/tmp/provero_{state.lower()}_bulk.zip")
        with open(zip_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"[{state}] Extracting ZIP ...")
        with zipfile.ZipFile(zip_path) as z:
            csv_names = [n for n in z.namelist() if n.endswith(".csv") or n.endswith(".txt")]
            if not csv_names:
                raise ValueError(f"No CSV/TXT found in ZIP: {z.namelist()}")
            z.extract(csv_names[0], "/tmp")
            out_path = Path("/tmp") / csv_names[0]
    else:
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"[{state}] Downloaded: {out_path} ({size_mb:.1f} MB)")
    return out_path

# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

def import_csv(file_path: Path, state: str, db: Session, limit: int = 0) -> int:
    parser = PARSERS[state]
    count = 0
    skipped = 0
    batch = []
    BATCH_SIZE = 500

    print(f"[{state}] Reading {file_path} ...")

    with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
        # Detect delimiter
        sample = f.read(4096)
        f.seek(0)
        delimiter = "\t" if sample.count("\t") > sample.count(",") else ","

        reader = csv.DictReader(f, delimiter=delimiter)

        for i, row in enumerate(reader):
            if limit and i >= limit:
                break

            parsed = parser(row)
            if not parsed:
                skipped += 1
                continue

            batch.append(parsed)

            if len(batch) >= BATCH_SIZE:
                _upsert_batch(batch, db)
                count += len(batch)
                batch = []
                print(f"[{state}] Imported {count:,} records so far ...", end="\r")

        if batch:
            _upsert_batch(batch, db)
            count += len(batch)

    print(f"\n[{state}] Done. Imported {count:,} records. Skipped {skipped:,} empty rows.")
    return count


def _upsert_batch(batch: list[dict], db: Session):
    for rec in batch:
        existing = (
            db.query(Business)
            .filter(Business.entity_number == rec["entity_number"],
                    Business.state == rec["state"])
            .first()
        ) if rec["entity_number"] else None

        if not existing:
            existing = (
                db.query(Business)
                .filter(Business.name == rec["name"],
                        Business.state == rec["state"])
                .first()
            )

        if existing:
            existing.entity_type = rec["entity_type"] or existing.entity_type
            existing.status = rec["status"] or existing.status
            existing.entity_number = rec["entity_number"] or existing.entity_number
            existing.incorporation_date = rec["incorporation_date"] or existing.incorporation_date
            existing.registered_agent = rec["registered_agent"] or existing.registered_agent
            existing.last_updated = datetime.utcnow()
        else:
            db.add(Business(
                name=rec["name"],
                entity_type=rec["entity_type"] or "",
                status=rec["status"] or "",
                state=rec["state"],
                entity_number=rec["entity_number"],
                registered_agent=rec["registered_agent"],
                incorporation_date=rec["incorporation_date"],
                last_updated=datetime.utcnow(),
                source_url=rec["source_url"],
            ))
    db.commit()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Import bulk SOS business data")
    parser.add_argument("--state", required=True, choices=["NY", "FL", "CA"],
                        help="State to import")
    parser.add_argument("--file", help="Path to local CSV file (skip download)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max rows to import (0 = all, use 1000 for testing)")
    args = parser.parse_args()

    state = args.state.upper()

    if args.file:
        file_path = Path(args.file)
    else:
        source = SOURCES[state]
        if source["format"] == "manual":
            print(f"\n[{state}] This state requires a manual download.")
            print(f"  Description: {source['description']}")
            print(f"  URL: {source['url']}")
            print(f"\n  Download the CSV file, then re-run with:")
            print(f"  python scripts/import_bulk_data.py --state {state} --file /path/to/file.csv\n")
            sys.exit(0)
        file_path = download_file(source["url"], state)

    db = SessionLocal()
    try:
        total = import_csv(file_path, state, db, limit=args.limit)
        print(f"\n✅ Successfully imported {total:,} {state} business records.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
