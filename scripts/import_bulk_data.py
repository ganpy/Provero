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
from api.models.license import License

# ---------------------------------------------------------------------------
# Bulk data source URLs
# ---------------------------------------------------------------------------

SOURCES = {
    "NY": {
        "url": "https://data.ny.gov/api/views/n9v6-gdp6/rows.csv?accessType=DOWNLOAD",
        "description": "New York Active Businesses (data.ny.gov)",
        "format": "csv",
    },
    "TX_LICENSE": {
        "url": "https://data.texas.gov/api/views/7358-krk7/rows.csv?accessType=DOWNLOAD",
        "description": "Texas TDLR All Licenses",
        "format": "csv",
    },
    "CT_LICENSE": {
        "url": "https://data.ct.gov/api/views/fxib-2xng/rows.csv?accessType=DOWNLOAD",
        "description": "Connecticut State Licenses and Credentials",
        "format": "csv",
    },
    "OR_LICENSE": {
        "url": "https://data.oregon.gov/api/views/vhbr-cuaq/rows.csv?accessType=DOWNLOAD",
        "description": "Oregon Contractor and Trade Licenses",
        "format": "csv",
    },
    "WA_LICENSE": {
        "url": "https://data.wa.gov/api/views/m8qx-ubtq/rows.csv?accessType=DOWNLOAD",
        "description": "Washington State L&I Contractor Licenses",
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
    "CO": {
        "url": "https://data.colorado.gov/api/views/4ykn-tg5h/rows.csv?accessType=DOWNLOAD",
        "description": "Colorado Business Entities",
        "format": "csv",
    },
    "IA": {
        "url": "https://data.iowa.gov/resource/ez5t-3qay.csv?$limit=500000",
        "description": "Iowa Active Business Entities (data.iowa.gov)",
        "format": "csv",
    },
    "OR": {
        "url": "https://data.oregon.gov/api/views/tckn-sxa6/rows.csv?accessType=DOWNLOAD",
        "description": "Oregon Active Business Registrations (data.oregon.gov)",
        "format": "csv",
    },
    "CT": {
        "url": "https://data.ct.gov/resource/n7gp-d28j.csv?$limit=500000&$where=status='Active'",
        "description": "Connecticut Business Registry - Active (data.ct.gov)",
        "format": "csv",
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


def _parse_co_row(row: dict) -> Optional[dict]:
    name = (row.get("entityname") or "").strip()
    if not name:
        return None
    agent_first = (row.get("agentfirstname") or "").strip()
    agent_last = (row.get("agentlastname") or "").strip()
    agent_org = (row.get("agentorganizationname") or "").strip()
    registered_agent = (f"{agent_first} {agent_last}".strip() or agent_org) or None
    return {
        "name": name,
        "entity_type": (row.get("entitytype") or "").strip(),
        "status": (row.get("entitystatus") or "").strip(),
        "state": "CO",
        "entity_number": (row.get("entityid") or "").strip() or None,
        "registered_agent": registered_agent,
        "incorporation_date": _parse_date(row.get("entityformdate") or ""),
        "source_url": "https://data.colorado.gov/Business/Business-Entities-in-Colorado/4ykn-tg5h",
    }


def _parse_ia_row(row: dict) -> Optional[dict]:
    name = row.get("legal_name", "").strip()
    if not name:
        return None
    return {
        "name": name,
        "entity_number": row.get("corp_number", "").strip(),
        "entity_type": row.get("corporation_type", "").strip(),
        "status": "Active",
        "state": "IA",
        "incorporation_date": _parse_date(row.get("effective_date", "")),
        "registered_agent": row.get("registered_agent", "").strip() or None,
        "source_url": f"https://data.iowa.gov/resource/ez5t-3qay/{row.get('corp_number', '')}",
    }


def _parse_or_row(row: dict) -> Optional[dict]:
    name = row.get("Business Name", "").strip()
    if not name:
        return None
    return {
        "name": name,
        "entity_number": row.get("Registry Number", "").strip(),
        "entity_type": row.get("Entity Type", "").strip(),
        "status": "Active",
        "state": "OR",
        "incorporation_date": _parse_date(row.get("Registry Date", "")),
        "registered_agent": None,
        "source_url": row.get("Business Details", "").strip() or None,
    }


def _parse_ct_row(row: dict) -> Optional[dict]:
    name = row.get("name", "").strip()
    if not name:
        return None
    return {
        "name": name,
        "entity_number": row.get("accountnumber", "").strip(),
        "entity_type": row.get("business_type", "").strip(),
        "status": row.get("status", "").strip(),
        "state": "CT",
        "incorporation_date": _parse_date(row.get("date_registration", "")),
        "registered_agent": None,
        "source_url": "https://data.ct.gov/resource/n7gp-d28j",
    }


PARSERS = {
    "NY": _parse_ny_row,
    "FL": _parse_fl_row,
    "CA": _parse_ca_row,
    "CO": _parse_co_row,
    "IA": _parse_ia_row,
    "OR": _parse_or_row,
    "CT": _parse_ct_row,
}


def _parse_ct_license_row(row: dict) -> Optional[dict]:
    full_name = (row.get("Name") or "").strip()
    license_number = (row.get("CredentialNumber") or "").strip()
    if not license_number:
        return None
    if not full_name and not license_number:
        return None
    return {
        "full_name": full_name or license_number,
        "license_type": (row.get("Credential") or "").strip(),
        "license_number": license_number or None,
        "state": "CT",
        "status": (row.get("Status") or "").strip().capitalize(),
        "issued_date": _parse_date(row.get("IssueDate") or ""),
        "expiry_date": _parse_date(row.get("ExpirationDate") or ""),
        "source_url": "https://data.ct.gov/Business/State-Licenses-and-Credentials/fxib-2xng",
    }


def _parse_tx_license_row(row: dict) -> Optional[dict]:
    full_name = (row.get("OWNER NAME") or row.get("BUSINESS NAME") or "").strip()
    license_number = (row.get("LICENSE NUMBER") or "").strip()
    if not full_name and not license_number:
        return None
    expiry = _parse_date(row.get("LICENSE EXPIRATION DATE (MMDDCCYY)") or "")
    today = date.today()
    status = "Expired" if expiry and expiry < today else "Active"
    return {
        "full_name": full_name or license_number,
        "license_type": (row.get("LICENSE TYPE") or "").strip(),
        "license_number": license_number or None,
        "state": "TX",
        "status": status,
        "issued_date": None,
        "expiry_date": expiry,
        "source_url": "https://www.tdlr.texas.gov/",
    }

def _parse_or_license_row(row: dict) -> Optional[dict]:
    full_name = (row.get("Full_Name") or row.get("DBA") or "").strip()
    license_number = (row.get("LicNbr") or "").strip()
    if not full_name and not license_number:
        return None
    return {
        "full_name": full_name or license_number,
        "license_type": (row.get("Profession") or "").strip(),
        "license_number": license_number or None,
        "state": "OR",
        "status": (row.get("Lic_Status") or "").strip(),
        "issued_date": None,
        "expiry_date": _parse_date(row.get("Expiration_Date") or ""),
        "source_url": "https://www.oregon.gov/bcd/licensing",
    }


def _parse_wa_license_row(row: dict) -> Optional[dict]:
    full_name = (row.get("PrimaryPrincipalName") or row.get("BusinessName") or "").strip()
    license_number = (row.get("ContractorLicenseNumber") or "").strip()
    if not full_name and not license_number:
        return None
    return {
        "full_name": full_name or license_number,
        "license_type": (row.get("ContractorLicenseTypeCodeDesc") or "").strip(),
        "license_number": license_number or None,
        "state": "WA",
        "status": (row.get("ContractorLicenseStatus") or "").strip().capitalize(),
        "issued_date": _parse_date(row.get("LicenseEffectiveDate") or ""),
        "expiry_date": _parse_date(row.get("LicenseExpirationDate") or ""),
        "source_url": "https://data.wa.gov/Labor/L-I-Contractor-License-Data-General/m8qx-ubtq",
    }


LICENSE_PARSERS = {
    "TX_LICENSE": _parse_tx_license_row,
    "CT_LICENSE": _parse_ct_license_row,
    "OR_LICENSE": _parse_or_license_row,
    "WA_LICENSE": _parse_wa_license_row,
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


def _upsert_license_batch(batch: list[dict], db: Session):
    now = datetime.utcnow()
    db.bulk_save_objects([
        License(
            full_name=rec["full_name"],
            license_type=rec["license_type"] or "",
            license_number=rec["license_number"],
            state=rec["state"],
            status=rec["status"] or "",
            issued_date=rec["issued_date"],
            expiry_date=rec["expiry_date"],
            last_updated=now,
            source_url=rec["source_url"],
        )
        for rec in batch
    ])
    db.commit()


def import_license_csv(file_path: Path, state: str, db: Session, limit: int = 0) -> int:
    parser = LICENSE_PARSERS[state]
    count = 0
    skipped = 0
    batch = []
    BATCH_SIZE = 500

    print(f"[{state}] Reading {file_path} ...")

    with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
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
                _upsert_license_batch(batch, db)
                count += len(batch)
                batch = []
                print(f"[{state}] Imported {count:,} records so far ...", end="\r")

        if batch:
            _upsert_license_batch(batch, db)
            count += len(batch)

    print(f"\n[{state}] Done. Imported {count:,} records. Skipped {skipped:,} empty rows.")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Import bulk SOS business / license data")
    parser.add_argument("--state", required=True,
                        choices=["NY", "FL", "CA", "CO", "IA", "OR", "CT", "TX_LICENSE", "CT_LICENSE", "OR_LICENSE", "WA_LICENSE"],
                        help="State / dataset to import")
    parser.add_argument("--type", dest="import_type", default="business",
                        choices=["business", "license"],
                        help="Record type to import (default: business)")
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
        if args.import_type == "license":
            total = import_license_csv(file_path, state, db, limit=args.limit)
            print(f"\n✅ Successfully imported {total:,} {state} license records.")
        else:
            total = import_csv(file_path, state, db, limit=args.limit)
            print(f"\n✅ Successfully imported {total:,} {state} business records.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
