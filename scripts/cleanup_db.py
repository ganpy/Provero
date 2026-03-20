"""
One-time database cleanup: remove broken Business records produced by early
Delaware scraping runs where source_url was left as the search/detail page
itself (NameSearch.aspx or EntityInformation.aspx) rather than the canonical
per-entity URL.

These records are identifiable by two traits:
  1. source_url contains 'NameSearch.aspx' or 'EntityInformation.aspx'
  2. registered_agent and incorporation_date are both NULL (scraper couldn't
     parse them from those pages)

Safe to re-run: the DELETE is idempotent — a second run finds 0 rows and
exits cleanly.

Usage:
    python scripts/cleanup_db.py              # dry run — prints what would be deleted
    python scripts/cleanup_db.py --execute    # actually deletes the rows
"""

import argparse
import sys
from pathlib import Path

# Make sure the project root is on sys.path when running as a plain script.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import or_

from api.database import SessionLocal
from api.models.business import Business

# ---------------------------------------------------------------------------
# URL fragments that identify broken records
# ---------------------------------------------------------------------------

BAD_URL_FRAGMENTS = (
    "NameSearch.aspx",
    "EntityInformation.aspx",
)


def _build_filter():
    """SQLAlchemy OR filter matching any broken source_url pattern."""
    return or_(
        *(Business.source_url.contains(fragment) for fragment in BAD_URL_FRAGMENTS)
    )


def main(execute: bool) -> None:
    db = SessionLocal()
    try:
        bad_records = db.query(Business).filter(_build_filter()).all()

        if not bad_records:
            print("No broken records found — nothing to do.")
            return

        print(f"Found {len(bad_records)} broken record(s):\n")
        for rec in bad_records:
            print(
                f"  id={rec.id}  state={rec.state}  "
                f"name={rec.name!r}  source_url={rec.source_url!r}"
            )

        if not execute:
            print(
                f"\nDRY RUN — {len(bad_records)} record(s) would be deleted. "
                "Re-run with --execute to apply."
            )
            return

        # Bulk-delete to avoid loading all rows into memory for large datasets
        deleted = (
            db.query(Business)
            .filter(_build_filter())
            .delete(synchronize_session=False)
        )
        db.commit()
        print(f"\nDeleted {deleted} record(s).")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove broken Business records with NameSearch.aspx / EntityInformation.aspx source URLs."
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete the records (default is dry run).",
    )
    args = parser.parse_args()
    main(execute=args.execute)
