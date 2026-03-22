"""
Nightly scraper job — refreshes DE, WY, TX business data in the database.
Run this as a Railway cron job.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.database import SessionLocal
from scrapers.business.delaware import DelawareScraper
from scrapers.business.wyoming import WyomingScraper
from scrapers.business.texas import TexasScraper
from scrapers.business.delaware import save_records as de_save
from scrapers.business.wyoming import save_records as wy_save
from scrapers.business.texas import save_records as tx_save

SEARCHES = ["Apple", "Google", "Microsoft", "Amazon", "Tesla", "Meta", "Inc", "LLC", "Corp"]

def run_state(scraper_class, save_fn, state_name):
    print(f"\n=== Running {state_name} scraper ===")
    scraper = scraper_class()
    db = SessionLocal()
    total = 0
    try:
        for query in SEARCHES:
            try:
                records = scraper.search(query)
                saved = save_fn(records, db)
                total += len(saved)
                print(f"  [{state_name}] '{query}' → {len(saved)} records")
            except Exception as e:
                print(f"  [{state_name}] '{query}' failed: {e}")
    finally:
        db.close()
    print(f"  [{state_name}] Total: {total} records saved/updated")

if __name__ == "__main__":
    print("Starting nightly scraper job...")
    run_state(DelawareScraper, de_save, "Delaware")
    run_state(WyomingScraper, wy_save, "Wyoming")
    run_state(TexasScraper, tx_save, "Texas")
    print("\n✅ Scraper job complete.")
