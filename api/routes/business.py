"""
Business entity search and lookup endpoints.

Routes (all require X-API-Key header):
  GET /search?name={name}&state={state}
      Search by name + state. Returns cached DB results if fresh (<24 h),
      otherwise runs the appropriate state scraper, persists results, and
      returns the fresh data.

  GET /lookup?entity_number={entity_number}&state={state}
      Look up a single business by its state-assigned entity number.
      Only queries the database — does not trigger a scrape.

  GET /states
      List all states supported by this API.
"""

from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from api.auth import APIKey, verify_api_key
from api.database import get_db
from api.models.business import Business
from api.schemas.business import (
    BusinessResponse,
    BusinessSearchResponse,
    StateInfo,
    StatesResponse,
)
from scrapers.business.delaware import DelawareScraper, save_records as de_save
from scrapers.business.florida import FloridaScraper, save_records as fl_save
from scrapers.business.texas import TexasScraper, save_records as tx_save
from scrapers.business.wyoming import WyomingScraper, save_records as wy_save
from scrapers.business.base import BusinessRecord, BusinessScraper

router = APIRouter(tags=["business"])

# ---------------------------------------------------------------------------
# Supported states
# ---------------------------------------------------------------------------

SUPPORTED_STATES: dict[str, str] = {
    "DE": "Delaware",
    "FL": "Florida",
    "TX": "Texas",
    "WY": "Wyoming",
}

# ---------------------------------------------------------------------------
# Scraper registry — maps state code → (scraper class, save_records fn)
# ---------------------------------------------------------------------------

_SCRAPERS: dict[str, tuple[type[BusinessScraper], callable]] = {
    "DE": (DelawareScraper, de_save),
    "FL": (FloridaScraper, fl_save),
    "TX": (TexasScraper, tx_save),
    "WY": (WyomingScraper, wy_save),
}

# Records younger than this are considered fresh — no scrape needed
_CACHE_TTL = timedelta(hours=24)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_stale(records: list[Business]) -> bool:
    """Return True if the most-recently-updated record is older than 24 hours."""
    if not records:
        return True
    newest = max(r.last_updated for r in records)
    # last_updated is stored as a naive UTC datetime
    age = datetime.utcnow() - newest.replace(tzinfo=None)
    return age > _CACHE_TTL


def _db_search(db: Session, name: str, state: str) -> list[Business]:
    """Query the DB for businesses matching name (case-insensitive) and state."""
    return (
        db.query(Business)
        .filter(
            Business.name.ilike(f"%{name}%"),
            Business.state == state,
        )
        .order_by(Business.last_updated.desc())
        .all()
    )


def _run_scraper(
    scraper_cls: type[BusinessScraper],
    save_fn: callable,
    name: str,
    db: Session,
) -> list[Business]:
    """
    Run *scraper_cls* for *name*, persist with *save_fn*, and return the
    freshly-saved Business ORM objects.
    """
    scraper = scraper_cls()
    records: list[BusinessRecord] = scraper.search(name)
    if not records:
        return []
    saved = save_fn(records, db)
    return saved


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/search",
    response_model=BusinessSearchResponse,
    summary="Search for businesses by name and state",
)
def search_businesses(
    name: str = Query(..., min_length=2, description="Business name to search for"),
    state: str = Query(..., min_length=2, max_length=2, description="2-letter state code"),
    _api_key: APIKey = Depends(verify_api_key),
    db: Session = Depends(get_db),
) -> BusinessSearchResponse:
    """
    Search for businesses matching *name* in the given *state*.

    1. Checks the database for existing records (case-insensitive name match).
    2. If records exist **and** are younger than 24 hours, returns them
       immediately (`cached: true`).
    3. Otherwise runs the appropriate state scraper, upserts results into the
       database, and returns the fresh data (`cached: false`).

    Supported states: DE, FL, TX, WY.
    """
    state_upper = state.upper()

    if state_upper not in _SCRAPERS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"State '{state_upper}' is not supported. "
                f"Supported states: {', '.join(sorted(_SCRAPERS))}."
            ),
        )

    # --- 1. Cache check ---
    cached_records = _db_search(db, name, state_upper)
    if cached_records and not _is_stale(cached_records):
        return BusinessSearchResponse(
            results=cached_records,
            total=len(cached_records),
            state=state_upper,
            cached=True,
        )

    # --- 2. Scrape ---
    scraper_cls, save_fn = _SCRAPERS[state_upper]
    try:
        fresh_records = _run_scraper(scraper_cls, save_fn, name, db)
    except Exception as exc:
        # Scraper failed — fall back to stale cache if available, else 502
        if cached_records:
            return BusinessSearchResponse(
                results=cached_records,
                total=len(cached_records),
                state=state_upper,
                cached=True,
            )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Scraper failed for state {state_upper}: {exc}",
        ) from exc

    if not fresh_records:
        return BusinessSearchResponse(
            results=[],
            total=0,
            state=state_upper,
            cached=False,
        )

    # Re-query the DB so the response reflects the full upserted rows
    # (the save_fn may have returned ORM objects detached from *db*)
    results = _db_search(db, name, state_upper)
    return BusinessSearchResponse(
        results=results,
        total=len(results),
        state=state_upper,
        cached=False,
    )


@router.get(
    "/lookup",
    response_model=BusinessResponse,
    summary="Look up a business by entity number and state",
)
def lookup_business(
    entity_number: str = Query(..., description="State-assigned entity / taxpayer number"),
    state: str = Query(..., min_length=2, max_length=2, description="2-letter state code"),
    _api_key: APIKey = Depends(verify_api_key),
    db: Session = Depends(get_db),
) -> BusinessResponse:
    """
    Return the database record for a specific business identified by its
    state-issued entity number and state code.

    This endpoint only queries the database — it does not trigger a scrape.
    Use `/search` first to populate the database if needed.
    """
    state_upper = state.upper()

    business = (
        db.query(Business)
        .filter(
            Business.entity_number == entity_number,
            Business.state == state_upper,
        )
        .first()
    )

    if business is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No business found with entity number '{entity_number}' "
                f"in state '{state_upper}'. "
                "Try /search to populate the database first."
            ),
        )

    return business


@router.get(
    "/states",
    response_model=StatesResponse,
    summary="List all supported states",
)
def list_states(
    _api_key: APIKey = Depends(verify_api_key),
) -> StatesResponse:
    """
    Return a list of all US states currently supported by the Provero
    business search API, along with their 2-letter state codes.
    """
    state_list = [
        StateInfo(code=code, name=name)
        for code, name in sorted(SUPPORTED_STATES.items())
    ]
    return StatesResponse(states=state_list, total=len(state_list))
