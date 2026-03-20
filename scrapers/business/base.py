"""
Base class for all state business-entity scrapers.

Each state scraper must subclass BusinessScraper and implement search().
The contract: search() returns a list of dicts that are ready to be upserted
into the Business table.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class BusinessRecord:
    """Normalised result returned by every state scraper."""

    name: str
    entity_type: str
    status: str
    state: str                          # 2-letter code
    source_url: str
    entity_number: Optional[str] = None
    registered_agent: Optional[str] = None
    officers: Optional[dict] = None
    incorporation_date: Optional[date] = None
    last_updated: datetime = field(default_factory=datetime.utcnow)


class BusinessScraper(ABC):
    """
    Abstract base for state business-entity scrapers.

    Subclasses implement search() and optionally override _fetch_detail()
    for detail-page enrichment.
    """

    # Subclasses set this to the 2-letter state code they cover.
    state_code: str = ""

    @abstractmethod
    def search(self, name: str) -> list[BusinessRecord]:
        """
        Search the state registry for entities matching *name*.

        Returns a list of BusinessRecord objects (may be empty).
        Implementations should:
          1. Load the search page (via Playwright or requests).
          2. Submit the name query.
          3. Parse the results table.
          4. Optionally visit each detail page for enrichment.
          5. Return the list — do NOT write to the DB here.
        """

    def _fetch_detail(self, detail_url: str) -> dict:
        """
        Optional hook: visit a detail page and return a dict of extra fields.

        The returned dict is merged into the BusinessRecord by the caller.
        Override in subclasses that support detail-page enrichment.
        """
        return {}
