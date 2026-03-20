"""
Pydantic schemas for the Business API endpoints.
"""

import uuid
from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class BusinessResponse(BaseModel):
    """Full business entity as returned by search and lookup endpoints."""

    id: uuid.UUID
    name: str
    entity_number: Optional[str]
    entity_type: str
    status: str
    state: str
    registered_agent: Optional[str]
    incorporation_date: Optional[date]
    source_url: str
    last_updated: datetime

    model_config = {"from_attributes": True}


class BusinessSearchResponse(BaseModel):
    """Envelope returned by GET /business/search."""

    results: list[BusinessResponse]
    total: int
    state: str
    cached: bool  # True if served from DB cache, False if freshly scraped


class StateInfo(BaseModel):
    """A single entry in the supported-states list."""

    code: str
    name: str


class StatesResponse(BaseModel):
    """Envelope returned by GET /business/states."""

    states: list[StateInfo]
    total: int
