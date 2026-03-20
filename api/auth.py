import os
import secrets
import uuid
from datetime import datetime

from fastapi import Depends, Header, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, Session, mapped_column

from api.database import Base, get_db


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

class APIKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    key: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    owner_name: Mapped[str] = mapped_column(String, nullable=False)
    owner_email: Mapped[str] = mapped_column(String, nullable=False)
    tier: Mapped[str] = mapped_column(String, nullable=False, default="free")
    calls_today: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    calls_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )


# ---------------------------------------------------------------------------
# Key generation
# ---------------------------------------------------------------------------

def generate_api_key() -> str:
    """Return a cryptographically secure key prefixed with pvr_."""
    token = secrets.token_urlsafe(32)
    return f"pvr_{token}"


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

def verify_api_key(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db),
) -> APIKey:
    """Validate the key, increment usage counters, and return the APIKey row."""
    api_key = db.query(APIKey).filter(APIKey.key == x_api_key).first()

    if api_key is None or not api_key.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    api_key.calls_today += 1
    api_key.calls_total += 1
    db.commit()
    db.refresh(api_key)

    return api_key


# ---------------------------------------------------------------------------
# Schemas for the internal provisioning endpoint
# ---------------------------------------------------------------------------

class APIKeyCreateRequest(BaseModel):
    owner_name: str
    owner_email: str
    tier: str = "free"


class APIKeyCreateResponse(BaseModel):
    id: uuid.UUID
    key: str
    owner_name: str
    owner_email: str
    tier: str
    created_at: datetime

    model_config = {"from_attributes": True}
