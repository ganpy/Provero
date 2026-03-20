import uuid
from datetime import date, datetime

from sqlalchemy import JSON, Date, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from api.database import Base


class Business(Base):
    __tablename__ = "businesses"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    entity_number: Mapped[str | None] = mapped_column(
        String, nullable=True, index=True
    )
    entity_type: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    registered_agent: Mapped[str | None] = mapped_column(String, nullable=True)
    officers: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    incorporation_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    source_url: Mapped[str] = mapped_column(String, nullable=False)
