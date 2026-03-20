import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from api.database import Base


class License(Base):
    __tablename__ = "licenses"

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, default=uuid.uuid4
    )
    full_name: Mapped[str] = mapped_column(String, nullable=False)
    license_type: Mapped[str] = mapped_column(String, nullable=False)
    license_number: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String(2), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    issued_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expiry_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    source_url: Mapped[str] = mapped_column(String, nullable=False)
