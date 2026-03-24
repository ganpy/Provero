import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from api.auth import verify_api_key
from api.database import get_db
from api.models.license import License

router = APIRouter()

SUPPORTED_STATES = ["TX"]


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class LicenseResponse(BaseModel):
    id: uuid.UUID
    full_name: str
    license_type: str
    license_number: str
    state: str
    status: str
    issued_date: Optional[date]
    expiry_date: Optional[date]
    source_url: str

    model_config = {"from_attributes": True}


class LicenseSearchResponse(BaseModel):
    results: list[LicenseResponse]
    total: int
    state: str
    cached: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/search", response_model=LicenseSearchResponse, tags=["license"])
def search_license(
    name: str = Query(..., min_length=2),
    state: str = Query(..., min_length=2, max_length=2),
    license_type: Optional[str] = Query(None),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    state = state.upper()

    q = db.query(License).filter(
        License.state == state,
        License.full_name.ilike(f"%{name}%"),
    )
    if license_type:
        q = q.filter(License.license_type.ilike(f"%{license_type}%"))

    results = q.limit(10).all()

    return LicenseSearchResponse(
        results=[LicenseResponse.model_validate(r) for r in results],
        total=len(results),
        state=state,
        cached=True,
    )


@router.get("/lookup/{license_number}", response_model=LicenseResponse, tags=["license"])
def lookup_license(
    license_number: str,
    state: str = Query(..., min_length=2, max_length=2),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    state = state.upper()
    result = (
        db.query(License)
        .filter(License.license_number == license_number, License.state == state)
        .first()
    )
    if not result:
        raise HTTPException(status_code=404, detail="License not found")
    return LicenseResponse.model_validate(result)


@router.get("/states", tags=["license"])
def list_states(api_key=Depends(verify_api_key)):
    return {"states": SUPPORTED_STATES}
