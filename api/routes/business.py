from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from api.database import get_db
from api.auth import verify_api_key
from api.models.business import Business
from api.schemas.business import BusinessResponse, BusinessSearchResponse

router = APIRouter()

SUPPORTED_STATES = {
    "DE": "Delaware",
    "WY": "Wyoming",
    "TX": "Texas",
    "NY": "New York",
}

@router.get("/search", response_model=BusinessSearchResponse)
def search_business(
    name: str = Query(..., min_length=2),
    state: str = Query(..., min_length=2, max_length=2),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    state = state.upper()
    if state not in SUPPORTED_STATES:
        raise HTTPException(
            status_code=400,
            detail=f"State '{state}' not supported. Supported states: {', '.join(SUPPORTED_STATES.keys())}"
        )

    results = (
        db.query(Business)
        .filter(
            Business.state == state,
            Business.name.ilike(f"%{name}%")
        )
        .limit(10)
        .all()
    )

    return BusinessSearchResponse(
        results=[BusinessResponse.model_validate(r) for r in results],
        total=len(results),
        state=state,
        cached=True,
    )

@router.get("/lookup/{entity_number}", response_model=BusinessResponse)
def lookup_business(
    entity_number: str,
    state: str = Query(..., min_length=2, max_length=2),
    api_key=Depends(verify_api_key),
    db: Session = Depends(get_db),
):
    state = state.upper()
    result = (
        db.query(Business)
        .filter(Business.entity_number == entity_number, Business.state == state)
        .first()
    )
    if not result:
        raise HTTPException(status_code=404, detail="Business not found")
    return BusinessResponse.model_validate(result)

@router.get("/states")
def list_states(api_key=Depends(verify_api_key)):
    return {"states": [{"code": k, "name": v} for k, v in SUPPORTED_STATES.items()]}
