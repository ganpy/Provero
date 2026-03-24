from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

import api.models  # noqa: F401 — registers all models with Base.metadata
from api.auth import (
    APIKey,
    APIKeyCreateRequest,
    APIKeyCreateResponse,
    generate_api_key,
)
from api.database import Base, engine, get_db
from api.routes.billing import router as billing_router
from api.routes.business import router as business_router
from api.routes.license import router as license_router

app = FastAPI(
    title="Provero API",
    version="0.1.0",
    description="Business verification and license lookup API",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(billing_router, prefix="/billing")
app.include_router(business_router, prefix="/business")
app.include_router(license_router, prefix="/license")


@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)


@app.get("/health", tags=["health"])
def health_check():
    return {"status": "ok"}


@app.post(
    "/internal/api-keys",
    response_model=APIKeyCreateResponse,
    tags=["internal"],
    summary="Issue a new API key to a customer",
)
def create_api_key(
    payload: APIKeyCreateRequest,
    db: Session = Depends(get_db),
) -> APIKeyCreateResponse:
    api_key = APIKey(
        key=generate_api_key(),
        owner_name=payload.owner_name,
        owner_email=payload.owner_email,
        tier=payload.tier,
    )
    db.add(api_key)
    db.commit()
    db.refresh(api_key)
    return api_key
