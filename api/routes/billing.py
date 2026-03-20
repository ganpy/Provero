"""
Billing endpoints.

Routes (no API-key auth — these are the entry points for new customers):

  POST /billing/subscribe
      Body: {email, tier}
      • Free tier  → creates an API key immediately and returns it.
      • Paid tiers → creates a Stripe Checkout session and returns the URL.

  POST /billing/webhook
      Stripe webhook receiver.  Must be registered in the Stripe dashboard
      pointing at https://<host>/billing/webhook.

      Handled events:
        checkout.session.completed
          → Creates an API key for the paying customer and returns it in
            the response body (Stripe ignores the body; it is included for
            observability / testing).

      The raw request body is read before any parsing so the Stripe
      signature can be verified against STRIPE_WEBHOOK_SECRET.
"""

import os

import stripe
from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from api.auth import APIKey, generate_api_key
from api.database import get_db
from billing.payments import PLANS, create_checkout_session

router = APIRouter(tags=["billing"])

WEBHOOK_SECRET: str = os.getenv("STRIPE_WEBHOOK_SECRET", "")


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class SubscribeRequest(BaseModel):
    email: EmailStr
    tier: str


class SubscribeResponse(BaseModel):
    """
    Returned by POST /billing/subscribe.

    Exactly one of `checkout_url` or `api_key` will be set:
      • checkout_url — customer must complete payment at this URL (paid tiers)
      • api_key      — key issued immediately (free tier; no payment needed)
    """
    tier: str
    checkout_url: str | None = None
    api_key: str | None = None
    message: str


class WebhookResponse(BaseModel):
    received: bool
    event_type: str
    api_key: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_api_key_record(email: str, tier: str, db: Session) -> APIKey:
    """Insert a new APIKey row and return it (caller must commit)."""
    # Derive a display name from the email address (e.g. "alice@example.com" → "alice")
    owner_name = email.split("@")[0]
    record = APIKey(
        key=generate_api_key(),
        owner_name=owner_name,
        owner_email=email,
        tier=tier,
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "/subscribe",
    response_model=SubscribeResponse,
    summary="Subscribe to a Provero plan",
    status_code=status.HTTP_200_OK,
)
def subscribe(
    payload: SubscribeRequest,
    db: Session = Depends(get_db),
) -> SubscribeResponse:
    """
    Start the subscription flow for *email* on the requested *tier*.

    **Free tier** — an API key is created immediately and returned.  No
    payment is required.

    **Paid tiers (starter / pro)** — a Stripe Checkout session is created
    and its URL is returned.  The customer completes payment on Stripe's
    hosted page; on success Stripe calls the `/billing/webhook` endpoint
    which issues the API key.

    Supported tiers: `free`, `starter`, `pro`.
    """
    tier_key = payload.tier.lower()

    if tier_key not in PLANS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Unknown tier '{tier_key}'. "
                f"Valid options: {', '.join(PLANS)}."
            ),
        )

    # Free tier: skip Stripe, issue the key immediately
    if PLANS[tier_key]["amount_cents"] == 0:
        record = _create_api_key_record(str(payload.email), tier_key, db)
        return SubscribeResponse(
            tier=tier_key,
            api_key=record.key,
            message=(
                "Free plan activated. Keep your API key safe — "
                "it will not be shown again."
            ),
        )

    # Paid tier: create a Stripe Checkout session
    try:
        checkout_url = create_checkout_session(str(payload.email), tier_key)
    except RuntimeError as exc:
        # Price IDs not configured yet (setup not run)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    except stripe.StripeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Stripe error: {exc.user_message or str(exc)}",
        ) from exc

    return SubscribeResponse(
        tier=tier_key,
        checkout_url=checkout_url,
        message=(
            "Complete your subscription at the checkout URL. "
            "Your API key will be issued after payment is confirmed."
        ),
    )


@router.post(
    "/webhook",
    response_model=WebhookResponse,
    summary="Stripe webhook receiver",
    status_code=status.HTTP_200_OK,
)
async def stripe_webhook(
    request: Request,
    db: Session = Depends(get_db),
) -> WebhookResponse:
    """
    Receive and verify Stripe webhook events.

    **Security**: the raw request body is verified against the
    `Stripe-Signature` header using `STRIPE_WEBHOOK_SECRET`.  Requests
    with an invalid or missing signature are rejected with 400.

    **Handled events**:
    - `checkout.session.completed` — payment succeeded; an API key is
      created for the customer and returned in the response body.

    All other event types are acknowledged with `received: true` and
    ignored.
    """
    # Read raw bytes — must happen before any body parsing or FastAPI
    # decodes the stream and signature verification fails.
    raw_body: bytes = await request.body()
    sig_header: str = request.headers.get("stripe-signature", "")

    # Verify the webhook signature
    try:
        event = stripe.Webhook.construct_event(
            payload=raw_body,
            sig_header=sig_header,
            secret=WEBHOOK_SECRET,
        )
    except stripe.error.SignatureVerificationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid Stripe signature: {exc}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Webhook payload error: {exc}",
        ) from exc

    event_type: str = event["type"]

    # -----------------------------------------------------------------------
    # checkout.session.completed — payment confirmed, issue API key
    # -----------------------------------------------------------------------
    if event_type == "checkout.session.completed":
        session = event["data"]["object"]

        # Metadata was attached when we created the checkout session
        metadata: dict = session.get("metadata") or {}
        email: str = metadata.get("email") or session.get("customer_email") or ""
        tier: str = metadata.get("tier", "starter")

        if not email:
            # Shouldn't happen in normal flow, but guard against it
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Webhook missing customer email in session metadata. "
                    "Cannot issue API key."
                ),
            )

        record = _create_api_key_record(email, tier, db)

        return WebhookResponse(
            received=True,
            event_type=event_type,
            api_key=record.key,
        )

    # -----------------------------------------------------------------------
    # All other events — acknowledge and ignore
    # -----------------------------------------------------------------------
    return WebhookResponse(received=True, event_type=event_type)
