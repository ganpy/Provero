"""
Stripe billing integration for Provero.

Plan catalogue
--------------
  free    —   $0 / month,    100 calls / month  (no Stripe checkout)
  starter — $99 / month,  10 000 calls / month
  pro     — $299 / month, 100 000 calls / month

First-time setup
----------------
After adding STRIPE_SECRET_KEY to .env, run the setup utility once to create
the products and prices in your Stripe account:

    python billing/payments.py --setup

It prints STRIPE_STARTER_PRICE_ID and STRIPE_PRO_PRICE_ID values that you
then add to .env (and .env.example).  Subsequent calls to
create_checkout_session() use those IDs.

Checkout flow
-------------
  1. Client POSTs to POST /billing/subscribe {email, tier}.
  2. Server calls create_checkout_session(email, tier) → returns a Stripe
     hosted checkout URL.
  3. Customer completes payment on Stripe's page.
  4. Stripe POSTs checkout.session.completed to POST /billing/webhook.
  5. Webhook handler creates an API key and returns it in the response body.

Why billing/payments.py and not billing/stripe.py?
---------------------------------------------------
A module named stripe.py anywhere on sys.path shadows the stripe PyPI package.
Running `python billing/stripe.py` inserts billing/ at sys.path[0], making
`import stripe` import the file itself instead of the library, causing
AttributeError on stripe.Product / stripe.checkout / etc.
This module uses a safe name that can never collide with an installed package.
"""

import os
import sys

# ---------------------------------------------------------------------------
# sys.path guard — must run before `import stripe`
# ---------------------------------------------------------------------------
# When this file is executed directly (python billing/payments.py), Python
# inserts the billing/ directory at sys.path[0].  That makes every .py file
# in billing/ importable under its bare name, so `import stripe` would
# resolve to billing/stripe.py (the compat shim) instead of the real stripe
# library installed in the venv.  We filter any sys.path entry that resolves
# to the billing/ directory (comparing resolved absolute paths so both
# relative and absolute entries are matched correctly).
_billing_dir = os.path.abspath(os.path.dirname(__file__))
sys.path[:] = [p for p in sys.path if os.path.abspath(p) != _billing_dir]

import stripe
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Stripe client configuration
# ---------------------------------------------------------------------------

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

# ---------------------------------------------------------------------------
# Plan catalogue
# ---------------------------------------------------------------------------

# Each entry holds everything needed to describe the plan and build a
# Stripe checkout session.  stripe_price_id is None for the free tier
# (no payment required) and populated from env vars for paid tiers.

PLANS: dict[str, dict] = {
    "free": {
        "display_name": "Provero Free",
        "description": "100 API calls / month — no credit card required",
        "calls_per_month": 100,
        "amount_cents": 0,
        "stripe_price_id": None,
    },
    "starter": {
        "display_name": "Provero Starter",
        "description": "10,000 API calls / month at $29 / month",
        "calls_per_month": 10_000,
        "amount_cents": 2_900,
        "stripe_price_id": os.getenv("STRIPE_STARTER_PRICE_ID"),
    },
    "pro": {
        "display_name": "Provero Pro",
        "description": "100,000 API calls / month at $99 / month",
        "calls_per_month": 100_000,
        "amount_cents": 9_900,
        "stripe_price_id": os.getenv("STRIPE_PRO_PRICE_ID"),
    },
    "license_starter": {
        "display_name": "Provero License Starter",
        "description": "10,000 API calls / month at $29 / month",
        "calls_per_month": 10_000,
        "amount_cents": 2_900,
        "stripe_price_id": os.getenv("STRIPE_LICENSE_STARTER_PRICE_ID"),
    },
    "license_pro": {
        "display_name": "Provero License Pro",
        "description": "100,000 API calls / month at $99 / month",
        "calls_per_month": 100_000,
        "amount_cents": 9_900,
        "stripe_price_id": os.getenv("STRIPE_LICENSE_PRO_PRICE_ID"),
    },
    "bundle_starter": {
        "display_name": "Provero Bundle Starter",
        "description": "10,000 API calls / month at $39 / month",
        "calls_per_month": 10_000,
        "amount_cents": 3_900,
        "stripe_price_id": os.getenv("STRIPE_BUNDLE_STARTER_PRICE_ID"),
    },
    "bundle_pro": {
        "display_name": "Provero Bundle Pro",
        "description": "100,000 API calls / month at $129 / month",
        "calls_per_month": 100_000,
        "amount_cents": 12_900,
        "stripe_price_id": os.getenv("STRIPE_BUNDLE_PRO_PRICE_ID"),
    },
}

# URLs Stripe redirects customers to after checkout
SUCCESS_URL: str = os.getenv(
    "STRIPE_SUCCESS_URL", "https://provero.io/billing/success"
)
CANCEL_URL: str = os.getenv(
    "STRIPE_CANCEL_URL", "https://provero.io/billing/cancel"
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_checkout_session(email: str, tier: str) -> str:
    """
    Create a Stripe Checkout session for *email* subscribing to *tier*.

    Returns the session URL that the caller redirects the customer to.

    Raises
    ------
    ValueError
        If *tier* is unrecognised or is "free" (no checkout needed for $0).
    RuntimeError
        If the Stripe price ID for the requested tier is not configured
        (run `python billing/payments.py --setup` and add the IDs to .env).
    stripe.StripeError
        Propagated directly for the caller to handle.
    """
    tier_key = tier.lower()
    plan = PLANS.get(tier_key)
    if plan is None:
        raise ValueError(
            f"Unknown tier {tier!r}. Valid options: {', '.join(PLANS)}."
        )

    if plan["amount_cents"] == 0:
        raise ValueError(
            "The free tier does not require a checkout session. "
            "Create an API key directly via POST /internal/api-keys."
        )

    price_id = plan["stripe_price_id"]
    if not price_id:
        raise RuntimeError(
            f"Stripe price ID for tier '{tier_key}' is not set. "
            "Run `python billing/payments.py --setup` and add "
            f"STRIPE_{tier_key.upper()}_PRICE_ID to .env."
        )

    session = stripe.checkout.Session.create(
        mode="subscription",
        customer_email=email,
        line_items=[{"price": price_id, "quantity": 1}],
        # {CHECKOUT_SESSION_ID} is a Stripe template literal — it is filled
        # in by Stripe before redirecting the customer, not by Python.
        success_url=SUCCESS_URL + "?session_id={CHECKOUT_SESSION_ID}",
        cancel_url=CANCEL_URL,
        # Metadata is echoed back in the webhook event so we know which
        # customer/tier to issue an API key for.
        metadata={"email": email, "tier": tier_key},
        subscription_data={
            "metadata": {"email": email, "tier": tier_key},
        },
    )
    return session.url


# ---------------------------------------------------------------------------
# One-time setup utility
# ---------------------------------------------------------------------------

def setup_stripe_products() -> dict[str, str]:
    """
    Create Stripe products and recurring monthly prices for all paid tiers.

    Safe to call multiple times — checks for existing products/prices by
    iterating stripe.Product.list() and filtering on metadata client-side,
    then does the same for prices.  Only creates what is missing.

    Uses stripe.Product.list() rather than stripe.Product.search() because
    .list() works reliably across all stripe-python versions >= 5 without
    requiring the Stripe Search API to be enabled on the account.

    Returns a dict mapping tier name → Stripe price ID, e.g.:
        {"starter": "price_xxx", "pro": "price_yyy"}

    Print-friendly output is emitted to stdout so the operator can copy
    the IDs into .env.
    """
    if not stripe.api_key:
        raise RuntimeError(
            "STRIPE_SECRET_KEY is not set. Add it to .env before running setup."
        )

    price_ids: dict[str, str] = {}

    for tier_key, plan in PLANS.items():
        if plan["amount_cents"] == 0:
            # No Stripe product needed for the free tier
            continue

        # ---- Find or create the product --------------------------------
        # Iterate all active products and match on metadata["provero_tier"].
        # Using .list() + client-side filter instead of .search() to avoid
        # any dependency on the Stripe Search API or version-specific query
        # syntax.
        matching_products = [
            p
            for p in stripe.Product.list(active=True, limit=100).auto_paging_iter()
            if p.metadata.get("provero_tier") == tier_key
        ]

        if matching_products:
            product = matching_products[0]
            print(f"[setup] Found existing product for '{tier_key}': {product.id}")
        else:
            product = stripe.Product.create(
                name=plan["display_name"],
                description=plan["description"],
                metadata={"provero_tier": tier_key},
            )
            print(f"[setup] Created product for '{tier_key}': {product.id}")

        # ---- Find or create the recurring monthly price ----------------
        # List prices for this product, then filter client-side for the
        # correct amount and interval — avoids passing a 'recurring' filter
        # dict to Price.list() which has inconsistent behaviour across
        # library versions.
        all_prices = stripe.Price.list(
            product=product.id,
            active=True,
            limit=100,
        )
        matching_prices = [
            p
            for p in all_prices.auto_paging_iter()
            if (
                p.unit_amount == plan["amount_cents"]
                and p.recurring is not None
                and p.recurring.interval == "month"
            )
        ]

        if matching_prices:
            price = matching_prices[0]
            print(f"[setup]   Found existing price for '{tier_key}': {price.id}")
        else:
            price = stripe.Price.create(
                product=product.id,
                unit_amount=plan["amount_cents"],
                currency="usd",
                recurring={"interval": "month"},
                metadata={"provero_tier": tier_key},
            )
            print(f"[setup]   Created price for '{tier_key}': {price.id}")

        price_ids[tier_key] = price.id

    print("\n--- Add these to your .env ---")
    for tier_key, price_id in price_ids.items():
        print(f"STRIPE_{tier_key.upper()}_PRICE_ID={price_id}")

    return price_ids


# ---------------------------------------------------------------------------
# CLI entry point: python billing/payments.py --setup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Provero Stripe setup utility"
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Create Stripe products and prices for all paid tiers.",
    )
    args = parser.parse_args()

    if args.setup:
        setup_stripe_products()
    else:
        parser.print_help()
        sys.exit(1)
