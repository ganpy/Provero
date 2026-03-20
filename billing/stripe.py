"""
Compatibility shim — re-exports everything from billing.payments.

The logic was moved to billing/payments.py to avoid a fatal naming collision:
a file called stripe.py, when run directly (python billing/stripe.py), causes
Python to insert billing/ into sys.path[0].  Any subsequent `import stripe`
then resolves to this file itself instead of the installed stripe library,
producing AttributeError: module 'stripe' has no attribute 'Product' (or
.checkout, .Webhook, etc.).

This shim contains no `import stripe` statement so it is safe to import even
when billing/ is on sys.path.

Use billing.payments directly for new code:
    from billing.payments import PLANS, create_checkout_session, ...
"""

from billing.payments import (  # noqa: F401
    CANCEL_URL,
    PLANS,
    SUCCESS_URL,
    create_checkout_session,
    setup_stripe_products,
)
