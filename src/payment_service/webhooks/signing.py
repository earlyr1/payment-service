"""Webhook signatures.

Header format: `X-Webhook-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>.<raw body>")>`.
Receivers must recompute the HMAC over the raw request body and reject stale timestamps
(replay protection), the same scheme as Stripe's.
"""

import hashlib
import hmac

SIGNATURE_HEADER = "X-Webhook-Signature"
DEFAULT_TOLERANCE_SECONDS = 300


def compute_signature(secret: str, timestamp: int, body: bytes) -> str:
    return hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()


def build_signature_header(secret: str, timestamp: int, body: bytes) -> str:
    return f"t={timestamp},v1={compute_signature(secret, timestamp, body)}"


def verify_signature_header(
    secret: str,
    header: str,
    body: bytes,
    *,
    now: int,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
) -> bool:
    try:
        parts = dict(item.split("=", 1) for item in header.split(","))
        timestamp = int(parts["t"])
        signature = parts["v1"]
    except KeyError, ValueError:
        return False
    if abs(now - timestamp) > tolerance_seconds:
        return False
    return hmac.compare_digest(signature, compute_signature(secret, timestamp, body))
