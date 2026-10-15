import re
import secrets
from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.services.payments import PaymentService

IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
MAX_IDEMPOTENCY_KEY_LENGTH = 255
# Visible ASCII only: keys end up in logs and headers.
_IDEMPOTENCY_KEY_RE = re.compile(r"^[\x21-\x7e]+$")

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False, description="Static API key")


def get_settings(request: Request) -> Settings:
    settings: Settings = request.app.state.settings
    return settings


async def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    api_key: Annotated[str | None, Depends(_api_key_header)],
) -> None:
    expected = settings.api_key.get_secret_value()
    # Constant-time comparison: do not leak the key through response timing.
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected.encode()):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    sessionmaker: async_sessionmaker[AsyncSession] = request.app.state.sessionmaker
    async with sessionmaker() as session:
        yield session


def get_payment_service(session: Annotated[AsyncSession, Depends(get_session)]) -> PaymentService:
    return PaymentService(session)


def idempotency_key(
    key: Annotated[
        str | None,
        Header(
            alias=IDEMPOTENCY_KEY_HEADER,
            description="Unique key of this request (e.g. a UUID). Retrying with the same key and body returns "
            "the original payment instead of creating a new one.",
        ),
    ] = None,
) -> str:
    # Validated by hand to answer 400 (as in the IETF Idempotency-Key draft) rather than FastAPI's generic 422.
    if key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail=f"{IDEMPOTENCY_KEY_HEADER} header is required")
    if len(key) > MAX_IDEMPOTENCY_KEY_LENGTH or not _IDEMPOTENCY_KEY_RE.fullmatch(key):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail=f"{IDEMPOTENCY_KEY_HEADER} must be 1-{MAX_IDEMPOTENCY_KEY_LENGTH} visible ASCII characters",
        )
    return key
