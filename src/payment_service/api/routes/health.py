import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.api.deps import get_session

logger = logging.getLogger(__name__)

# Probes are unauthenticated on purpose: orchestrators call them without credentials,
# and they expose nothing but up/down.
router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live", summary="Liveness probe")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready", summary="Readiness probe: the database is reachable")
async def ready(response: Response, session: Annotated[AsyncSession, Depends(get_session)]) -> dict[str, str]:
    # The broker is deliberately not checked: thanks to the outbox the API keeps accepting
    # payments while RabbitMQ is unavailable.
    try:
        await session.execute(text("SELECT 1"))
    except Exception:
        logger.exception("readiness check failed")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "unavailable"}
    return {"status": "ok"}
