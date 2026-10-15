from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent

_MAX_ERROR_LENGTH = 2000


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def lock_unpublished_batch(self, limit: int) -> Sequence[OutboxEvent]:
        """Lock the oldest unpublished events for the current transaction.

        SKIP LOCKED lets several relay instances work in parallel without
        publishing the same row twice.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    async def mark_published(self, event_ids: Sequence[UUID]) -> None:
        if not event_ids:
            return
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id.in_(event_ids))
            .values(published_at=func.now(), publish_attempts=OutboxEvent.publish_attempts + 1, last_error=None)
        )

    async def record_failure(self, event_id: UUID, error: str) -> None:
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(publish_attempts=OutboxEvent.publish_attempts + 1, last_error=error[:_MAX_ERROR_LENGTH])
        )

    async def delete_published_before(self, cutoff: datetime, limit: int) -> int:
        """Delete up to `limit` events published before `cutoff`. Returns the number deleted."""
        victims = (
            select(OutboxEvent.id)
            .where(OutboxEvent.published_at < cutoff)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .scalar_subquery()
        )
        result = await self._session.execute(
            delete(OutboxEvent).where(OutboxEvent.id.in_(victims)).returning(OutboxEvent.id)
        )
        return len(result.all())
