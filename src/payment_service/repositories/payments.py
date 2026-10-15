from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import Payment
from payment_service.domain.enums import PaymentStatus

_MAX_ERROR_LENGTH = 2000


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_if_absent(self, values: dict[str, Any]) -> Payment | None:
        """Insert a payment unless one with the same idempotency key exists.

        Returns the new payment, or None on an idempotency key conflict. Safe under
        concurrency: PostgreSQL serializes conflicting inserts on the unique index.
        """
        stmt = (
            insert(Payment)
            .values(**values)
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return (await self._session.scalars(select(Payment).where(Payment.idempotency_key == key))).one_or_none()

    async def complete(
        self,
        payment_id: UUID,
        *,
        status: PaymentStatus,
        failure_reason: str | None,
        processed_at: datetime,
    ) -> bool:
        """Move a pending payment to a final status. Returns False if it was not pending anymore."""
        if not status.is_final:
            raise ValueError(f"{status} is not a final status")
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, failure_reason=failure_reason, processed_at=processed_at)
            .returning(Payment.id)
        )
        return (await self._session.execute(stmt)).first() is not None

    async def record_webhook_failure(self, payment_id: UUID, error: str) -> None:
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(
                webhook_attempts=Payment.webhook_attempts + 1,
                webhook_last_error=error[:_MAX_ERROR_LENGTH],
            )
        )

    async def record_webhook_delivered(self, payment_id: UUID) -> None:
        await self._session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(
                webhook_attempts=Payment.webhook_attempts + 1,
                webhook_delivered_at=func.now(),
                webhook_last_error=None,
            )
        )
