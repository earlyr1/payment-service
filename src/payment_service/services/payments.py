import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain.enums import Currency, PaymentStatus
from payment_service.domain.events import PaymentCreatedEvent
from payment_service.messaging.topology import PAYMENTS_NEW
from payment_service.repositories.outbox import OutboxRepository
from payment_service.repositories.payments import PaymentRepository


class IdempotencyKeyReusedError(Exception):
    """The idempotency key was already used for a request with different parameters."""


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    amount: Decimal
    currency: Currency
    webhook_url: str
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> str:
        """Stable hash of the request parameters, used to detect idempotency key reuse."""
        canonical = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False)
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    replayed: bool  # True when an earlier request with the same idempotency key is returned


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create_payment(self, command: CreatePaymentCommand, *, idempotency_key: str) -> CreatePaymentResult:
        fingerprint = command.fingerprint()
        payment_id = uuid.uuid7()

        async with self._session.begin():
            payment = await self._payments.insert_if_absent(
                {
                    "id": payment_id,
                    "idempotency_key": idempotency_key,
                    "request_fingerprint": fingerprint,
                    "amount": command.amount,
                    "currency": command.currency,
                    "description": command.description,
                    "metadata_": command.metadata,
                    "status": PaymentStatus.PENDING,
                    "webhook_url": command.webhook_url,
                }
            )
            if payment is None:
                return await self._replay(idempotency_key, fingerprint)

            # Same transaction as the payment row: either both are committed or neither is.
            event = PaymentCreatedEvent(event_id=uuid.uuid7(), occurred_at=datetime.now(UTC), payment_id=payment.id)
            self._outbox.add(
                OutboxEvent(
                    id=event.event_id,
                    aggregate_type="payment",
                    aggregate_id=payment.id,
                    event_type=event.type,
                    routing_key=PAYMENTS_NEW,
                    payload=event.model_dump(mode="json"),
                )
            )
        return CreatePaymentResult(payment=payment, replayed=False)

    async def _replay(self, idempotency_key: str, fingerprint: str) -> CreatePaymentResult:
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is None:  # pragma: no cover - ON CONFLICT saw a committed row, READ COMMITTED sees it too
            raise RuntimeError("idempotency key conflict reported, but no payment found")
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyKeyReusedError
        return CreatePaymentResult(payment=existing, replayed=True)

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        return await self._payments.get(payment_id)
