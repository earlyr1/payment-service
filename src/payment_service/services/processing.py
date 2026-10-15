import logging
import uuid
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.db.locks import advisory_lock_key, try_advisory_lock
from payment_service.db.models import Payment
from payment_service.db.session import create_sessionmaker
from payment_service.domain.enums import PaymentStatus
from payment_service.gateway.emulator import PaymentGateway
from payment_service.repositories.payments import PaymentRepository
from payment_service.webhooks.sender import WebhookDeliveryError, WebhookSender

logger = logging.getLogger(__name__)

_WEBHOOK_ID_NAMESPACE = uuid.UUID("6f1d3f0e-3c55-4a8e-9a43-1f0c2b7d9e21")


class NonRetriableError(Exception):
    """Processing can never succeed for this message; retrying is pointless."""


class PaymentNotFoundError(NonRetriableError):
    pass


class PaymentBusyError(Exception):
    """Another delivery of the same payment is being processed right now; try again later.

    Not a failure: the consumer postpones such a delivery without consuming an attempt."""


def is_retriable(exc: BaseException) -> bool:
    if isinstance(exc, NonRetriableError):
        return False
    if isinstance(exc, WebhookDeliveryError):
        return exc.retriable
    # Unknown errors (database hiccups, gateway timeouts, bugs) are retried; the attempt
    # limit and the DLQ bound the damage if they turn out to be permanent.
    return True


def webhook_id(payment_id: UUID, status: str) -> str:
    """Deterministic per (payment, outcome): every retry carries the same id, so receivers can dedupe."""
    return str(uuid.uuid5(_WEBHOOK_ID_NAMESPACE, f"{payment_id}:{status}"))


def build_webhook_payload(payment: Payment) -> dict[str, Any]:
    return {
        "id": webhook_id(payment.id, payment.status),
        "type": f"payment.{payment.status}",
        "created_at": payment.processed_at.isoformat() if payment.processed_at else None,
        "data": {
            "payment_id": str(payment.id),
            "status": payment.status,
            "amount": str(payment.amount),
            "currency": payment.currency,
            "description": payment.description,
            "metadata": payment.metadata_,
            "failure_reason": payment.failure_reason,
            "idempotency_key": payment.idempotency_key,
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        },
    }


class PaymentProcessor:
    """Charges a payment through the gateway, stores the outcome and notifies the client.

    The message may be delivered any number of times (at-least-once delivery), so:
    - a payment is processed by one worker at a time (advisory lock); a duplicate delivery
      arriving meanwhile is postponed through the retry queue rather than dropped, and on
      its next attempt finds the work done and becomes a no-op;
    - the gateway is called only while the payment is still pending, with the payment id as
      its idempotency key (covers a crash between the charge and the commit);
    - the final status is written with a compare-and-set on `status = 'pending'`;
    - the webhook is skipped once it has been delivered.

    Each in-flight message holds one database connection for the lock, plus short-lived
    connections for queries: size the pool accordingly (see the consumer app).
    """

    def __init__(self, engine: AsyncEngine, gateway: PaymentGateway, webhook_sender: WebhookSender) -> None:
        self._engine = engine
        self._sessionmaker = create_sessionmaker(engine)
        self._gateway = gateway
        self._webhooks = webhook_sender

    async def process(self, payment_id: UUID) -> None:
        async with try_advisory_lock(self._engine, advisory_lock_key("payment-processing", str(payment_id))) as locked:
            if not locked:
                raise PaymentBusyError(f"payment {payment_id} is being processed by another delivery")
            await self._process(payment_id)

    async def _process(self, payment_id: UUID) -> None:
        payment = await self._load(payment_id)

        if payment.status == PaymentStatus.PENDING:
            payment = await self._charge(payment)

        if payment.webhook_delivered_at is not None:
            logger.info("webhook already delivered, nothing to do", extra={"payment_id": str(payment_id)})
            return
        await self._notify(payment)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._sessionmaker() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"payment {payment_id} does not exist")
        return payment

    async def _charge(self, payment: Payment) -> Payment:
        # No transaction is held open while waiting for the (slow) external gateway.
        result = await self._gateway.charge(payment_id=payment.id, amount=payment.amount, currency=payment.currency)
        status = PaymentStatus.SUCCEEDED if result.succeeded else PaymentStatus.FAILED

        async with self._sessionmaker() as session, session.begin():
            updated = await PaymentRepository(session).complete(
                payment.id, status=status, failure_reason=result.failure_reason, processed_at=datetime.now(UTC)
            )
        if updated:
            logger.info("payment processed", extra={"payment_id": str(payment.id), "status": status.value})
        else:
            # Should not happen under the lock; kept as a safety net, the stored result wins.
            logger.warning("payment was completed concurrently", extra={"payment_id": str(payment.id)})
        return await self._load(payment.id)

    async def _notify(self, payment: Payment) -> None:
        payload = build_webhook_payload(payment)
        try:
            await self._webhooks.send(
                payment.webhook_url, webhook_id=payload["id"], event_type=payload["type"], payload=payload
            )
        except WebhookDeliveryError as exc:
            async with self._sessionmaker() as session, session.begin():
                await PaymentRepository(session).record_webhook_failure(payment.id, str(exc))
            raise

        async with self._sessionmaker() as session, session.begin():
            await PaymentRepository(session).record_webhook_delivered(payment.id)
        logger.info("webhook delivered", extra={"payment_id": str(payment.id)})
