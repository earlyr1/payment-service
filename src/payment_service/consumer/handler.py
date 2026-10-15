import logging
from datetime import UTC, datetime
from typing import Any

from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from payment_service.domain.events import PaymentCreatedEvent
from payment_service.logging import bind_log_context
from payment_service.messaging.topology import (
    ATTEMPT_HEADER,
    DEAD_LETTER_EXCHANGE,
    ERROR_HEADER,
    ERROR_TYPE_HEADER,
    PAYMENTS_NEW_DLQ,
    POSTPONED_HEADER,
    RETRY_EXCHANGE,
    RetryPolicy,
    retry_routing_key,
)
from payment_service.services.processing import PaymentBusyError, PaymentProcessor, is_retriable

logger = logging.getLogger(__name__)

_MAX_ERROR_HEADER_LENGTH = 1000
# A delivery postponed because the same payment is being processed does not consume an attempt;
# this caps how long it may keep waiting (with the default 2s delay: about a minute).
MAX_POSTPONEMENTS = 30


def _int_header(headers: dict[str, Any], name: str, default: int) -> int:
    try:
        return max(default, int(headers.get(name, default)))
    except TypeError, ValueError:
        return default


def _attempt_of(headers: dict[str, Any]) -> int:
    return _int_header(headers, ATTEMPT_HEADER, 1)


class PaymentMessageHandler:
    """Consumes `payments.new` and routes failures.

    The message is acknowledged only after its fate is settled: processed, re-published to a
    delay queue for the next attempt, or re-published to the DLQ. Re-publishing uses publisher
    confirms, so a crash at any point leads to redelivery (handled idempotently), never to loss.
    """

    def __init__(
        self,
        broker: RabbitBroker,
        processor: PaymentProcessor,
        retry_policy: RetryPolicy,
        publish_timeout_seconds: float,
    ) -> None:
        self._broker = broker
        self._processor = processor
        self._retry_policy = retry_policy
        self._publish_timeout = publish_timeout_seconds

    async def __call__(self, message: RabbitMessage) -> None:
        attempt = _attempt_of(message.headers)
        with bind_log_context(message_id=message.message_id, attempt=attempt):
            try:
                await self._handle(message, attempt)
            except Exception:
                # Could not settle the message (e.g. the broker refused the re-publish):
                # return it to the queue. The quorum queue delivery limit stops endless loops.
                logger.exception("failed to settle message, requeueing")
                await message.nack(requeue=True)
            else:
                await message.ack()

    async def _handle(self, message: RabbitMessage, attempt: int) -> None:
        try:
            event = PaymentCreatedEvent.model_validate_json(message.body)
        except ValidationError as exc:
            logger.error("malformed message", extra={"error": str(exc)})
            await self._dead_letter(message, attempt, exc)
            return

        with bind_log_context(payment_id=str(event.payment_id)):
            try:
                await self._processor.process(event.payment_id)
            except PaymentBusyError as exc:
                await self._postpone(message, attempt, exc)
            except Exception as exc:
                if is_retriable(exc) and attempt < self._retry_policy.max_attempts:
                    await self._schedule_retry(message, attempt, exc)
                else:
                    await self._dead_letter(message, attempt, exc)

    async def _schedule_retry(self, message: RabbitMessage, attempt: int, exc: Exception) -> None:
        delay_ms = self._retry_policy.delay_ms(attempt)
        logger.warning(
            "processing failed, retry scheduled",
            extra={"error": repr(exc), "retry_in_ms": delay_ms, "next_attempt": attempt + 1},
        )
        await self._republish(
            message,
            exchange=RETRY_EXCHANGE.name,
            routing_key=retry_routing_key(delay_ms),
            headers={ATTEMPT_HEADER: attempt + 1, **self._error_headers(exc)},
        )

    async def _postpone(self, message: RabbitMessage, attempt: int, exc: PaymentBusyError) -> None:
        postponed = _int_header(message.headers, POSTPONED_HEADER, 0) + 1
        if postponed > MAX_POSTPONEMENTS:
            await self._dead_letter(message, attempt, exc)
            return
        delay_ms = self._retry_policy.delay_ms(1)
        logger.info("payment is busy, delivery postponed", extra={"retry_in_ms": delay_ms, "postponed": postponed})
        await self._republish(
            message,
            exchange=RETRY_EXCHANGE.name,
            routing_key=retry_routing_key(delay_ms),
            headers={ATTEMPT_HEADER: attempt, POSTPONED_HEADER: postponed},
        )

    async def _dead_letter(self, message: RabbitMessage, attempt: int, exc: Exception) -> None:
        logger.error(
            "message moved to DLQ",
            exc_info=exc,
            extra={"retriable": is_retriable(exc), "attempts_made": attempt},
        )
        await self._republish(
            message,
            exchange=DEAD_LETTER_EXCHANGE.name,
            routing_key=PAYMENTS_NEW_DLQ,
            headers={
                ATTEMPT_HEADER: attempt,
                "x-dead-lettered-at": datetime.now(UTC).isoformat(),
                **self._error_headers(exc),
            },
        )

    async def _republish(
        self, message: RabbitMessage, *, exchange: str, routing_key: str, headers: dict[str, Any]
    ) -> None:
        await self._broker.publish(
            message.body,
            exchange=exchange,
            routing_key=routing_key,
            headers={**message.headers, **headers},
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type,
            persist=True,
            mandatory=True,
            timeout=self._publish_timeout,
        )

    @staticmethod
    def _error_headers(exc: Exception) -> dict[str, str]:
        return {
            ERROR_TYPE_HEADER: type(exc).__name__,
            ERROR_HEADER: str(exc)[:_MAX_ERROR_HEADER_LENGTH],
        }
