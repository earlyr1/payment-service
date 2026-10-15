import asyncio
import contextlib
import json
import logging
import time
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.db.models import OutboxEvent
from payment_service.messaging.topology import PAYMENTS_EXCHANGE
from payment_service.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

EVENT_TYPE_HEADER = "x-event-type"
_MAX_ERROR_BACKOFF_SECONDS = 30.0
_CLEANUP_BATCH_SIZE = 1000


class OutboxRelay:
    """Publishes committed outbox events to RabbitMQ.

    Delivery is at-least-once: an event is marked as published only after the broker
    confirmed it. A crash between the confirm and the commit publishes the event again,
    which consumers tolerate (they are idempotent; the message id is the event id).
    Several relay instances can run concurrently thanks to `FOR UPDATE SKIP LOCKED`.
    """

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        broker: RabbitBroker,
        *,
        batch_size: int,
        poll_interval_seconds: float,
        publish_timeout_seconds: float,
        retention: timedelta,
        cleanup_interval_seconds: float,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._broker = broker
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds
        self._publish_timeout = publish_timeout_seconds
        self._retention = retention
        self._cleanup_interval = cleanup_interval_seconds

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("outbox relay started")
        error_backoff = self._poll_interval
        next_cleanup_at = time.monotonic()

        while not stop.is_set():
            try:
                published, failed = await self.publish_batch()
                if time.monotonic() >= next_cleanup_at:
                    await self.cleanup()
                    next_cleanup_at = time.monotonic() + self._cleanup_interval
            except Exception:
                logger.exception("outbox relay iteration failed")
                published, failed = 0, 1

            if failed:
                # The broker or the database is in trouble: back off instead of hammering it.
                await _wait(stop, error_backoff)
                error_backoff = min(error_backoff * 2, _MAX_ERROR_BACKOFF_SECONDS)
                continue
            error_backoff = self._poll_interval
            if published < self._batch_size:  # caught up; a full batch means more is waiting
                await _wait(stop, self._poll_interval)
        logger.info("outbox relay stopped")

    async def publish_batch(self) -> tuple[int, int]:
        """Publish one batch of pending events. Returns (published, failed)."""
        async with self._sessionmaker() as session, session.begin():
            repo = OutboxRepository(session)
            events = await repo.lock_unpublished_batch(self._batch_size)
            if not events:
                return 0, 0

            # Concurrently, each awaiting its own publisher confirm.
            results = await asyncio.gather(*(self._publish(event) for event in events), return_exceptions=True)

            published = []
            for event, result in zip(events, results, strict=True):
                if isinstance(result, BaseException):
                    logger.warning(
                        "failed to publish outbox event",
                        extra={"event_id": str(event.id), "error": repr(result)},
                    )
                    await repo.record_failure(event.id, repr(result))
                else:
                    published.append(event.id)
            await repo.mark_published(published)

        failed = len(events) - len(published)
        logger.info("outbox batch published", extra={"published": len(published), "failed": failed})
        return len(published), failed

    async def _publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            json.dumps(event.payload, separators=(",", ":")).encode(),
            exchange=PAYMENTS_EXCHANGE.name,
            routing_key=event.routing_key,
            message_id=str(event.id),
            content_type="application/json",
            headers={EVENT_TYPE_HEADER: event.event_type},
            timestamp=event.created_at,
            persist=True,
            mandatory=True,
            timeout=self._publish_timeout,
        )

    async def cleanup(self) -> int:
        """Delete events published longer ago than the retention period."""
        cutoff = datetime.now(UTC) - self._retention
        total = 0
        while True:
            async with self._sessionmaker() as session, session.begin():
                deleted = await OutboxRepository(session).delete_published_before(cutoff, _CLEANUP_BATCH_SIZE)
            total += deleted
            if deleted < _CLEANUP_BATCH_SIZE:
                break
        if total:
            logger.info("outbox cleaned up", extra={"deleted": total})
        return total


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
