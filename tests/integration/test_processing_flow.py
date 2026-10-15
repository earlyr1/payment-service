"""End-to-end: outbox relay -> RabbitMQ -> consumer -> PostgreSQL + webhook, against real containers."""

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker, RabbitQueue
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.config import Settings
from payment_service.consumer.app import create_consumer_app
from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain.enums import Currency
from payment_service.messaging.broker import create_broker, declare_service_topology, retry_policy_from
from payment_service.messaging.topology import (
    ATTEMPT_HEADER,
    ERROR_HEADER,
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW,
    dead_letter_queue,
    payments_new_queue,
    retry_queue,
)
from payment_service.outbox.relay import OutboxRelay
from payment_service.services.payments import CreatePaymentCommand, PaymentService
from payment_service.webhooks.sender import WEBHOOK_ID_HEADER
from payment_service.webhooks.signing import SIGNATURE_HEADER, verify_signature_header
from tests.conftest import TEST_SIGNING_SECRET
from tests.integration.helpers import FakeGateway, WebhookServer, run_webhook_server, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_db")]


def _all_queues(settings: Settings) -> list[RabbitQueue]:
    return [
        payments_new_queue(settings.consumer_delivery_limit),
        dead_letter_queue(),
        *(retry_queue(d) for d in retry_policy_from(settings).retry_delays_ms()),
    ]


@pytest.fixture
async def broker(settings: Settings) -> AsyncIterator[RabbitBroker]:
    broker = create_broker(settings, app_id="tests")
    await broker.connect()
    await declare_service_topology(broker, settings)
    for queue in _all_queues(settings):
        await (await broker.declare_queue(queue)).purge()
    yield broker
    await broker.stop()


@pytest.fixture
async def webhooks() -> AsyncIterator[WebhookServer]:
    async with run_webhook_server() as server:
        yield server


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture
async def consumer(settings: Settings, broker: RabbitBroker, gateway: FakeGateway) -> AsyncIterator[None]:
    app = create_consumer_app(settings, gateway=gateway)
    await app.start()
    yield
    await app.stop()


@pytest.fixture
async def relay(
    settings: Settings, broker: RabbitBroker, sessionmaker: async_sessionmaker[AsyncSession]
) -> AsyncIterator[OutboxRelay]:
    relay = OutboxRelay(
        sessionmaker,
        broker,
        batch_size=settings.outbox_batch_size,
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
        publish_timeout_seconds=settings.rabbitmq_timeout_seconds,
        retention=timedelta(hours=1),
        cleanup_interval_seconds=3600,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    yield relay
    stop.set()
    await task


async def _create_payment(sessionmaker: async_sessionmaker[AsyncSession], webhook_url: str) -> uuid.UUID:
    async with sessionmaker() as session:
        result = await PaymentService(session).create_payment(
            CreatePaymentCommand(
                amount=Decimal("10.00"), currency=Currency.USD, webhook_url=webhook_url, metadata={"k": "v"}
            ),
            idempotency_key=str(uuid.uuid4()),
        )
    return result.payment.id


async def _payment(sessionmaker: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> Payment:
    async with sessionmaker() as session:
        payment = await session.get(Payment, payment_id)
    assert payment is not None
    return payment


async def _get_one(broker: RabbitBroker, queue: RabbitQueue) -> aio_pika.abc.AbstractIncomingMessage | None:
    declared = await broker.declare_queue(queue)
    return await declared.get(no_ack=True, fail=False)


@pytest.mark.usefixtures("consumer", "relay")
async def test_successful_payment(
    sessionmaker: async_sessionmaker[AsyncSession], webhooks: WebhookServer, gateway: FakeGateway
) -> None:
    payment_id = await _create_payment(sessionmaker, webhooks.url("/hook"))

    await wait_for(lambda: _webhook_delivered(sessionmaker, payment_id))

    payment = await _payment(sessionmaker, payment_id)
    assert payment.status == "succeeded"
    assert payment.processed_at is not None
    assert payment.webhook_attempts == 1
    assert gateway.calls == [payment_id]

    [webhook] = webhooks.calls("/hook")
    body = webhook.json()
    assert body["type"] == "payment.succeeded"
    assert body["data"]["payment_id"] == str(payment_id)
    assert body["data"]["amount"] == "10.00"
    assert body["data"]["metadata"] == {"k": "v"}
    assert webhook.headers[WEBHOOK_ID_HEADER.lower()] == body["id"]
    assert verify_signature_header(
        TEST_SIGNING_SECRET, webhook.headers[SIGNATURE_HEADER.lower()], webhook.body, now=int(time.time())
    )

    async with sessionmaker() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is not None


@pytest.mark.usefixtures("consumer", "relay")
async def test_declined_payment(
    sessionmaker: async_sessionmaker[AsyncSession], webhooks: WebhookServer, gateway: FakeGateway
) -> None:
    gateway.succeed = False
    payment_id = await _create_payment(sessionmaker, webhooks.url("/hook"))

    await wait_for(lambda: _webhook_delivered(sessionmaker, payment_id))

    payment = await _payment(sessionmaker, payment_id)
    assert payment.status == "failed"
    assert payment.failure_reason == "insufficient_funds"
    assert webhooks.calls("/hook")[0].json()["type"] == "payment.failed"


@pytest.mark.usefixtures("consumer", "relay")
async def test_webhook_retried_until_delivered(
    sessionmaker: async_sessionmaker[AsyncSession], webhooks: WebhookServer, gateway: FakeGateway
) -> None:
    webhooks.scripts["/flaky"] = [500, 503]  # two failures, then 200
    payment_id = await _create_payment(sessionmaker, webhooks.url("/flaky"))

    await wait_for(lambda: _webhook_delivered(sessionmaker, payment_id))

    payment = await _payment(sessionmaker, payment_id)
    assert payment.webhook_attempts == 3
    assert payment.webhook_last_error is None
    calls = webhooks.calls("/flaky")
    assert len(calls) == 3
    assert len({c.headers[WEBHOOK_ID_HEADER.lower()] for c in calls}) == 1  # same id on every retry
    assert gateway.calls == [payment_id]  # retries never charge twice


@pytest.mark.usefixtures("consumer", "relay")
async def test_message_dead_lettered_after_max_attempts(
    settings: Settings,
    broker: RabbitBroker,
    sessionmaker: async_sessionmaker[AsyncSession],
    webhooks: WebhookServer,
    gateway: FakeGateway,
) -> None:
    webhooks.scripts["/down"] = [500] * 100
    payment_id = await _create_payment(sessionmaker, webhooks.url("/down"))

    dead = await wait_for(lambda: _get_one(broker, dead_letter_queue()))

    assert dead.headers[ATTEMPT_HEADER] == settings.consumer_max_attempts == 3
    assert "HTTP 500" in str(dead.headers[ERROR_HEADER])
    assert json.loads(dead.body)["payment_id"] == str(payment_id)
    assert len(webhooks.calls("/down")) == 3
    assert gateway.calls == [payment_id]

    payment = await _payment(sessionmaker, payment_id)
    assert payment.status == "succeeded"  # the charge itself is not undone by a webhook failure
    assert payment.webhook_delivered_at is None
    assert payment.webhook_attempts == 3
    assert "HTTP 500" in (payment.webhook_last_error or "")


@pytest.mark.usefixtures("consumer")
async def test_duplicate_messages_are_processed_once(
    broker: RabbitBroker,
    sessionmaker: async_sessionmaker[AsyncSession],
    webhooks: WebhookServer,
    gateway: FakeGateway,
) -> None:
    payment_id = await _create_payment(sessionmaker, webhooks.url("/hook"))
    async with sessionmaker() as session:
        event = (await session.scalars(select(OutboxEvent))).one()

    # Simulates the outbox relay crashing after the publish and before marking the event.
    for _ in range(3):
        await broker.publish(
            json.dumps(event.payload).encode(),
            exchange=PAYMENTS_EXCHANGE.name,
            routing_key=PAYMENTS_NEW,
            message_id=str(event.id),
            persist=True,
        )

    await wait_for(lambda: _webhook_delivered(sessionmaker, payment_id))
    await asyncio.sleep(1.5)  # let the postponed duplicates come back and drain

    assert gateway.calls == [payment_id]
    assert len(webhooks.calls("/hook")) == 1
    assert await _get_one(broker, dead_letter_queue()) is None


@pytest.mark.usefixtures("consumer")
async def test_duplicate_of_a_slow_payment_does_not_reach_dlq(
    broker: RabbitBroker,
    sessionmaker: async_sessionmaker[AsyncSession],
    webhooks: WebhookServer,
    gateway: FakeGateway,
) -> None:
    # Processing takes far longer than all retry delays together (0.2s + 0.4s): a postponed duplicate
    # must not burn its attempts and end up in the DLQ while the original is still running.
    gateway.delay_seconds = 2
    payment_id = await _create_payment(sessionmaker, webhooks.url("/hook"))
    async with sessionmaker() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    for _ in range(2):
        await broker.publish(
            json.dumps(event.payload).encode(),
            exchange=PAYMENTS_EXCHANGE.name,
            routing_key=PAYMENTS_NEW,
            message_id=str(event.id),
            persist=True,
        )

    await wait_for(lambda: _webhook_delivered(sessionmaker, payment_id))
    await asyncio.sleep(1)

    assert gateway.calls == [payment_id]
    assert len(webhooks.calls("/hook")) == 1
    assert await _get_one(broker, dead_letter_queue()) is None


@pytest.mark.usefixtures("consumer")
@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b'{"event_id": "x"}',
        json.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "occurred_at": datetime.now(UTC).isoformat(),
                "payment_id": str(uuid.uuid4()),
            }
        ).encode(),  # well-formed, but the payment does not exist
    ],
    ids=["malformed", "invalid-schema", "unknown-payment"],
)
async def test_unprocessable_messages_go_straight_to_dlq(broker: RabbitBroker, body: bytes) -> None:
    await broker.publish(body, exchange=PAYMENTS_EXCHANGE.name, routing_key=PAYMENTS_NEW, persist=True)

    dead = await wait_for(lambda: _get_one(broker, dead_letter_queue()))

    assert dead.body == body
    assert dead.headers[ATTEMPT_HEADER] == 1  # not retried


async def test_relay_publishes_pending_events(
    broker: RabbitBroker, relay: OutboxRelay, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    payment_ids = {await _create_payment(sessionmaker, "https://example.com/hook") for _ in range(5)}

    messages = []
    for _ in range(5):
        message = await wait_for(lambda: _get_one(broker, payments_new_queue(10)))
        messages.append(message)

    assert {uuid.UUID(json.loads(m.body)["payment_id"]) for m in messages} == payment_ids
    async with sessionmaker() as session:
        events = (await session.scalars(select(OutboxEvent))).all()
    assert all(e.published_at is not None for e in events)
    assert {m.message_id for m in messages} == {str(e.id) for e in events}  # message id == event id


class _BrokenBroker:
    async def publish(self, *args: Any, **kwargs: Any) -> None:
        raise ConnectionError("broker is down")


async def test_relay_keeps_events_when_publishing_fails(
    settings: Settings, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    await _create_payment(sessionmaker, "https://example.com/hook")
    relay = OutboxRelay(
        sessionmaker,
        _BrokenBroker(),  # type: ignore[arg-type]
        batch_size=10,
        poll_interval_seconds=0.05,
        publish_timeout_seconds=1,
        retention=timedelta(hours=1),
        cleanup_interval_seconds=3600,
    )

    assert await relay.publish_batch() == (0, 1)

    async with sessionmaker() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is None
    assert event.publish_attempts == 1
    assert "broker is down" in (event.last_error or "")


async def test_relay_cleanup_removes_only_old_published_events(
    relay: OutboxRelay, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    await _create_payment(sessionmaker, "https://example.com/hook")
    await _create_payment(sessionmaker, "https://example.com/hook")
    await wait_for(lambda: _all_published(sessionmaker))
    async with sessionmaker() as session, session.begin():
        old = (await session.scalars(select(OutboxEvent.id).limit(1))).one()
        await session.execute(
            update(OutboxEvent).where(OutboxEvent.id == old).values(published_at=datetime.now(UTC) - timedelta(days=30))
        )

    assert await relay.cleanup() == 1

    async with sessionmaker() as session:
        remaining = (await session.scalars(select(OutboxEvent.id))).all()
    assert old not in remaining
    assert len(remaining) == 1


async def _webhook_delivered(sessionmaker: async_sessionmaker[AsyncSession], payment_id: uuid.UUID) -> bool:
    return (await _payment(sessionmaker, payment_id)).webhook_delivered_at is not None


async def _all_published(sessionmaker: async_sessionmaker[AsyncSession]) -> bool:
    async with sessionmaker() as session:
        events = (await session.scalars(select(OutboxEvent))).all()
    return bool(events) and all(e.published_at is not None for e in events)
