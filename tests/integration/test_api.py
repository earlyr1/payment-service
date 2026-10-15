import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from asgi_lifespan import LifespanManager
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payment_service.api.app import create_app
from payment_service.config import Settings
from payment_service.db.models import OutboxEvent, Payment
from payment_service.services.payments import PaymentService
from tests.conftest import TEST_API_KEY

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("clean_db")]

PAYMENTS = "/api/v1/payments"
BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42, "tags": ["a", "b"]},
    "webhook_url": "https://merchant.example.com/webhooks/payments",
}


@pytest.fixture
async def client(db_settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(db_settings)
    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", headers={"X-API-Key": TEST_API_KEY}
        ) as client:
            yield client


def _key() -> dict[str, str]:
    return {"Idempotency-Key": str(uuid.uuid4())}


async def _count(sessionmaker: async_sessionmaker[AsyncSession], model: type[Payment | OutboxEvent]) -> int:
    async with sessionmaker() as session:
        return (await session.scalar(select(func.count()).select_from(model))) or 0


async def test_create_payment(client: httpx.AsyncClient, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
    response = await client.post(PAYMENTS, json=BODY, headers=_key())

    assert response.status_code == 202
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert response.headers["Location"] == f"{PAYMENTS}/{data['payment_id']}"
    assert "Idempotent-Replayed" not in response.headers

    async with sessionmaker() as session:
        events = (await session.scalars(select(OutboxEvent))).all()
    assert len(events) == 1
    assert events[0].routing_key == "payments.new"
    assert events[0].published_at is None
    assert events[0].payload["payment_id"] == data["payment_id"]


async def test_get_payment(client: httpx.AsyncClient) -> None:
    key = _key()
    created = (await client.post(PAYMENTS, json=BODY, headers=key)).json()

    response = await client.get(f"{PAYMENTS}/{created['payment_id']}")

    assert response.status_code == 200
    data = response.json()
    assert data["payment_id"] == created["payment_id"]
    assert data["amount"] == "1500.00"  # decimals are serialized as strings
    assert data["currency"] == "RUB"
    assert data["description"] == "Order #42"
    assert data["metadata"] == BODY["metadata"]
    assert data["webhook_url"] == BODY["webhook_url"]
    assert data["idempotency_key"] == key["Idempotency-Key"]
    assert data["status"] == "pending"
    assert data["failure_reason"] is None
    assert data["processed_at"] is None


async def test_get_unknown_payment(client: httpx.AsyncClient) -> None:
    assert (await client.get(f"{PAYMENTS}/{uuid.uuid4()}")).status_code == 404
    assert (await client.get(f"{PAYMENTS}/not-a-uuid")).status_code == 422


async def test_replay_with_same_key_returns_original(
    client: httpx.AsyncClient, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    key = _key()
    first = await client.post(PAYMENTS, json=BODY, headers=key)
    # Same parameters, different formatting of the amount.
    second = await client.post(PAYMENTS, json={**BODY, "amount": 1500}, headers=key)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await _count(sessionmaker, Payment) == 1
    assert await _count(sessionmaker, OutboxEvent) == 1


async def test_key_reuse_with_different_body_is_rejected(
    client: httpx.AsyncClient, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    key = _key()
    await client.post(PAYMENTS, json=BODY, headers=key)
    response = await client.post(PAYMENTS, json={**BODY, "amount": "1.00"}, headers=key)

    assert response.status_code == 422
    assert "Idempotency-Key" in response.json()["detail"]
    assert await _count(sessionmaker, Payment) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: httpx.AsyncClient, sessionmaker: async_sessionmaker[AsyncSession]
) -> None:
    key = _key()
    responses = await asyncio.gather(*(client.post(PAYMENTS, json=BODY, headers=key) for _ in range(10)))

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert sum(r.headers.get("Idempotent-Replayed") == "true" for r in responses) == 9
    assert await _count(sessionmaker, Payment) == 1
    assert await _count(sessionmaker, OutboxEvent) == 1


@pytest.mark.parametrize("headers", [{}, {"Idempotency-Key": ""}, {"Idempotency-Key": "x" * 256}])
async def test_idempotency_key_is_required_and_validated(client: httpx.AsyncClient, headers: dict[str, str]) -> None:
    response = await client.post(PAYMENTS, json=BODY, headers=headers)
    assert response.status_code == 400


async def test_invalid_body_is_rejected(client: httpx.AsyncClient) -> None:
    response = await client.post(PAYMENTS, json={**BODY, "amount": "-5"}, headers=_key())
    assert response.status_code == 422


@pytest.mark.parametrize("api_key", [None, "", "wrong-key"])
@pytest.mark.parametrize(("method", "path"), [("POST", PAYMENTS), ("GET", f"{PAYMENTS}/{uuid.uuid4()}")])
async def test_api_key_is_required(client: httpx.AsyncClient, api_key: str | None, method: str, path: str) -> None:
    headers = {**_key(), "X-API-Key": api_key} if api_key is not None else _key()
    if api_key is None:
        client.headers.pop("X-API-Key")
    response = await client.request(method, path, json=BODY, headers=headers)
    assert response.status_code == 401


async def test_health(client: httpx.AsyncClient) -> None:
    client.headers.pop("X-API-Key")
    assert (await client.get("/health/live")).status_code == 200
    assert (await client.get("/health/ready")).json() == {"status": "ok"}


async def test_request_id_is_propagated(client: httpx.AsyncClient) -> None:
    response = await client.get("/health/live", headers={"X-Request-ID": "req-123"})
    assert response.headers["X-Request-ID"] == "req-123"
    assert (await client.get("/health/live")).headers["X-Request-ID"]


async def test_unhandled_error_returns_500_with_request_id(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def explode(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(PaymentService, "get_payment", explode)
    response = await client.get(f"{PAYMENTS}/{uuid.uuid4()}", headers={"X-Request-ID": "req-500"})

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal error"}
    assert response.headers["X-Request-ID"] == "req-500"
