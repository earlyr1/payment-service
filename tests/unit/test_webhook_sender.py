import asyncio
import time
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from payment_service.services.processing import NonRetriableError, PaymentNotFoundError, is_retriable
from payment_service.webhooks.sender import WEBHOOK_ID_HEADER, WebhookDeliveryError, WebhookSender
from payment_service.webhooks.signing import SIGNATURE_HEADER, verify_signature_header

SECRET = "secret-secret-secret"
URL = "https://hooks.example.com/payments"


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with WebhookSender.create_client(timeout_seconds=1) as client:
        yield client


def _sender(client: httpx.AsyncClient, *, allow_private: bool = True) -> WebhookSender:
    return WebhookSender(client, signing_secret=SECRET, allow_private_targets=allow_private, total_timeout_seconds=1)


@respx.mock
async def test_sends_signed_json(client: httpx.AsyncClient) -> None:
    route = respx.post(URL).respond(204)
    await _sender(client).send(URL, webhook_id="wh-1", event_type="payment.succeeded", payload={"a": 1})

    request = route.calls.last.request
    assert request.headers[WEBHOOK_ID_HEADER] == "wh-1"
    assert request.headers["content-type"] == "application/json"
    assert verify_signature_header(SECRET, request.headers[SIGNATURE_HEADER], request.content, now=int(time.time()))


@respx.mock
@pytest.mark.parametrize("status", [301, 400, 404, 500, 503])
async def test_non_2xx_is_a_retriable_failure(client: httpx.AsyncClient, status: int) -> None:
    respx.post(URL).respond(status)
    with pytest.raises(WebhookDeliveryError) as exc_info:
        await _sender(client).send(URL, webhook_id="wh", event_type="t", payload={})
    assert exc_info.value.retriable


@respx.mock
async def test_timeout_is_a_retriable_failure(client: httpx.AsyncClient) -> None:
    respx.post(URL).mock(side_effect=httpx.ReadTimeout("slow"))
    with pytest.raises(WebhookDeliveryError) as exc_info:
        await _sender(client).send(URL, webhook_id="wh", event_type="t", payload={})
    assert exc_info.value.retriable


async def test_slow_receiver_hits_the_total_deadline(client: httpx.AsyncClient) -> None:
    async def trickle(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)  # stands in for a receiver dribbling bytes under the per-read timeout
        return httpx.Response(200)

    with respx.mock:
        respx.post(URL).mock(side_effect=trickle)
        with pytest.raises(WebhookDeliveryError) as exc_info:
            await _sender(client).send(URL, webhook_id="wh", event_type="t", payload={})
    assert exc_info.value.retriable


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1:8080/hook", "http://10.1.2.3/hook", "http://169.254.169.254/latest/meta-data", "http://[::1]/"],
)
async def test_private_targets_are_refused(client: httpx.AsyncClient, url: str) -> None:
    with pytest.raises(WebhookDeliveryError) as exc_info:
        await _sender(client, allow_private=False).send(url, webhook_id="wh", event_type="t", payload={})
    assert not exc_info.value.retriable


def test_error_classification() -> None:
    assert is_retriable(RuntimeError("db is down"))
    assert is_retriable(WebhookDeliveryError("500", retriable=True))
    assert not is_retriable(WebhookDeliveryError("private target", retriable=False))
    assert not is_retriable(PaymentNotFoundError("missing"))
    assert not is_retriable(NonRetriableError("bad"))
