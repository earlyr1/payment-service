import asyncio
import contextlib
import json
import socket
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

import uvicorn
from fastapi import FastAPI, Request, Response

from payment_service.gateway.emulator import ChargeResult


async def wait_for[T](probe: Callable[[], Awaitable[T | None]], *, seconds: float = 15.0) -> T:
    """Poll `probe` until it returns a truthy value, and return that value."""
    result: T | None = None
    try:
        async with asyncio.timeout(seconds):
            while not (result := await probe()):  # noqa: ASYNC110 - polling external state on purpose
                await asyncio.sleep(0.05)
    except TimeoutError:
        raise AssertionError(f"condition not met within {seconds}s (last value: {result!r})") from None
    assert result is not None
    return result


@dataclass
class FakeGateway:
    succeed: bool = True
    delay_seconds: float = 0.0
    calls: list[UUID] = field(default_factory=list)

    async def charge(self, *, payment_id: UUID, amount: Decimal, currency: str) -> ChargeResult:
        self.calls.append(payment_id)
        await asyncio.sleep(self.delay_seconds)
        if self.succeed:
            return ChargeResult(succeeded=True)
        return ChargeResult(succeeded=False, failure_reason="insufficient_funds")


@dataclass
class ReceivedWebhook:
    path: str
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class WebhookServer:
    """A real HTTP server; answers each path with scripted status codes (200 once the script runs out)."""

    base_url: str
    received: list[ReceivedWebhook]
    scripts: dict[str, list[int]]

    def url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def calls(self, path: str) -> list[ReceivedWebhook]:
        return [r for r in self.received if r.path == path]


@contextlib.asynccontextmanager
async def run_webhook_server() -> AsyncIterator[WebhookServer]:
    received: list[ReceivedWebhook] = []
    scripts: dict[str, list[int]] = {}
    app = FastAPI()

    @app.post("/{path:path}")
    async def receive(path: str, request: Request) -> Response:
        full_path = f"/{path}"
        received.append(ReceivedWebhook(full_path, dict(request.headers), await request.body()))
        script = scripts.get(full_path)
        return Response(status_code=script.pop(0) if script else 200)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_config=None, lifespan="off"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    await wait_for(lambda: asyncio.sleep(0, result=server.started))
    try:
        yield WebhookServer(f"http://127.0.0.1:{port}", received, scripts)
    finally:
        server.should_exit = True
        await task
