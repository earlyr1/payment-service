import asyncio
import ipaddress
import json
import socket
import time
from typing import Any
from urllib.parse import urlsplit

import httpx

from payment_service.webhooks.signing import SIGNATURE_HEADER, build_signature_header

WEBHOOK_ID_HEADER = "X-Webhook-Id"
WEBHOOK_EVENT_HEADER = "X-Webhook-Event"
USER_AGENT = "payment-service-webhooks/1.0"


class WebhookDeliveryError(Exception):
    def __init__(self, message: str, *, retriable: bool) -> None:
        super().__init__(message)
        self.retriable = retriable


class WebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        signing_secret: str,
        allow_private_targets: bool,
        total_timeout_seconds: float,
    ) -> None:
        self._client = client
        self._total_timeout = total_timeout_seconds
        self._secret = signing_secret
        self._allow_private_targets = allow_private_targets

    @staticmethod
    def create_client(timeout_seconds: float) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=False,  # a redirect could point at an internal address
            headers={"User-Agent": USER_AGENT},
        )

    async def send(self, url: str, *, webhook_id: str, event_type: str, payload: dict[str, Any]) -> None:
        """POST a signed JSON payload. Success means a 2xx answer; anything else raises WebhookDeliveryError."""
        if not self._allow_private_targets:
            await self._ensure_public_target(url)

        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        headers = {
            "Content-Type": "application/json",
            WEBHOOK_ID_HEADER: webhook_id,
            WEBHOOK_EVENT_HEADER: event_type,
            SIGNATURE_HEADER: build_signature_header(self._secret, int(time.time()), body),
        }
        try:
            # httpx timeouts apply per network operation; a receiver trickling bytes would never trip
            # them, so the whole exchange also gets a hard deadline.
            async with asyncio.timeout(self._total_timeout):
                # Streamed so that a huge response body is never read into memory.
                async with self._client.stream("POST", url, content=body, headers=headers) as response:
                    status = response.status_code
        except TimeoutError as exc:
            raise WebhookDeliveryError("timeout: no complete answer in time", retriable=True) from exc
        except httpx.TimeoutException as exc:
            raise WebhookDeliveryError(f"timeout: {exc!r}", retriable=True) from exc
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"transport error: {exc!r}", retriable=True) from exc

        if not 200 <= status < 300:
            # 4xx are retried as well: receivers often answer 404/400 transiently during deploys.
            raise WebhookDeliveryError(f"receiver answered HTTP {status}", retriable=True)

    @staticmethod
    async def _ensure_public_target(url: str) -> None:
        """SSRF guard: refuse targets resolving to loopback/private/link-local/reserved addresses.

        Note: the address is resolved again by the HTTP client, so a DNS-rebinding attacker could
        still win the race; production deployments should additionally route webhooks through an
        egress proxy that enforces the same policy.
        """
        parts = urlsplit(url)
        host = parts.hostname
        if not host:
            raise WebhookDeliveryError("webhook URL has no host", retriable=False)
        port = parts.port or (443 if parts.scheme == "https" else 80)
        try:
            infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            raise WebhookDeliveryError(f"cannot resolve {host}: {exc}", retriable=True) from exc

        for *_, sockaddr in infos:
            address = ipaddress.ip_address(sockaddr[0])
            if not address.is_global:
                raise WebhookDeliveryError(f"webhook target {host} resolves to non-public {address}", retriable=False)
