"""Demo webhook receiver (local development only, not part of the service).

Endpoints:
    POST /webhooks/ok     -> always 200
    POST /webhooks/flaky  -> 500 for the first 2 deliveries of a webhook id, then 200 (shows retries)
    POST /webhooks/fail   -> always 500 (shows retries ending in the DLQ)
    GET  /webhooks        -> the last received webhooks, newest first, with signature check results
"""

import json
import logging
import os
import time
from collections import Counter, deque
from typing import Any, Literal

import uvicorn
from fastapi import FastAPI, Request, Response, status

from payment_service.logging import configure_logging
from payment_service.webhooks.sender import WEBHOOK_ID_HEADER
from payment_service.webhooks.signing import SIGNATURE_HEADER, verify_signature_header

logger = logging.getLogger("webhook_receiver")

FLAKY_FAILURES = 2


def create_receiver_app(signing_secret: str) -> FastAPI:
    app = FastAPI(title="Demo webhook receiver")
    received: deque[dict[str, Any]] = deque(maxlen=200)
    deliveries: Counter[str] = Counter()

    @app.post("/webhooks/{mode}")
    async def receive(mode: Literal["ok", "flaky", "fail"], request: Request) -> Response:
        body = await request.body()
        webhook_id = request.headers.get(WEBHOOK_ID_HEADER, "")
        signature_valid = verify_signature_header(
            signing_secret, request.headers.get(SIGNATURE_HEADER, ""), body, now=int(time.time())
        )
        deliveries[webhook_id] += 1
        attempt = deliveries[webhook_id]

        answer = status.HTTP_200_OK
        if not signature_valid:
            answer = status.HTTP_401_UNAUTHORIZED
        elif mode == "fail" or (mode == "flaky" and attempt <= FLAKY_FAILURES):
            answer = status.HTTP_500_INTERNAL_SERVER_ERROR

        received.appendleft(
            {
                "mode": mode,
                "delivery": attempt,
                "answered": answer,
                "signature_valid": signature_valid,
                "headers": {k: v for k, v in request.headers.items() if k.lower().startswith("x-webhook")},
                "body": json.loads(body) if body else None,
            }
        )
        logger.info(
            "webhook received",
            extra={"mode": mode, "webhook_id": webhook_id, "delivery": attempt, "answered": answer},
        )
        return Response(status_code=answer)

    @app.get("/webhooks")
    async def list_received() -> list[dict[str, Any]]:
        return list(received)

    return app


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"), os.environ.get("LOG_FORMAT", "console"))
    app = create_receiver_app(os.environ["WEBHOOK_SIGNING_SECRET"])
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "9000")), log_config=None)  # noqa: S104


if __name__ == "__main__":
    main()
