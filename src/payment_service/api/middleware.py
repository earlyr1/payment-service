import json
import logging
import re
import uuid

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from payment_service.logging import bind_log_context

logger = logging.getLogger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"
_VALID_REQUEST_ID = re.compile(r"^[\w\-.:]{1,128}$")


class RequestIdMiddleware:
    """Propagates X-Request-ID (or generates one), binds it to logs and echoes it in the response.

    Also turns unhandled exceptions into a JSON 500 here rather than in Starlette's outermost
    ServerErrorMiddleware, so that the error log and the response still carry the request id.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming = dict(scope["headers"]).get(REQUEST_ID_HEADER.lower().encode(), b"").decode("latin-1")
        request_id = incoming if _VALID_REQUEST_ID.fullmatch(incoming) else uuid.uuid4().hex
        response_started = False

        async def send_with_request_id(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                message.setdefault("headers", []).append((REQUEST_ID_HEADER.lower().encode(), request_id.encode()))
            await send(message)

        with bind_log_context(request_id=request_id):
            try:
                await self.app(scope, receive, send_with_request_id)
            except Exception:
                logger.exception("unhandled error", extra={"method": scope["method"], "path": scope["path"]})
                if response_started:  # too late to change the response; let the server drop the connection
                    raise
                body = json.dumps({"detail": "Internal error"}).encode()
                await send_with_request_id(
                    {
                        "type": "http.response.start",
                        "status": 500,
                        "headers": [
                            (b"content-type", b"application/json"),
                            (b"content-length", str(len(body)).encode()),
                        ],
                    }
                )
                await send({"type": "http.response.body", "body": body})
