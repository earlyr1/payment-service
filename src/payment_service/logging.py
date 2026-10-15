"""Structured logging on top of stdlib `logging`.

Every record is rendered as one JSON object per line (or a readable line in `console` mode).
Values passed via `extra=` and values bound with `bind_log_context` are included as fields.
"""

import contextvars
import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

_log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("log_context", default={})  # noqa: B039 - never mutated

# Attributes every LogRecord has; anything else on a record came from `extra=`.
_RESERVED_ATTRS = frozenset(logging.makeLogRecord({}).__dict__) | {"message", "asctime", "taskName"}


@contextmanager
def bind_log_context(**fields: Any) -> Iterator[None]:
    """Attach fields to every log record emitted inside the block (task-local)."""
    token = _log_context.set({**_log_context.get(), **fields})
    try:
        yield
    finally:
        _log_context.reset(token)


def _record_fields(record: logging.LogRecord) -> dict[str, Any]:
    fields = dict(_log_context.get())
    fields.update({k: v for k, v in record.__dict__.items() if k not in _RESERVED_ATTRS and not k.startswith("_")})
    return fields


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            **_record_fields(record),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class ConsoleFormatter(logging.Formatter):
    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        if fields := _record_fields(record):
            line += " " + " ".join(f"{k}={v}" for k, v in fields.items())
        return line


def configure_logging(level: str, fmt: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if fmt == "json" else ConsoleFormatter())

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)

    # Route third-party loggers through the root handler instead of their own.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "faststream", "faststream.access"):
        lib_logger = logging.getLogger(name)
        lib_logger.handlers.clear()
        lib_logger.propagate = True
    for noisy in ("aiormq", "aio_pika", "httpx", "httpcore"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))
