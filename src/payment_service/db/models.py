from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, Numeric, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payment_service.db.base import Base
from payment_service.domain.enums import Currency, PaymentStatus

AMOUNT_PRECISION = 18
AMOUNT_SCALE = 2


def _sql_in(values: type[Currency] | type[PaymentStatus]) -> str:
    return ", ".join(f"'{v.value}'" for v in values)


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint(f"currency IN ({_sql_in(Currency)})", name="currency_valid"),
        CheckConstraint(f"status IN ({_sql_in(PaymentStatus)})", name="status_valid"),
        CheckConstraint("(status = 'pending') = (processed_at IS NULL)", name="processed_at_matches_status"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64))

    amount: Mapped[Decimal] = mapped_column(Numeric(AMOUNT_PRECISION, AMOUNT_SCALE))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str | None] = mapped_column(String(1000))
    # `metadata` is reserved by SQLAlchemy's declarative API, hence the attribute name.
    metadata_: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, server_default=text("'{}'::jsonb"))

    status: Mapped[str] = mapped_column(String(16), server_default=PaymentStatus.PENDING.value)
    failure_reason: Mapped[str | None] = mapped_column(String(64))

    webhook_url: Mapped[str] = mapped_column(String(2048))
    webhook_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    webhook_last_error: Mapped[str | None] = mapped_column(Text)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class OutboxEvent(Base):
    """Transactional outbox: written in the same transaction as the business change,
    published to the broker afterwards by the outbox relay."""

    __tablename__ = "outbox"
    __table_args__ = (
        # The relay only ever scans unpublished rows; keep that index tiny.
        Index("ix_outbox_unpublished", "created_at", postgresql_where=text("published_at IS NULL")),
        Index("ix_outbox_published_at", "published_at", postgresql_where=text("published_at IS NOT NULL")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(64))
    aggregate_id: Mapped[UUID]
    event_type: Mapped[str] = mapped_column(String(128))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)
