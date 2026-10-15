import json
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from payment_service.db.models import AMOUNT_PRECISION, AMOUNT_SCALE, Payment
from payment_service.domain.enums import Currency, PaymentStatus

MAX_METADATA_BYTES = 8 * 1024
MAX_WEBHOOK_URL_LENGTH = 2048

Amount = Annotated[
    Decimal,
    Field(
        gt=0,
        max_digits=AMOUNT_PRECISION,
        decimal_places=AMOUNT_SCALE,
        description="Positive amount with at most 2 decimal places. Accepted as a JSON string (recommended) or "
        "number; always returned as a string to avoid floating point loss.",
        examples=["1500.00"],
    ),
]


def _contains_nul(value: Any) -> bool:
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_contains_nul(k) or _contains_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_contains_nul(v) for v in value)
    return False


class PaymentCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str | None = Field(default=None, min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=f"Arbitrary JSON object stored with the payment, up to {MAX_METADATA_BYTES} bytes serialized",
    )
    webhook_url: HttpUrl = Field(description="Receives a POST with the payment result")

    @field_validator("amount")
    @classmethod
    def _normalize_amount(cls, value: Decimal) -> Decimal:
        # "100", "100.0" and "100.00" are the same amount and must produce the same idempotency fingerprint.
        return value.quantize(Decimal(1).scaleb(-AMOUNT_SCALE))

    @field_validator("description", "metadata")
    @classmethod
    def _reject_nul(cls, value: Any) -> Any:
        # PostgreSQL cannot store U+0000 in text or jsonb.
        if _contains_nul(value):
            raise ValueError("must not contain NUL characters")
        return value

    @field_validator("metadata")
    @classmethod
    def _limit_metadata_size(cls, value: dict[str, Any]) -> dict[str, Any]:
        if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata must not exceed {MAX_METADATA_BYTES} bytes when serialized")
        return value

    @field_validator("webhook_url")
    @classmethod
    def _limit_webhook_url_length(cls, value: HttpUrl) -> HttpUrl:
        if len(str(value)) > MAX_WEBHOOK_URL_LENGTH:
            raise ValueError(f"webhook_url must not exceed {MAX_WEBHOOK_URL_LENGTH} characters")
        return value


class PaymentAcceptedResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentAcceptedResponse:
        return cls(payment_id=payment.id, status=PaymentStatus(payment.status), created_at=payment.created_at)


class PaymentResponse(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    webhook_url: str
    idempotency_key: str
    failure_reason: str | None = Field(description="Gateway decline reason, set when status is `failed`")
    created_at: datetime
    processed_at: datetime | None = Field(description="When the gateway returned a final result")

    @classmethod
    def from_model(cls, payment: Payment) -> PaymentResponse:
        return cls(
            payment_id=payment.id,
            status=PaymentStatus(payment.status),
            amount=payment.amount,
            currency=Currency(payment.currency),
            description=payment.description,
            metadata=payment.metadata_,
            webhook_url=payment.webhook_url,
            idempotency_key=payment.idempotency_key,
            failure_reason=payment.failure_reason,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorResponse(BaseModel):
    detail: str
