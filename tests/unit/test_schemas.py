from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from payment_service.api.schemas import MAX_METADATA_BYTES, PaymentCreateRequest
from payment_service.domain.enums import Currency
from payment_service.services.payments import CreatePaymentCommand


def _request(**overrides: Any) -> dict[str, Any]:
    return {"amount": "100.50", "currency": "RUB", "webhook_url": "https://example.com/hook", **overrides}


def _command(body: PaymentCreateRequest) -> CreatePaymentCommand:
    return CreatePaymentCommand(
        amount=body.amount,
        currency=Currency(body.currency),
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
    )


@pytest.mark.parametrize(
    ("raw", "expected"), [("100", "100.00"), ("100.5", "100.50"), (100, "100.00"), ("0.01", "0.01")]
)
def test_amount_is_normalized_to_two_decimals(raw: Any, expected: str) -> None:
    assert PaymentCreateRequest.model_validate(_request(amount=raw)).amount == Decimal(expected)
    assert str(PaymentCreateRequest.model_validate(_request(amount=raw)).amount) == expected


@pytest.mark.parametrize("raw", ["0", "-1", "100.001", "1e20", "abc", "NaN", "Infinity"])
def test_invalid_amounts_are_rejected(raw: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(_request(amount=raw))


def test_unknown_currency_is_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(_request(currency="GBP"))


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(_request(status="succeeded"))


@pytest.mark.parametrize("url", ["ftp://example.com/hook", "not a url", "javascript:alert(1)"])
def test_webhook_url_must_be_http(url: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(_request(webhook_url=url))


def test_metadata_size_is_limited() -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(_request(metadata={"blob": "x" * MAX_METADATA_BYTES}))


def test_fingerprint_ignores_amount_formatting_and_key_order() -> None:
    a = PaymentCreateRequest.model_validate(_request(amount="100.5", metadata={"a": 1, "b": 2}))
    b = PaymentCreateRequest.model_validate(_request(amount=100.50, metadata={"b": 2, "a": 1}))
    assert _command(a).fingerprint() == _command(b).fingerprint()


def test_fingerprint_changes_with_parameters() -> None:
    a = PaymentCreateRequest.model_validate(_request())
    b = PaymentCreateRequest.model_validate(_request(currency="USD"))
    assert _command(a).fingerprint() != _command(b).fingerprint()


@pytest.mark.parametrize(
    "overrides",
    [{"description": "a\x00b"}, {"metadata": {"k": "\x00"}}, {"metadata": {"\x00": 1}}, {"metadata": {"k": ["\x00"]}}],
)
def test_nul_characters_are_rejected(overrides: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        PaymentCreateRequest.model_validate(_request(**overrides))
