import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status

from payment_service.api.deps import get_payment_service, idempotency_key, require_api_key
from payment_service.api.schemas import ErrorResponse, PaymentAcceptedResponse, PaymentCreateRequest, PaymentResponse
from payment_service.domain.enums import Currency
from payment_service.services.payments import CreatePaymentCommand, IdempotencyKeyReusedError, PaymentService

logger = logging.getLogger(__name__)

IDEMPOTENT_REPLAYED_HEADER = "Idempotent-Replayed"

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse, "description": "Missing or invalid API key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAcceptedResponse,
    summary="Create a payment",
    description="Accepts the payment for asynchronous processing. The result is delivered to `webhook_url` "
    "and can be polled via GET. Requests are idempotent by the `Idempotency-Key` header.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse, "description": "Missing or malformed Idempotency-Key"},
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "description": "Invalid body, or Idempotency-Key reused with a different body",
        },
    },
)
async def create_payment(
    body: PaymentCreateRequest,
    response: Response,
    key: Annotated[str, Depends(idempotency_key)],
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentAcceptedResponse:
    command = CreatePaymentCommand(
        amount=body.amount,
        currency=Currency(body.currency),
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
    )
    try:
        result = await service.create_payment(command, idempotency_key=key)
    except IdempotencyKeyReusedError:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="Idempotency-Key has already been used with different request parameters",
        ) from None

    payment = result.payment
    response.headers["Location"] = f"{router.prefix}/{payment.id}"
    if result.replayed:
        response.headers[IDEMPOTENT_REPLAYED_HEADER] = "true"
    else:
        logger.info(
            "payment created",
            extra={"payment_id": str(payment.id), "amount": str(payment.amount), "currency": payment.currency},
        )
    return PaymentAcceptedResponse.from_model(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Get a payment",
    responses={status.HTTP_404_NOT_FOUND: {"model": ErrorResponse, "description": "Payment not found"}},
)
async def get_payment(
    payment_id: UUID,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentResponse:
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentResponse.from_model(payment)
