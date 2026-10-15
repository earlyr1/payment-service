import asyncio
import logging
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ChargeResult:
    succeeded: bool
    failure_reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(self, *, payment_id: UUID, amount: Decimal, currency: str) -> ChargeResult:
        """Charge the payment. `payment_id` doubles as the gateway idempotency key.

        Returns a business outcome (approved / declined). Technical problems
        (timeouts, 5xx from the gateway) are raised as exceptions and retried.
        """
        ...


class EmulatedGateway:
    """Stand-in for an external acquirer: random latency, configurable approval rate."""

    def __init__(
        self,
        *,
        min_delay_seconds: float,
        max_delay_seconds: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay_seconds
        self._max_delay = max_delay_seconds
        self._success_rate = success_rate
        self._rng = rng or random.Random()  # noqa: S311 - emulation, not cryptography

    async def charge(self, *, payment_id: UUID, amount: Decimal, currency: str) -> ChargeResult:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            result = ChargeResult(succeeded=True)
        else:
            result = ChargeResult(succeeded=False, failure_reason="declined_by_gateway")
        logger.info(
            "gateway charge finished",
            extra={"payment_id": str(payment_id), "succeeded": result.succeeded, "reason": result.failure_reason},
        )
        return result
