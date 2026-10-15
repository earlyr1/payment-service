"""Message contracts exchanged through the broker.

These models are the wire format: change them only in a backward-compatible way
(add optional fields), or introduce a new `type`.
"""

from datetime import datetime
from typing import Final, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

PAYMENT_CREATED: Final = "payment.created"


class PaymentCreatedEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: UUID
    type: Literal["payment.created"] = PAYMENT_CREATED
    occurred_at: datetime
    payment_id: UUID
