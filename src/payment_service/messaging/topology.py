"""RabbitMQ topology: exchanges, queues and bindings.

    outbox relay ──rk=payments.new──► [payments] ──► (payments.new)  consumed by the worker
                                          ▲               │
                 TTL expired, dead-lettered back          │ retriable error, attempts left:
                                          │               ▼
                      (payments.new.retry.<delay>ms) ◄── [payments.retry]

    (payments.new) ── attempts exhausted / non-retriable error ──► [payments.dlx] ──► (payments.new.dlq)
                   └─ delivery limit exceeded (crash loop) ───────┘   (broker-side dead-lettering)

Retry delays are implemented with one TTL queue per attempt (queue-level TTL), so a long
delay never blocks a short one (no head-of-line blocking as with per-message TTL).
The delay is part of the queue name: changing it creates a new queue instead of failing
with PRECONDITION_FAILED on redeclaration.

All queues are quorum queues (replicated, durable; the recommended type since RabbitMQ 4).
Dead-lettering uses the at-least-once strategy so a message is never lost between queues.
"""

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

PAYMENTS_NEW = "payments.new"
PAYMENTS_NEW_DLQ = "payments.new.dlq"

# Headers set by this service on messages it re-publishes.
ATTEMPT_HEADER = "x-attempt"
ERROR_HEADER = "x-last-error"
ERROR_TYPE_HEADER = "x-last-error-type"
POSTPONED_HEADER = "x-postponed"


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    base_delay_seconds: float
    max_delay_seconds: float

    def delay_ms(self, failed_attempt: int) -> int:
        """Delay before the next attempt after `failed_attempt` (1-based) failed: base * 2^(n-1), capped."""
        if failed_attempt < 1:
            raise ValueError("attempts are 1-based")
        delay = min(self.base_delay_seconds * (1 << (failed_attempt - 1)), self.max_delay_seconds)
        return round(delay * 1000)

    def retry_delays_ms(self) -> list[int]:
        """Distinct delays used between attempts. The first one always exists: it is also used to
        postpone a message whose payment is busy, even when max_attempts == 1."""
        return sorted({self.delay_ms(n) for n in range(1, max(2, self.max_attempts))})


def retry_routing_key(delay_ms: int) -> str:
    return f"{PAYMENTS_NEW}.retry.{delay_ms}ms"


def payments_new_queue(delivery_limit: int) -> RabbitQueue:
    return RabbitQueue(
        PAYMENTS_NEW,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=PAYMENTS_NEW,
        arguments={
            "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_DLQ,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",  # required by at-least-once dead-lettering
            "x-delivery-limit": delivery_limit,
        },
    )


def dead_letter_queue() -> RabbitQueue:
    return RabbitQueue(PAYMENTS_NEW_DLQ, queue_type=QueueType.QUORUM, durable=True, routing_key=PAYMENTS_NEW_DLQ)


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = retry_routing_key(delay_ms)
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


async def declare_topology(broker: RabbitBroker, *, retry_policy: RetryPolicy, delivery_limit: int) -> None:
    """Idempotently declare every exchange, queue and binding the service relies on.

    Called on startup by both the publisher and the consumer, so neither depends on
    the other having started first and no message is ever published into the void.
    """
    exchanges = {}
    for exchange in (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DEAD_LETTER_EXCHANGE):
        exchanges[exchange.name] = await broker.declare_exchange(exchange)

    bindings: list[tuple[RabbitQueue, RabbitExchange]] = [
        (payments_new_queue(delivery_limit), PAYMENTS_EXCHANGE),
        (dead_letter_queue(), DEAD_LETTER_EXCHANGE),
        *((retry_queue(delay), RETRY_EXCHANGE) for delay in retry_policy.retry_delays_ms()),
    ]
    for queue, exchange in bindings:
        declared = await broker.declare_queue(queue)
        await declared.bind(exchanges[exchange.name], routing_key=queue.routing())
