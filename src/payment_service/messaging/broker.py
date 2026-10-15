import logging

from faststream.rabbit import Channel, RabbitBroker

from payment_service.config import Settings
from payment_service.messaging.topology import RetryPolicy, declare_topology


def create_broker(settings: Settings, *, app_id: str) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        app_id=app_id,
        # Publisher confirms: publish() returns only once the broker has taken responsibility
        # for the message. on_return_raises: an unroutable `mandatory` message raises instead
        # of being silently dropped.
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        timeout=settings.rabbitmq_timeout_seconds,
        logger=logging.getLogger("faststream"),
        graceful_timeout=30.0,
    )


def retry_policy_from(settings: Settings) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=settings.consumer_max_attempts,
        base_delay_seconds=settings.consumer_retry_base_delay_seconds,
        max_delay_seconds=settings.consumer_retry_max_delay_seconds,
    )


async def declare_service_topology(broker: RabbitBroker, settings: Settings) -> None:
    await declare_topology(
        broker, retry_policy=retry_policy_from(settings), delivery_limit=settings.consumer_delivery_limit
    )
