from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel
from faststream.rabbit.annotations import RabbitMessage

from payment_service.config import Settings
from payment_service.consumer.handler import PaymentMessageHandler
from payment_service.db.session import create_engine
from payment_service.gateway.emulator import EmulatedGateway, PaymentGateway
from payment_service.messaging.broker import create_broker, declare_service_topology, retry_policy_from
from payment_service.messaging.topology import PAYMENTS_EXCHANGE, payments_new_queue
from payment_service.services.processing import PaymentProcessor
from payment_service.webhooks.sender import WebhookSender


def create_consumer_app(settings: Settings, *, gateway: PaymentGateway | None = None) -> FastStream:
    # Every in-flight message holds a connection for its processing lock and needs one more for queries.
    if settings.db_pool_size + settings.db_max_overflow < 2 * settings.consumer_prefetch_count:
        raise ValueError("DB_POOL_SIZE + DB_MAX_OVERFLOW must be at least 2 * CONSUMER_PREFETCH_COUNT")

    broker = create_broker(settings, app_id="payment-service-consumer")
    retry_policy = retry_policy_from(settings)

    engine = create_engine(settings, application_name="payment-service-consumer")
    http_client = WebhookSender.create_client(settings.webhook_timeout_seconds)
    processor = PaymentProcessor(
        engine=engine,
        gateway=gateway
        or EmulatedGateway(
            min_delay_seconds=settings.gateway_min_delay_seconds,
            max_delay_seconds=settings.gateway_max_delay_seconds,
            success_rate=settings.gateway_success_rate,
        ),
        webhook_sender=WebhookSender(
            http_client,
            signing_secret=settings.webhook_signing_secret.get_secret_value(),
            allow_private_targets=settings.webhook_allow_private_targets,
            total_timeout_seconds=settings.webhook_timeout_seconds,
        ),
    )
    handler = PaymentMessageHandler(
        broker, processor, retry_policy, publish_timeout_seconds=settings.rabbitmq_timeout_seconds
    )

    @broker.subscriber(
        payments_new_queue(settings.consumer_delivery_limit),
        PAYMENTS_EXCHANGE,
        # Messages are processed concurrently up to the prefetch limit (the work is I/O bound).
        channel=Channel(prefetch_count=settings.consumer_prefetch_count),
        ack_policy=AckPolicy.MANUAL,  # the handler settles every message itself
    )
    async def on_payment_created(message: RabbitMessage) -> None:
        await handler(message)

    async def declare() -> None:
        await broker.connect()
        await declare_service_topology(broker, settings)

    async def release_resources() -> None:
        await http_client.aclose()
        await engine.dispose()

    return FastStream(broker, on_startup=[declare], after_shutdown=[release_resources])
