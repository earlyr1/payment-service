import asyncio
import signal
from datetime import timedelta

from payment_service.config import Settings, get_settings
from payment_service.db.session import create_engine, create_sessionmaker
from payment_service.logging import configure_logging
from payment_service.messaging.broker import create_broker, declare_service_topology
from payment_service.outbox.relay import OutboxRelay


async def run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    engine = create_engine(settings, application_name="payment-service-outbox-relay")
    broker = create_broker(settings, app_id="payment-service-outbox-relay")
    try:
        await broker.connect()
        await declare_service_topology(broker, settings)
        relay = OutboxRelay(
            create_sessionmaker(engine),
            broker,
            batch_size=settings.outbox_batch_size,
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
            publish_timeout_seconds=settings.rabbitmq_timeout_seconds,
            retention=timedelta(hours=settings.outbox_retention_hours),
            cleanup_interval_seconds=settings.outbox_cleanup_interval_seconds,
        )
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
