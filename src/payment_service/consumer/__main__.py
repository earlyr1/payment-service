import asyncio

from payment_service.config import get_settings
from payment_service.consumer.app import create_consumer_app
from payment_service.logging import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    # FastStream handles SIGINT/SIGTERM: stops consuming and waits for in-flight messages.
    asyncio.run(create_consumer_app(settings).run())


if __name__ == "__main__":
    main()
