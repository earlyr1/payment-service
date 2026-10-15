import uvicorn

from payment_service.api.app import create_app
from payment_service.config import get_settings
from payment_service.logging import configure_logging


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
        log_config=None,  # keep the logging configured above
        # X-Forwarded-* are trusted only from FORWARDED_ALLOW_IPS (uvicorn reads it; default 127.0.0.1).
        proxy_headers=True,
        timeout_graceful_shutdown=20,
    )


if __name__ == "__main__":
    main()
