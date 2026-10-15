from functools import lru_cache
from typing import Literal

from pydantic import Field, PostgresDsn, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service configuration, read from environment variables (and `.env` if present)."""

    # hide_input_in_errors: a misconfigured secret must not end up in startup logs.
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True
    )

    # --- common ---
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_format: Literal["json", "console"] = "json"

    # --- database ---
    database_url: PostgresDsn = Field(
        description="SQLAlchemy URL, must use the asyncpg driver: postgresql+asyncpg://...",
    )
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    db_pool_timeout_seconds: float = Field(default=10.0, gt=0)
    db_statement_timeout_ms: int = Field(default=10_000, ge=0)

    # --- broker ---
    rabbitmq_url: SecretStr = Field(description="amqp://user:password@host:5672/vhost")
    rabbitmq_timeout_seconds: float = Field(default=10.0, gt=0, description="Connect and publish-confirm timeout")

    # --- API ---
    api_key: SecretStr = Field(min_length=16, description="Static key expected in the X-API-Key header")
    api_host: str = "0.0.0.0"  # noqa: S104 - the API is meant to listen inside a container
    api_port: int = 8000
    api_docs_enabled: bool = True

    # --- outbox relay ---
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1, le=10_000)
    outbox_retention_hours: int = Field(default=72, ge=1)
    outbox_cleanup_interval_seconds: float = Field(default=3600, gt=0)

    # --- consumer ---
    consumer_prefetch_count: int = Field(default=10, ge=1)
    consumer_max_attempts: int = Field(default=3, ge=1, le=10)
    consumer_retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    consumer_retry_max_delay_seconds: float = Field(default=300.0, gt=0)
    consumer_delivery_limit: int = Field(
        default=10,
        ge=1,
        description="Quorum queue safety net: redeliveries (crashes, nacks) before the broker dead-letters a message",
    )

    # --- payment gateway emulator ---
    gateway_min_delay_seconds: float = Field(default=2.0, ge=0)
    gateway_max_delay_seconds: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    # --- webhooks ---
    webhook_signing_secret: SecretStr = Field(min_length=16)
    webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    webhook_allow_private_targets: bool = Field(
        default=False,
        description="Allow webhooks to loopback/private/link-local addresses. Never enable in production.",
    )

    @model_validator(mode="after")
    def _check_consistency(self) -> Settings:
        if self.database_url.scheme != "postgresql+asyncpg":
            raise ValueError("DATABASE_URL must use the postgresql+asyncpg:// scheme")
        if self.gateway_min_delay_seconds > self.gateway_max_delay_seconds:
            raise ValueError("GATEWAY_MIN_DELAY_SECONDS must not exceed GATEWAY_MAX_DELAY_SECONDS")
        if self.consumer_retry_base_delay_seconds > self.consumer_retry_max_delay_seconds:
            raise ValueError("CONSUMER_RETRY_BASE_DELAY_SECONDS must not exceed CONSUMER_RETRY_MAX_DELAY_SECONDS")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # values come from the environment
