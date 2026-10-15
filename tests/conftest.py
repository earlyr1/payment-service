import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from payment_service.config import Settings
from payment_service.db.session import create_engine, create_sessionmaker

POSTGRES_IMAGE = "postgres:18.6-alpine"
RABBITMQ_IMAGE = "rabbitmq:4.3.6-management-alpine"
ROOT = Path(__file__).resolve().parent.parent

TEST_API_KEY = "test-api-key-0123456789"
TEST_SIGNING_SECRET = "test-signing-secret-0123456789"


def make_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://user:pass@localhost:5432/db",
        "rabbitmq_url": "amqp://guest:guest@localhost:5672/",
        "api_key": TEST_API_KEY,
        "webhook_signing_secret": TEST_SIGNING_SECRET,
        "log_format": "console",
        "log_level": "DEBUG",
        "gateway_min_delay_seconds": 0,
        "gateway_max_delay_seconds": 0,
        "gateway_success_rate": 1.0,
        "consumer_retry_base_delay_seconds": 0.2,
        "webhook_allow_private_targets": True,
        "webhook_timeout_seconds": 2,
        "outbox_poll_interval_seconds": 0.05,
        **overrides,
    }
    return Settings(_env_file=None, **values)  # type: ignore[arg-type]


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver="asyncpg") as container:
        url = container.get_connection_url()
        config = Config(str(ROOT / "alembic.ini"))
        previous = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = url
        try:
            command.upgrade(config, "head")
        finally:
            if previous is None:
                del os.environ["DATABASE_URL"]
            else:
                os.environ["DATABASE_URL"] = previous
        yield url


@pytest.fixture(scope="session")
def rabbitmq_url() -> Iterator[str]:
    container = (
        DockerContainer(RABBITMQ_IMAGE)
        .with_exposed_ports(5672)
        .waiting_for(LogMessageWaitStrategy("Server startup complete").with_startup_timeout(120))
    )
    with container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


@pytest.fixture(scope="session")
def db_settings(postgres_url: str) -> Settings:
    """Settings for tests that need only the database (no RabbitMQ container is started)."""
    return make_settings(database_url=postgres_url)


@pytest.fixture(scope="session")
def settings(postgres_url: str, rabbitmq_url: str) -> Settings:
    return make_settings(database_url=postgres_url, rabbitmq_url=rabbitmq_url)


@pytest.fixture(scope="session")
async def engine(db_settings: Settings) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(db_settings, application_name="tests")
    yield engine
    await engine.dispose()


@pytest.fixture(scope="session")
def sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_sessionmaker(engine)


@pytest.fixture
async def clean_db(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
