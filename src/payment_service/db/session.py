from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from payment_service.config import Settings


def create_engine(settings: Settings, *, application_name: str) -> AsyncEngine:
    return create_async_engine(
        settings.database_url.unicode_string(),
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_seconds,
        pool_pre_ping=True,
        connect_args={
            "server_settings": {
                "application_name": application_name,
                "statement_timeout": str(settings.db_statement_timeout_ms),
                # A transaction left open by a bug must not hold locks forever.
                "idle_in_transaction_session_timeout": "60000",
            },
        },
    )


def create_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
