from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payment_service.api.middleware import RequestIdMiddleware
from payment_service.api.routes import health, payments
from payment_service.config import Settings
from payment_service.db.session import create_engine, create_sessionmaker


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings, application_name="payment-service-api")
        app.state.sessionmaker = create_sessionmaker(engine)
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(
        title="Payment Service",
        version="1.0.0",
        description="Asynchronous payment processing. All `/api/v1` endpoints require the `X-API-Key` header.",
        lifespan=lifespan,
        docs_url="/docs" if settings.api_docs_enabled else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.api_docs_enabled else None,
    )
    app.state.settings = settings
    app.add_middleware(RequestIdMiddleware)
    app.include_router(payments.router)
    app.include_router(health.router)

    return app
