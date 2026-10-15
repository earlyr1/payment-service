import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from payment_service.db.locks import advisory_lock_key, try_advisory_lock

pytestmark = pytest.mark.integration


async def test_lock_is_exclusive_and_released(engine: AsyncEngine) -> None:
    key = advisory_lock_key("tests", "payment-1")

    async with try_advisory_lock(engine, key) as first:
        assert first
        async with try_advisory_lock(engine, key) as second:
            assert not second
        async with try_advisory_lock(engine, advisory_lock_key("tests", "payment-2")) as other:
            assert other

    async with try_advisory_lock(engine, key) as again:
        assert again


async def test_lock_is_released_when_the_body_fails(engine: AsyncEngine) -> None:
    key = advisory_lock_key("tests", "payment-3")
    with pytest.raises(RuntimeError):
        async with try_advisory_lock(engine, key):
            raise RuntimeError("boom")

    async with try_advisory_lock(engine, key) as acquired:
        assert acquired
