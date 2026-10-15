import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine


def advisory_lock_key(namespace: str, value: str) -> int:
    """Map (namespace, value) onto PostgreSQL's signed 64-bit advisory lock key space."""
    digest = hashlib.blake2b(f"{namespace}:{value}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


@asynccontextmanager
async def try_advisory_lock(engine: AsyncEngine, key: int) -> AsyncIterator[bool]:
    """Try to take a session-level advisory lock without waiting; yields whether it was acquired.

    Session-level (not transaction-level) on purpose: no transaction stays open while the
    caller talks to slow external systems. The lock lives as long as the dedicated connection,
    so if the process dies the server releases it automatically.
    """
    async with engine.connect() as conn:
        acquired = bool(await conn.scalar(select(func.pg_try_advisory_lock(key))))
        await conn.commit()  # end the implicit transaction; the session-level lock survives it
        if not acquired:
            yield False
            return
        try:
            yield True
        finally:
            try:
                await conn.scalar(select(func.pg_advisory_unlock(key)))
                await conn.commit()
            except BaseException:
                # Never return a connection that may still hold the lock to the pool:
                # dropping the connection releases the lock server-side.
                await conn.invalidate()
                raise
