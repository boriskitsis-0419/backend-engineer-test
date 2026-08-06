"""PostgreSQL connectivity: startup waiting, one-off connections and pooling."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import AsyncConnectionPool, ConnectionPool
from tenacity import retry, retry_if_exception_type, stop_after_delay, wait_fixed

from .config import Settings, get_settings
from .logging_config import get_logger

logger = get_logger(__name__)

_pool: ConnectionPool | None = None
_async_pool: AsyncConnectionPool | None = None


@retry(
    retry=retry_if_exception_type(psycopg.OperationalError),
    wait=wait_fixed(2),
    stop=stop_after_delay(60),
    reraise=True,
)
def wait_for_database(settings: Settings | None = None) -> None:
    """Block until Postgres accepts connections, or give up after 60s.

    Compose health checks already gate service startup, but the ETL and the
    Flyte tasks may run against a database that is still recovering, so every
    entry point calls this first.
    """
    settings = settings or get_settings()
    with psycopg.connect(settings.dsn, connect_timeout=5) as conn:
        conn.execute("SELECT 1")
    logger.debug("database is accepting connections")


@contextmanager
def connect(*, autocommit: bool = False) -> Iterator[psycopg.Connection]:
    """Yield a single short-lived connection.

    Used by the ETL and the migration runner, which want full control over
    transaction boundaries rather than a pooled connection.
    """
    settings = get_settings()
    conn = psycopg.connect(settings.dsn, autocommit=autocommit)
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def get_pool() -> ConnectionPool:
    """Return the process-wide connection pool, creating it on first use.

    The API uses this so that concurrent GraphQL resolvers share a bounded set
    of connections instead of opening one per request.
    """
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = ConnectionPool(
            conninfo=settings.dsn,
            min_size=1,
            max_size=10,
            max_idle=300,
            open=False,
            name="ecommerce-api",
        )
        _pool.open()
        logger.info("opened connection pool (max_size=%d)", 10)
    return _pool


def close_pool() -> None:
    """Close the shared pool, if one was opened."""
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
        logger.info("closed connection pool")


async def get_async_pool() -> AsyncConnectionPool:
    """Return the process-wide async pool, opening it on first use.

    The GraphQL layer is async because DataLoader batching is: a loader
    gathers the keys requested across a tick of the event loop and issues one
    query for all of them. That only works if the resolvers awaiting it can
    yield, which rules out a synchronous driver.
    """
    global _async_pool
    if _async_pool is None:
        settings = get_settings()
        _async_pool = AsyncConnectionPool(
            conninfo=settings.dsn,
            min_size=1,
            max_size=10,
            max_idle=300,
            open=False,
            name="ecommerce-api-async",
        )
        await _async_pool.open(wait=True, timeout=30)
        logger.info("opened async connection pool (max_size=%d)", 10)
    return _async_pool


async def close_async_pool() -> None:
    global _async_pool
    if _async_pool is not None:
        await _async_pool.close()
        _async_pool = None
        logger.info("closed async connection pool")
