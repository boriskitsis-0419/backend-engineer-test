"""FastAPI application hosting the GraphQL endpoint."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from strawberry.fastapi import GraphQLRouter

from ..config import get_settings
from ..db import close_async_pool, get_async_pool
from ..logging_config import get_logger
from .loaders import Loaders
from .schema import schema

logger = get_logger(__name__)


async def get_context() -> dict:
    """Per-request context.

    Loaders are built fresh for every request: their cache is meant to
    deduplicate work inside a single query, not to act as a cross-request
    cache, which would serve stale rows after a mutation.
    """
    return {"loaders": Loaders()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_async_pool()
    logger.info("API ready")
    yield
    await close_async_pool()


app = FastAPI(
    title="E-commerce Analytics API",
    version="0.1.0",
    description="GraphQL API over the e-commerce analytics warehouse.",
    lifespan=lifespan,
)

app.include_router(
    GraphQLRouter(schema, context_getter=get_context),
    prefix="/graphql",
)


@app.get("/health", tags=["ops"])
async def health() -> JSONResponse:
    """Liveness plus database readiness, used by the Compose health check."""
    settings = get_settings()
    try:
        pool = await get_async_pool()
        async with pool.connection() as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:
        logger.warning("health check failed: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "unreachable"},
        )
    return JSONResponse(
        content={
            "status": "ok",
            "database": f"{settings.postgres_host}:{settings.postgres_port}",
        }
    )


@app.get("/", include_in_schema=False)
async def root() -> JSONResponse:
    return JSONResponse(
        content={
            "service": "e-commerce analytics API",
            "graphql": "/graphql",
            "health": "/health",
        }
    )
