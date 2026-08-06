"""FastAPI application hosting the GraphQL endpoint.

The GraphQL router is mounted in a later step; for now the app exposes the
health endpoint that Compose uses to gate service startup.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from ..config import get_settings
from ..db import close_pool, get_pool
from ..logging_config import get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the connection pool on startup and close it on shutdown."""
    get_pool()
    logger.info("API ready")
    yield
    close_pool()


app = FastAPI(
    title="E-commerce Analytics API",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", tags=["ops"])
def health() -> JSONResponse:
    """Liveness + database readiness probe."""
    settings = get_settings()
    try:
        with get_pool().connection() as conn:
            conn.execute("SELECT 1")
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
