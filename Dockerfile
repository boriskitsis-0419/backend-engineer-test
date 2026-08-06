FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# postgresql-client is included so `psql` is available inside the container for
# debugging and for the psql-based smoke checks in the README.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the dependency manifest first so the pip layer is cached across
# source changes.
COPY pyproject.toml README.md ./
RUN mkdir -p src/ecommerce_pipeline \
    && touch src/ecommerce_pipeline/__init__.py \
    && pip install --no-cache-dir -e ".[dev]"

COPY src/ ./src/
COPY migrations/ ./migrations/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

# Re-run the install so the real package metadata (entry points) is registered.
RUN pip install --no-cache-dir --no-deps -e .

RUN useradd --create-home --uid 1000 appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["uvicorn", "ecommerce_pipeline.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
