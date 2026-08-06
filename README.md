# E-commerce Analytics Pipeline

Data processing pipeline for an e-commerce analytics platform: a partitioned
PostgreSQL schema, a bulk-loading Python ETL, a GraphQL API and a Flyte
workflow that orchestrates the whole thing.

> Built for the Backend Engineer take-home test. See
> [`backend-engineer-test.md`](backend-engineer-test.md) for the brief and
> [`docs/technical-document.md`](docs/technical-document.md) for the design
> write-up.

## Quick start

Requires Docker with the Compose plugin. Nothing else — Python, Postgres and
`psql` all run inside containers.

```bash
cp .env.example .env
docker compose up -d --build     # postgres -> migrations -> API
curl localhost:8000/health
```

The API is served on <http://localhost:8000>, Postgres is published on
`localhost:5433` (host port chosen to avoid clashing with a local Postgres).

## Layout

```
migrations/                 numbered, forward-only SQL migrations
src/ecommerce_pipeline/
  config.py                 environment-backed settings
  db.py                     connection handling + pooling
  logging_config.py         shared logging setup
  migrations/runner.py      applies migrations/*.sql, tracks checksums
  api/                      FastAPI + GraphQL
scripts/                    data generation and operational helpers
tests/                      unit and integration tests
docs/                       technical write-up
data/sample/                committed sample CSVs used by the demo
```

## Common commands

| Task | Command |
| --- | --- |
| Bring the stack up | `docker compose up -d --build` |
| Migration status | `docker compose run --rm migrate python -m ecommerce_pipeline.migrations.runner status` |
| Apply migrations | `docker compose run --rm migrate` |
| Run the ETL | `docker compose run --rm etl` |
| Open a psql shell | `docker compose exec postgres psql -U ecommerce -d ecommerce` |
| Run the tests | `docker compose run --rm api pytest` |
| Tear down (keep data) | `docker compose down` |
| Tear down (drop data) | `docker compose down -v` |

## Configuration

All settings are environment variables, documented in
[`.env.example`](.env.example) and parsed by
[`config.py`](src/ecommerce_pipeline/config.py). The defaults work as-is for
local development.

## Development outside Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
export POSTGRES_HOST=localhost POSTGRES_PORT=5433
pytest
```
