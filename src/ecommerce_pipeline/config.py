"""Application configuration, loaded from the environment.

Every tunable the pipeline exposes lives here so that the ETL, the API and the
Flyte tasks all read the same values from the same place.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Repository root, resolved from this file: src/ecommerce_pipeline/config.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Runtime settings. Values come from the environment or a local .env."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- PostgreSQL ---
    postgres_user: str = "ecommerce"
    postgres_password: str = "ecommerce"
    postgres_db: str = "ecommerce"
    postgres_host: str = "localhost"
    postgres_port: int = 5432

    # --- Logging ---
    log_level: str = "INFO"

    # --- ETL ---
    # Rows per COPY batch. Trades round trips against peak memory in the loader.
    etl_batch_size: int = Field(default=50_000, gt=0)
    data_dir: Path = PROJECT_ROOT / "data" / "sample"
    migrations_dir: Path = PROJECT_ROOT / "migrations"

    # --- API ---
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    # Hard ceiling on page size so a single query cannot ask for the whole table.
    api_max_page_size: int = Field(default=200, gt=0)

    @property
    def dsn(self) -> str:
        """libpq connection string used by every psycopg connection."""
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    @property
    def sqlalchemy_url(self) -> str:
        """URL form of the DSN, for tools that expect one (e.g. pandas)."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings singleton."""
    return Settings()
