"""Shared fixtures.

Integration tests need a live PostgreSQL with the migrations applied. When one
is not reachable they are skipped rather than failed, so `pytest` still works
on a bare checkout.
"""

from __future__ import annotations

import psycopg
import pytest

from ecommerce_pipeline.config import get_settings


def _database_available() -> bool:
    try:
        with psycopg.connect(get_settings().dsn, connect_timeout=3) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def db_available() -> bool:
    return _database_available()


@pytest.fixture
def db(db_available):
    """A connection that rolls back everything the test did on teardown."""
    if not db_available:
        pytest.skip("no PostgreSQL reachable; set POSTGRES_HOST/PORT to run")

    conn = psycopg.connect(get_settings().dsn)
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


@pytest.fixture
def db_committed(db_available):
    """An autocommit connection, for tests that must span transactions.

    NOW() is the transaction timestamp, so anything asserting that a trigger
    moved a timestamp has to commit between the write and the re-read. Tests
    using this fixture are responsible for removing their own rows.
    """
    if not db_available:
        pytest.skip("no PostgreSQL reachable; set POSTGRES_HOST/PORT to run")

    conn = psycopg.connect(get_settings().dsn, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
