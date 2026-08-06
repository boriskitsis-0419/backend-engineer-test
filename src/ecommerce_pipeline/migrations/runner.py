"""Apply the numbered SQL migrations in ``migrations/``.

Deliberately minimal and forward-only. Each ``NNN_name.sql`` file is applied
once, inside its own transaction, and recorded in ``schema_migrations`` with a
checksum so that an already-applied file cannot be edited unnoticed.

    python -m ecommerce_pipeline.migrations.runner up
    python -m ecommerce_pipeline.migrations.runner status
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg

from ..config import get_settings
from ..db import connect, wait_for_database
from ..logging_config import get_logger

logger = get_logger(__name__)

# Guards against two runners (e.g. compose restarting the migrate service)
# applying the same migration concurrently. The value is arbitrary but fixed.
_ADVISORY_LOCK_KEY = 4_812_233_901

_FILENAME_RE = re.compile(r"^(?P<version>\d+)_(?P<name>.+)\.sql$")

_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     BIGINT      PRIMARY KEY,
    name        TEXT        NOT NULL,
    checksum    TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INTEGER     NOT NULL
);
"""


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path

    @property
    def sql(self) -> str:
        return self.path.read_text(encoding="utf-8")

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"{self.version:03d}_{self.name}"


def discover(migrations_dir: Path) -> list[Migration]:
    """Return every migration on disk, ordered by version.

    Raises if two files claim the same version, which would otherwise make the
    apply order depend on the filesystem.
    """
    if not migrations_dir.is_dir():
        raise FileNotFoundError(f"migrations directory not found: {migrations_dir}")

    found: dict[int, Migration] = {}
    for path in sorted(migrations_dir.glob("*.sql")):
        match = _FILENAME_RE.match(path.name)
        if not match:
            logger.warning("skipping %s: expected NNN_name.sql", path.name)
            continue
        version = int(match.group("version"))
        if version in found:
            raise ValueError(
                f"duplicate migration version {version}: "
                f"{found[version].path.name} and {path.name}"
            )
        found[version] = Migration(version, match.group("name"), path)

    return [found[v] for v in sorted(found)]


def _applied(conn: psycopg.Connection) -> dict[int, str]:
    """Map of version -> checksum for migrations already applied."""
    with conn.cursor() as cur:
        cur.execute("SELECT version, checksum FROM schema_migrations")
        return {row[0]: row[1] for row in cur.fetchall()}


def _verify_checksums(pending_source: list[Migration], applied: dict[int, str]) -> None:
    """Fail loudly if an already-applied migration was modified on disk."""
    for migration in pending_source:
        recorded = applied.get(migration.version)
        if recorded is not None and recorded != migration.checksum:
            raise RuntimeError(
                f"migration {migration} was modified after being applied "
                f"(recorded {recorded[:12]}…, on disk {migration.checksum[:12]}…). "
                "Forward-only: add a new migration instead of editing this one."
            )


def migrate_up(migrations_dir: Path | None = None) -> list[Migration]:
    """Apply every pending migration. Returns the ones that ran."""
    settings = get_settings()
    migrations_dir = migrations_dir or settings.migrations_dir
    migrations = discover(migrations_dir)

    wait_for_database(settings)
    applied_now: list[Migration] = []

    with connect(autocommit=True) as conn:
        conn.execute(_BOOTSTRAP)
        # Serialise concurrent runners; released when the session ends.
        conn.execute("SELECT pg_advisory_lock(%s)", (_ADVISORY_LOCK_KEY,))
        try:
            already = _applied(conn)
            _verify_checksums(migrations, already)

            pending = [m for m in migrations if m.version not in already]
            if not pending:
                logger.info("database is up to date (%d migrations applied)", len(already))
                return []

            logger.info("applying %d pending migration(s)", len(pending))
            for migration in pending:
                started = time.perf_counter()
                # Each migration is atomic: a failure rolls back that file's
                # statements and leaves schema_migrations untouched.
                with conn.transaction():
                    conn.execute(migration.sql)
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    conn.execute(
                        "INSERT INTO schema_migrations "
                        "(version, name, checksum, duration_ms) VALUES (%s, %s, %s, %s)",
                        (migration.version, migration.name, migration.checksum, elapsed_ms),
                    )
                logger.info("  applied %s (%d ms)", migration, elapsed_ms)
                applied_now.append(migration)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_KEY,))

    logger.info("migration complete: %d applied", len(applied_now))
    return applied_now


def migration_status(migrations_dir: Path | None = None) -> int:
    """Print which migrations are applied vs pending. Returns pending count."""
    settings = get_settings()
    migrations_dir = migrations_dir or settings.migrations_dir
    migrations = discover(migrations_dir)

    wait_for_database(settings)
    with connect(autocommit=True) as conn:
        conn.execute(_BOOTSTRAP)
        already = _applied(conn)

    pending = 0
    for migration in migrations:
        if migration.version in already:
            print(f"  [applied] {migration}")
        else:
            print(f"  [pending] {migration}")
            pending += 1
    print(f"\n{len(migrations) - pending} applied, {pending} pending")
    return pending


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("command", choices=["up", "status"], nargs="?", default="up")
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=None,
        help="override the directory containing NNN_name.sql files",
    )
    args = parser.parse_args(argv)

    try:
        if args.command == "up":
            migrate_up(args.migrations_dir)
        else:
            migration_status(args.migrations_dir)
    except Exception as exc:
        logger.error("migration failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
