"""Bulk loading into PostgreSQL.

Strategy, per entity:

1. Create an unlogged staging table shaped exactly like the target's loadable
   columns, via ``CREATE TEMP TABLE ... AS SELECT ... WITH NO DATA``. Deriving
   it from the target means the column types always agree, and CTAS drops the
   constraints, defaults and GENERATED-ness that would otherwise make the
   staging copy reject perfectly good input.
2. Stream every validated chunk into it with ``COPY ... FROM STDIN``. COPY is
   roughly an order of magnitude faster than row-wise INSERT because it avoids
   per-statement parse/plan overhead and writes in far fewer round trips.
3. Move the whole staging table into the target in a single
   ``INSERT ... SELECT ... ON CONFLICT DO UPDATE``.

Staging the full entity before the upsert -- rather than upserting per chunk --
also fixes an ordering hazard: product_categories references itself, so a
subcategory in chunk 2 whose parent is in chunk 1 would fail a per-chunk load.
One statement defers all foreign-key checks to the end of that statement.
"""

from __future__ import annotations

import io
import time
from dataclasses import dataclass, field

import pandas as pd
import psycopg

from ..logging_config import get_logger
from .spec import EntitySpec

logger = get_logger(__name__)

_INTEGER_TYPES = {"smallint", "integer", "bigint"}


@dataclass
class LoadStats:
    entity: str
    staged: int = 0
    inserted: int = 0
    updated: int = 0
    seconds: float = 0.0

    @property
    def affected(self) -> int:
        return self.inserted + self.updated

    @property
    def rows_per_second(self) -> float:
        return self.staged / self.seconds if self.seconds > 0 else 0.0

    def __str__(self) -> str:  # pragma: no cover - logging sugar
        return (
            f"{self.entity}: staged={self.staged} affected={self.affected} "
            f"in {self.seconds:.1f}s ({self.rows_per_second:,.0f} rows/s)"
        )


@dataclass
class Loader:
    """Loads one entity through a staging table."""

    conn: psycopg.Connection
    spec: EntitySpec
    stats: LoadStats = field(init=False)

    def __post_init__(self) -> None:
        self.stats = LoadStats(entity=self.spec.name)
        self._column_types: dict[str, str] = {}

    # -- staging ----------------------------------------------------------

    @property
    def staging_table(self) -> str:
        return f"stg_{self.spec.name}"

    def create_staging(self) -> None:
        columns = ", ".join(self.spec.columns)
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {self.staging_table}")
            # CTAS from the target guarantees identical column types while
            # discarding constraints, defaults and generated expressions.
            cur.execute(
                f"CREATE TEMP TABLE {self.staging_table} AS "
                f"SELECT {columns} FROM {self.spec.table} WITH NO DATA"
            )
        self._column_types = self._load_column_types()
        logger.debug("created staging table %s", self.staging_table)

    def _load_column_types(self) -> dict[str, str]:
        """Target column data types, used to coerce values before COPY."""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = %s AND table_schema = 'public'",
                (self.spec.table,),
            )
            return {name: dtype for name, dtype in cur.fetchall()}

    # -- copy -------------------------------------------------------------

    def _coerce(self, df: pd.DataFrame) -> pd.DataFrame:
        """Make a frame safe to serialise for COPY.

        Validation coerces ids with ``pd.to_numeric``, which yields float64
        whenever the column contains a null. Serialising that writes "1.0",
        which PostgreSQL rejects for an INTEGER column. Casting to pandas'
        nullable Int64 restores "1" while still allowing nulls.
        """
        out = df.loc[:, list(self.spec.columns)].copy()
        for column in self.spec.columns:
            dtype = self._column_types.get(column, "")
            if dtype in _INTEGER_TYPES:
                out[column] = pd.to_numeric(out[column], errors="coerce").round(0).astype("Int64")
        return out

    def copy_chunk(self, df: pd.DataFrame) -> int:
        """Stream one validated chunk into the staging table."""
        if df.empty:
            return 0

        frame = self._coerce(df)
        buffer = io.StringIO()
        frame.to_csv(buffer, index=False, header=False, na_rep="")
        buffer.seek(0)

        columns = ", ".join(frame.columns)
        statement = (
            f"COPY {self.staging_table} ({columns}) FROM STDIN WITH (FORMAT csv)"
        )
        with self.conn.cursor() as cur, cur.copy(statement) as copy:
            copy.write(buffer.read())

        self.stats.staged += len(frame)
        return len(frame)

    # -- referential quarantine -------------------------------------------

    def quarantine_orphans(self) -> tuple[dict[str, int], pd.DataFrame]:
        """Remove staged rows whose foreign-key parent does not exist.

        The database enforces these constraints, but it does so by aborting
        the entire INSERT: one order referencing a deleted customer would take
        the other 4,999,999 down with it. Resolving the references in staging
        first turns a fatal load error into a per-row rejection, which is the
        behaviour a nightly pipeline needs.

        Returns the count per constraint and the removed rows for quarantine.
        """
        removed: dict[str, int] = {}
        frames: list[pd.DataFrame] = []

        for fk in self.spec.foreign_keys:
            join = " AND ".join(
                f"p.{parent} = s.{child}"
                for child, parent in zip(fk.columns, fk.parent_columns)
            )
            conditions = [f"NOT EXISTS (SELECT 1 FROM {fk.parent_table} p WHERE {join})"]

            # A self-referencing parent may still be sitting in staging,
            # waiting to be inserted by the very same statement.
            if fk.parent_table == self.spec.table:
                self_join = " AND ".join(
                    f"q.{parent} = s.{child}"
                    for child, parent in zip(fk.columns, fk.parent_columns)
                )
                conditions.append(
                    f"NOT EXISTS (SELECT 1 FROM {self.staging_table} q WHERE {self_join})"
                )

            if fk.nullable:
                null_guard = " AND ".join(f"s.{col} IS NOT NULL" for col in fk.columns)
                conditions.insert(0, f"({null_guard})")

            label = f"missing_{'_'.join(fk.columns)}"
            with self.conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.staging_table} s "
                    f"WHERE {' AND '.join(conditions)} RETURNING *"
                )
                rows = cur.fetchall()
                columns = [d.name for d in cur.description]

            if rows:
                removed[label] = len(rows)
                frame = pd.DataFrame(rows, columns=columns)
                frame["_reject_reason"] = label
                frames.append(frame)
                self.stats.staged -= len(rows)
                logger.warning(
                    "%s: quarantined %d row(s) with no matching %s",
                    self.spec.name, len(rows), fk.parent_table,
                )

        orphans = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        return removed, orphans

    # -- upsert -----------------------------------------------------------

    def _upsert_sql(self) -> str:
        columns = ", ".join(self.spec.columns)
        key = ", ".join(self.spec.conflict_key)

        # A source batch can legitimately contain the same key twice (a
        # restated row). ON CONFLICT DO UPDATE cannot touch the same row twice
        # in one statement, so collapse duplicates first, keeping the last
        # occurrence by physical position.
        select = (
            f"SELECT DISTINCT ON ({key}) {columns} "
            f"FROM {self.staging_table} ORDER BY {key}, ctid DESC"
        )

        if not self.spec.update_columns:
            return (
                f"INSERT INTO {self.spec.table} ({columns}) {select} "
                f"ON CONFLICT ({key}) DO NOTHING"
            )

        assignments = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in self.spec.update_columns
        )
        return (
            f"INSERT INTO {self.spec.table} ({columns}) {select} "
            f"ON CONFLICT ({key}) DO UPDATE SET {assignments}"
        )

    def upsert(self) -> LoadStats:
        """Move staged rows into the target table."""
        started = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.staging_table}")
            staged = cur.fetchone()[0]

            if staged == 0:
                logger.info("%s: nothing staged, skipping upsert", self.spec.name)
                self.stats.seconds += time.perf_counter() - started
                return self.stats

            # Count keys that already exist, to split the upsert's row count
            # into inserts and updates. The obvious `RETURNING xmax = 0` trick
            # is unavailable here: PostgreSQL rejects system columns in
            # RETURNING when the target is partitioned ("cannot retrieve a
            # system column in this context"). This semi-join rides the
            # primary-key index, so it costs far less than a COUNT(*) of the
            # target table before and after.
            join = " AND ".join(
                f"t.{col} = s.{col}" for col in self.spec.conflict_key
            )
            cur.execute(
                f"SELECT count(*) FROM (SELECT DISTINCT "
                f"{', '.join(self.spec.conflict_key)} FROM {self.staging_table}) s "
                f"WHERE EXISTS (SELECT 1 FROM {self.spec.table} t WHERE {join})"
            )
            updated = cur.fetchone()[0]

            cur.execute(self._upsert_sql())
            affected = cur.rowcount

            self.stats.updated += updated
            self.stats.inserted += max(affected - updated, 0)

        self.stats.seconds += time.perf_counter() - started
        return self.stats

    def drop_staging(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {self.staging_table}")


def analyze(conn: psycopg.Connection, tables: list[str]) -> None:
    """Refresh planner statistics after a bulk change.

    Without this the planner works from pre-load statistics and will happily
    pick a nested loop over millions of rows it thinks are hundreds.
    """
    for table in tables:
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {table}")
    logger.info("analyzed %d table(s)", len(tables))


def ensure_partitions_for_range(
    conn: psycopg.Connection,
    parent: str,
    start,
    end,
    bound_suffix: str = " 00:00:00+00",
) -> int:
    """Create any monthly partitions the incoming date range needs."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ensure_monthly_partitions(%s, %s, %s, %s)",
            (parent, start, end, bound_suffix),
        )
        created = cur.fetchone()[0]
    if created:
        logger.info("created %d new partition(s) on %s", created, parent)
    return created


def sync_sequences(conn: psycopg.Connection) -> list[tuple[str, str, int]]:
    """Repair identity sequences after COPYing explicit primary keys.

    Bulk loads write ids straight from the source, which never advances the
    sequence behind an identity column. Left alone, the first row inserted
    afterwards without an explicit id collides with an existing key -- the API
    would start failing on its first write.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM sync_identity_sequences()")
        rows = cur.fetchall()
    logger.info("synced %d identity sequence(s)", len(rows))
    return rows
