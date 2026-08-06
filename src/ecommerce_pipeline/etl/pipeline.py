"""ETL orchestration and command-line entry point.

    python -m ecommerce_pipeline.etl.pipeline --source data/sample
    python -m ecommerce_pipeline.etl.pipeline --source data/new --incremental

Transaction model: each entity is loaded in its own transaction, so a failure
part-way through leaves earlier entities committed. That is deliberate --
wrapping 20M rows in one transaction would hold a single snapshot open for the
whole load and bloat WAL -- and it is safe because every load is an upsert
keyed on the primary key, so re-running is idempotent. Watermarks only advance
once the entire run succeeds, so a failed run re-reads the same window.

Run bookkeeping uses a separate autocommit connection, so the etl_run row and
its data-quality results survive a rollback of the data transaction and are
still there to explain what went wrong.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

from ..config import get_settings
from ..db import wait_for_database
from ..logging_config import configure_logging, get_logger
from . import extract, quality, transform, validate
from .load import Loader, analyze, ensure_partitions_for_range, sync_sequences
from .spec import ENTITIES, ENTITIES_BY_NAME, EntitySpec

logger = get_logger(__name__)

# Tables that are range-partitioned by a timestamp and therefore need
# partitions to exist before rows can be routed into them.
_PARTITIONED = {"orders", "order_items"}


@dataclass
class EntityReport:
    entity: str
    extracted: int = 0
    rejected: int = 0
    inserted: int = 0
    updated: int = 0
    seconds: float = 0.0
    reject_reasons: dict[str, int] = field(default_factory=dict)
    watermark: datetime | None = None


@dataclass
class RunReport:
    run_id: int | None = None
    entities: list[EntityReport] = field(default_factory=list)
    quality_results: list[quality.CheckResult] = field(default_factory=list)
    transform_stats: transform.TransformStats | None = None
    seconds: float = 0.0

    @property
    def extracted(self) -> int:
        return sum(e.extracted for e in self.entities)

    @property
    def loaded(self) -> int:
        return sum(e.inserted + e.updated for e in self.entities)

    @property
    def rejected(self) -> int:
        return sum(e.rejected for e in self.entities)

    @property
    def blocking_failures(self) -> list[quality.CheckResult]:
        return [r for r in self.quality_results if r.blocking]


# ---------------------------------------------------------------------------
# run bookkeeping
# ---------------------------------------------------------------------------


class RunRecorder:
    """Writes the etl_run row on its own autocommit connection."""

    def __init__(self, workflow: str, source: str) -> None:
        settings = get_settings()
        self.conn = psycopg.connect(settings.dsn, autocommit=True)
        self.run_id: int | None = None
        self._workflow = workflow
        self._source = source

    def start(self) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_run (workflow, source, status) "
                "VALUES (%s, %s, 'running') RETURNING run_id",
                (self._workflow, self._source),
            )
            self.run_id = cur.fetchone()[0]
        logger.info("etl_run %d started (%s)", self.run_id, self._workflow)
        return self.run_id

    def finish(self, report: RunReport, error: str | None = None) -> None:
        if self.run_id is None:
            return
        status = "failed" if error else "succeeded"
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE etl_run SET status = %s, finished_at = NOW(), "
                "rows_extracted = %s, rows_loaded = %s, rows_rejected = %s, "
                "error_message = %s WHERE run_id = %s",
                (status, report.extracted, report.loaded, report.rejected,
                 error, self.run_id),
            )
        logger.info("etl_run %d %s", self.run_id, status)

    def close(self) -> None:
        self.conn.close()


def read_watermark(conn: psycopg.Connection, entity: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute("SELECT watermark_ts FROM etl_watermark WHERE entity = %s", (entity,))
        row = cur.fetchone()
    return row[0] if row else None


def advance_watermark(conn: psycopg.Connection, entity: str,
                      value: datetime, run_id: int | None) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT advance_watermark(%s, %s, %s)", (entity, value, run_id))


# ---------------------------------------------------------------------------
# per-entity load
# ---------------------------------------------------------------------------


def _write_rejects(reject_dir: Path | None, entity: str, frame: pd.DataFrame) -> None:
    """Quarantine rejected rows so they can be inspected and replayed."""
    if reject_dir is None or frame.empty:
        return
    reject_dir.mkdir(parents=True, exist_ok=True)
    path = reject_dir / f"{entity}_rejects.csv"
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)


def load_entity(
    conn: psycopg.Connection,
    spec: EntitySpec,
    source_dir: Path,
    chunk_size: int,
    *,
    incremental: bool = False,
    reject_dir: Path | None = None,
    run_id: int | None = None,
) -> EntityReport:
    """Extract, validate and load a single entity."""
    started = time.perf_counter()
    report = EntityReport(entity=spec.name)

    since = None
    if incremental and spec.watermark_column:
        since = read_watermark(conn, spec.name)
        if since is not None:
            logger.info("%s: incremental from %s", spec.name, since.isoformat())

    loader = Loader(conn=conn, spec=spec)
    loader.create_staging()

    max_watermark: pd.Timestamp | None = None
    mismatched_revenue = 0

    for chunk in extract.read_chunks(spec, source_dir, chunk_size):
        report.extracted += len(chunk)
        result = validate.validate(spec.name, chunk)

        if result.rejected_count:
            report.rejected += result.rejected_count
            for reason, count in result.reasons.items():
                report.reject_reasons[reason] = report.reject_reasons.get(reason, 0) + count
            _write_rejects(reject_dir, spec.name, result.rejected)

        clean = result.clean

        # Cross-check the source's own total against the business rule the
        # database enforces. Reported, not rejected.
        if spec.name == "order_items" and not clean.empty:
            mismatched_revenue += int(validate.revenue_mismatch(clean).sum())

        if spec.watermark_column and not clean.empty:
            column = clean[spec.watermark_column]
            chunk_max = column.max()
            if pd.notna(chunk_max):
                max_watermark = chunk_max if max_watermark is None else max(max_watermark, chunk_max)
            if since is not None:
                clean = clean.loc[column > pd.Timestamp(since)]

        loader.copy_chunk(clean)

    if mismatched_revenue:
        logger.warning(
            "order_items: %d row(s) where the source total disagrees with "
            "price x quantity - discount", mismatched_revenue,
        )

    # Drop rows whose foreign-key parent is absent before the upsert, so a
    # single orphan cannot abort the whole statement.
    orphan_counts, orphan_rows = loader.quarantine_orphans()
    if orphan_counts:
        total_orphans = sum(orphan_counts.values())
        report.rejected += total_orphans
        for reason, count in orphan_counts.items():
            report.reject_reasons[reason] = report.reject_reasons.get(reason, 0) + count
        _write_rejects(reject_dir, spec.name, orphan_rows)

    # Partitions must exist before the upsert routes rows into them.
    if spec.table in _PARTITIONED and loader.stats.staged:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT (MIN({spec.watermark_column}) AT TIME ZONE 'UTC')::date,"
                f"       (MAX({spec.watermark_column}) AT TIME ZONE 'UTC')::date "
                f"FROM {loader.staging_table}"
            )
            low, high = cur.fetchone()
        if low and high:
            ensure_partitions_for_range(conn, spec.table, low, high)

    stats = loader.upsert()
    loader.drop_staging()

    report.inserted = stats.inserted
    report.updated = stats.updated
    report.seconds = time.perf_counter() - started
    report.watermark = max_watermark.to_pydatetime() if max_watermark is not None else None

    logger.info(
        "%s: extracted=%d rejected=%d inserted=%d updated=%d in %.1fs (%s rows/s)",
        spec.name, report.extracted, report.rejected, report.inserted,
        report.updated, report.seconds,
        f"{report.extracted / report.seconds:,.0f}" if report.seconds else "n/a",
    )
    return report


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------


def run_pipeline(
    source_dir: Path,
    *,
    entities: list[str] | None = None,
    chunk_size: int | None = None,
    incremental: bool = False,
    skip_transform: bool = False,
    skip_quality: bool = False,
    reject_dir: Path | None = None,
    workflow: str = "csv_to_postgres",
) -> RunReport:
    """Run the full pipeline and return a report of what happened."""
    settings = get_settings()
    chunk_size = chunk_size or settings.etl_batch_size

    selected = [s for s in ENTITIES if entities is None or s.name in entities]
    if not selected:
        raise ValueError(f"no known entities selected: {entities}")

    wait_for_database(settings)

    recorder = RunRecorder(workflow=workflow, source=str(source_dir))
    report = RunReport()
    started = time.perf_counter()
    error: str | None = None

    try:
        report.run_id = recorder.start()

        with psycopg.connect(settings.dsn) as conn:
            # -- load ------------------------------------------------------
            for spec in selected:
                entity_report = load_entity(
                    conn, spec, source_dir, chunk_size,
                    incremental=incremental, reject_dir=reject_dir,
                    run_id=report.run_id,
                )
                report.entities.append(entity_report)
                # One transaction per entity: bounded WAL, and safe to redo
                # because every load is an idempotent upsert.
                conn.commit()

            # Explicit ids came straight from the source, so the identity
            # sequences are still at their starting values.
            sync_sequences(conn)
            analyze(conn, [s.table for s in selected])
            conn.commit()

            # -- transform -------------------------------------------------
            if not skip_transform:
                date_range = _transform_range(conn, report, incremental)
                if date_range:
                    low, high = date_range
                    report.transform_stats = transform.run_transformations(
                        conn, low, high, scope_customers=incremental
                    )
                    conn.commit()
                else:
                    logger.info("no orders present; skipping transformations")

            # -- quality ---------------------------------------------------
            if not skip_quality:
                logger.info("running %d data-quality check(s)", len(quality.CHECKS))
                report.quality_results = quality.run_checks(conn, run_id=report.run_id)
                conn.commit()

            # -- watermarks ------------------------------------------------
            # Advanced only after everything above succeeded, so a failed run
            # re-reads the same window rather than skipping it.
            for entity_report in report.entities:
                if entity_report.watermark is not None:
                    advance_watermark(conn, entity_report.entity,
                                      entity_report.watermark, report.run_id)
            conn.commit()

        if not skip_transform:
            transform.refresh_materialized_views()

    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        logger.exception("pipeline failed")
        raise
    finally:
        report.seconds = time.perf_counter() - started
        recorder.finish(report, error=error)
        recorder.close()

    return report


def _transform_range(conn: psycopg.Connection, report: RunReport,
                     incremental: bool) -> tuple[date, date] | None:
    """Date span the transformations should cover.

    An incremental run only needs to rebuild the days it touched; a full run
    rebuilds everything present.
    """
    if incremental:
        dates = [e.watermark for e in report.entities
                 if e.entity == "orders" and e.watermark is not None]
        if dates:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT (MIN(order_date) AT TIME ZONE 'UTC')::date,"
                    "       (MAX(order_date) AT TIME ZONE 'UTC')::date "
                    "FROM orders WHERE order_date <= %s", (max(dates),)
                )
                low, high = cur.fetchone()
            if low and high:
                return low, high
    return transform.loaded_date_range(conn)


def print_summary(report: RunReport) -> None:
    """Human-readable end-of-run summary."""
    print("\n" + "=" * 72)
    print(f"ETL run {report.run_id} finished in {report.seconds:.1f}s")
    print("=" * 72)
    print(f"{'entity':<20} {'extracted':>10} {'rejected':>9} {'inserted':>9} "
          f"{'updated':>8} {'seconds':>8}")
    print("-" * 72)
    for entity in report.entities:
        print(f"{entity.entity:<20} {entity.extracted:>10,} {entity.rejected:>9,} "
              f"{entity.inserted:>9,} {entity.updated:>8,} {entity.seconds:>8.1f}")
    print("-" * 72)
    print(f"{'total':<20} {report.extracted:>10,} {report.rejected:>9,} "
          f"{report.loaded:>9,}")

    reasons = {}
    for entity in report.entities:
        for reason, count in entity.reject_reasons.items():
            reasons[f"{entity.entity}.{reason}"] = count
    if reasons:
        print("\nreject reasons:")
        for reason, count in sorted(reasons.items(), key=lambda kv: -kv[1]):
            print(f"  {reason:<48} {count:>8,}")

    if report.transform_stats:
        print(f"\ntransformations: {report.transform_stats.daily_sales_rows:,} daily "
              f"rollup rows, {report.transform_stats.customer_metric_rows:,} customer "
              f"metric rows ({report.transform_stats.seconds:.1f}s)")

    if report.quality_results:
        passed, warnings, errors = quality.summarise(report.quality_results)
        print(f"\ndata quality: {passed} passed, {warnings} warning(s), {errors} error(s)")
        for result in report.quality_results:
            if not result.passed:
                print(f"  [{result.check.severity.upper()}] {result.check.name}: "
                      f"observed {result.observed:g} (threshold {result.check.threshold:g})")
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load e-commerce CSVs into PostgreSQL")
    parser.add_argument("--source", type=Path, default=None,
                        help="directory containing the CSV files")
    parser.add_argument("--entities", nargs="*", default=None,
                        choices=sorted(ENTITIES_BY_NAME),
                        help="load only these entities (default: all)")
    parser.add_argument("--chunk-size", type=int, default=None,
                        help="rows per COPY batch")
    parser.add_argument("--incremental", action="store_true",
                        help="only load rows past the stored watermark")
    parser.add_argument("--skip-transform", action="store_true")
    parser.add_argument("--skip-quality", action="store_true")
    parser.add_argument("--reject-dir", type=Path, default=None,
                        help="write quarantined rows here")
    parser.add_argument("--workflow", default="csv_to_postgres",
                        help="label recorded on the etl_run row")
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args(argv)

    configure_logging(args.log_level, force=args.log_level is not None)
    settings = get_settings()
    source_dir = args.source or settings.data_dir

    try:
        report = run_pipeline(
            source_dir,
            entities=args.entities,
            chunk_size=args.chunk_size,
            incremental=args.incremental,
            skip_transform=args.skip_transform,
            skip_quality=args.skip_quality,
            reject_dir=args.reject_dir,
            workflow=args.workflow,
        )
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 2
    except Exception:
        return 1

    print_summary(report)

    if report.blocking_failures:
        logger.error("%d blocking data-quality failure(s)", len(report.blocking_failures))
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
