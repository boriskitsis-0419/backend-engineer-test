"""The pipeline stages as individually callable units.

The Flyte tasks in ``workflows.py`` are thin wrappers around these, so the
orchestration layer holds no business logic and the stages stay testable
without flytekit installed.

Each stage takes an explicit ``run_id`` so that a workflow split across several
tasks still records everything against one row in ``etl_run`` -- otherwise each
retryable task would open its own run and the history would be unreadable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg

from ..config import get_settings
from ..db import connect, wait_for_database
from ..etl import quality, transform
from ..etl.pipeline import RunRecorder, run_pipeline
from ..logging_config import get_logger
from ..migrations.runner import migrate_up

logger = get_logger(__name__)


@dataclass
class LoadSummary:
    run_id: int
    extracted: int = 0
    loaded: int = 0
    rejected: int = 0
    seconds: float = 0.0
    entities: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)


@dataclass
class TransformSummary:
    daily_sales_rows: int = 0
    customer_metric_rows: int = 0
    date_from: str = ""
    date_to: str = ""
    seconds: float = 0.0


@dataclass
class QualityVerdict:
    passed: int = 0
    warnings: int = 0
    errors: int = 0
    failed: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.errors == 0


class DataQualityError(RuntimeError):
    """Raised when an error-severity check fails, to fail the workflow."""


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------


def apply_migrations() -> int:
    """Bring the schema up to date. Idempotent, so it is safe on every run."""
    applied = migrate_up()
    logger.info("schema check complete: %d migration(s) applied", len(applied))
    return len(applied)


def verify_source(source: str) -> int:
    """Fail fast if the input is missing or malformed.

    Checking headers before a multi-hour load turns a late, confusing failure
    into an immediate, obvious one.
    """
    from ..etl import extract
    from ..etl.spec import ENTITIES

    source_dir = Path(source)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"source directory not found: {source_dir}")

    total = 0
    for spec in ENTITIES:
        path = extract.source_path(spec, source_dir)
        extract.validate_header(spec, path)
        rows = extract.count_rows(spec, source_dir)
        total += rows
        logger.info("  %-20s %10d row(s)", spec.name, rows)

    logger.info("source verified: %d row(s) across %d file(s)", total, len(ENTITIES))
    return total


def load(source: str, incremental: bool, reject_dir: str | None = None) -> LoadSummary:
    """Extract, validate and load. Transformations and checks run separately.

    Splitting them means a retry of a failed check does not re-COPY 20M rows,
    and a failure is attributable to a specific stage.
    """
    report = run_pipeline(
        Path(source),
        incremental=incremental,
        skip_transform=True,
        skip_quality=True,
        reject_dir=Path(reject_dir) if reject_dir else None,
        workflow="flyte_etl",
    )

    reasons: dict[str, int] = {}
    for entity in report.entities:
        for reason, count in entity.reject_reasons.items():
            reasons[f"{entity.entity}.{reason}"] = count

    return LoadSummary(
        run_id=report.run_id or 0,
        extracted=report.extracted,
        loaded=report.loaded,
        rejected=report.rejected,
        seconds=report.seconds,
        entities=len(report.entities),
        reject_reasons=reasons,
    )


def transform_loaded(run_id: int, incremental: bool) -> TransformSummary:
    """Rebuild the aggregates for whatever the load covered."""
    settings = get_settings()
    wait_for_database(settings)

    with connect() as conn:
        window = _refresh_window(conn, run_id, incremental)
        if window is None:
            logger.info("no orders present; nothing to transform")
            return TransformSummary()

        low, high = window
        stats = transform.run_transformations(conn, low, high, scope_customers=incremental)

    transform.refresh_materialized_views()

    return TransformSummary(
        daily_sales_rows=stats.daily_sales_rows,
        customer_metric_rows=stats.customer_metric_rows,
        date_from=low.isoformat(),
        date_to=high.isoformat(),
        seconds=stats.seconds,
    )


def _refresh_window(conn: psycopg.Connection, run_id: int,
                    incremental: bool) -> tuple[date, date] | None:
    """Date span the aggregates need rebuilding for.

    An incremental run only touched days at or after its previous watermark,
    so rebuilding the whole history every night would make the incremental
    load pointless.
    """
    if incremental:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT (watermark_ts AT TIME ZONE 'UTC')::date "
                "FROM etl_watermark WHERE entity = 'orders'"
            )
            row = cur.fetchone()
        if row and row[0]:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT (MAX(order_date) AT TIME ZONE 'UTC')::date FROM orders"
                )
                high = cur.fetchone()[0]
            if high:
                return row[0], high

    return transform.loaded_date_range(conn)


def check_quality(run_id: int, fail_on_error: bool = True) -> QualityVerdict:
    """Run the checks and, by default, fail the workflow on an error."""
    settings = get_settings()
    wait_for_database(settings)

    with connect() as conn:
        results = quality.run_checks(conn, run_id=run_id or None)

    passed, warnings, errors = quality.summarise(results)
    verdict = QualityVerdict(
        passed=passed,
        warnings=warnings,
        errors=errors,
        failed=[r.check.name for r in results if not r.passed],
    )

    if errors and fail_on_error:
        blocking = [r.check.name for r in results if r.blocking]
        raise DataQualityError(
            f"{errors} blocking data-quality failure(s): {', '.join(blocking)}"
        )
    return verdict


def finalise_run(run_id: int, load_summary: LoadSummary,
                 verdict: QualityVerdict) -> str:
    """Close out the etl_run row and emit the line an operator would page on."""
    if not run_id:
        return "no run recorded"

    settings = get_settings()
    with psycopg.connect(settings.dsn, autocommit=True) as conn:
        # Only reached when every prior stage passed, so it is safe to assert
        # success -- but never resurrect a run already marked failed.
        conn.execute(
            "UPDATE etl_run SET status = 'succeeded', finished_at = NOW(), "
            "rows_extracted = %s, rows_loaded = %s, rows_rejected = %s "
            "WHERE run_id = %s AND status <> 'failed'",
            (load_summary.extracted, load_summary.loaded, load_summary.rejected, run_id),
        )

    summary = (
        f"run {run_id}: extracted={load_summary.extracted} "
        f"loaded={load_summary.loaded} rejected={load_summary.rejected} "
        f"checks_passed={verdict.passed} warnings={verdict.warnings}"
    )
    logger.info("%s", summary)
    return summary


def mark_run_failed(run_id: int, message: str) -> None:
    """Record a failure against the run so the history explains itself.

    Deliberately not restricted to rows still marked 'running'. The load stage
    closes its own etl_run row as succeeded when the COPY finishes, which is
    true of the load but not of the workflow: a later quality-gate failure
    must be able to overwrite that verdict. Without this, a run that failed
    its gate still reads as 'succeeded' in the history, which is the one thing
    an operator must be able to trust.
    """
    if not run_id:
        return
    settings = get_settings()
    try:
        with psycopg.connect(settings.dsn, autocommit=True) as conn:
            conn.execute(
                "UPDATE etl_run SET status = 'failed', finished_at = NOW(), "
                "error_message = %s WHERE run_id = %s",
                (message[:2000], run_id),
            )
    except Exception:
        logger.exception("could not record failure for run %d", run_id)


__all__ = [
    "DataQualityError",
    "LoadSummary",
    "QualityVerdict",
    "RunRecorder",
    "TransformSummary",
    "apply_migrations",
    "check_quality",
    "finalise_run",
    "load",
    "mark_run_failed",
    "transform_loaded",
    "verify_source",
]
