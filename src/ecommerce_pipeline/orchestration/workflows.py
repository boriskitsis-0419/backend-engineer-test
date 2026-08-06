"""Flyte workflow orchestrating the ETL.

    schema -> verify source -> load -> transform -> quality gate -> finalise

Run it locally (no cluster needed):

    python -m ecommerce_pipeline.orchestration.workflows --source data/sample

Register it against a cluster:

    pyflyte register --project ecommerce --domain development \
        src/ecommerce_pipeline/orchestration/workflows.py

The tasks are deliberately thin: all the logic lives in ``steps.py``, which is
tested directly, so the workflow contributes orchestration and nothing else.

Why the stages are separate tasks rather than one:

* **Attributable failure.** "The load succeeded and the quality gate failed" is
  a different incident from "the load failed", and Flyte's UI shows which.
* **Cheap retries.** Retrying a failed check re-runs seconds of SQL. If it were
  one task, it would re-COPY 20M rows.
* **Right-sized resources.** The load is memory-hungry; the checks are not.
  Separate tasks can carry separate resource requests.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import timedelta

from ..config import get_settings
from ..logging_config import configure_logging, get_logger
from . import steps
from .steps import LoadSummary, QualityVerdict, TransformSummary

logger = get_logger(__name__)

try:
    from flytekit import Resources, task, workflow

    FLYTEKIT_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only without the extra
    FLYTEKIT_AVAILABLE = False

    # Allow the module to be imported, and the pipeline to be driven, without
    # flytekit installed. The decorators degrade to pass-throughs so the same
    # functions remain callable directly.
    def task(*args, **kwargs):  # type: ignore[misc]
        def decorate(fn):
            return fn
        return decorate(args[0]) if args and callable(args[0]) else decorate

    def workflow(*args, **kwargs):  # type: ignore[misc]
        def decorate(fn):
            return fn
        return decorate(args[0]) if args and callable(args[0]) else decorate

    class Resources:  # type: ignore[no-redef]
        def __init__(self, **kwargs):
            pass


# Applied to the tasks that touch the database. Retries cover transient
# failures -- a connection dropped during a failover, a deadlock -- and are
# safe because every stage is idempotent: the load upserts on the primary key
# and the aggregate refreshes delete-and-rebuild their window.
_DB_RETRIES = 2


@task(
    retries=_DB_RETRIES,
    timeout=timedelta(minutes=15),
    requests=Resources(cpu="1", mem="1Gi"),
)
def ensure_schema() -> int:
    """Apply any pending migrations. Idempotent, so it runs every time."""
    return steps.apply_migrations()


@task(
    retries=1,
    timeout=timedelta(minutes=30),
    requests=Resources(cpu="1", mem="1Gi"),
)
def verify_source(source: str, schema_version: int) -> int:
    """Check every input file exists and has the expected header.

    ``schema_version`` is unused but declared so Flyte orders this after
    ensure_schema; without a data dependency the two would run concurrently.
    """
    _ = schema_version
    return steps.verify_source(source)


@task(
    retries=_DB_RETRIES,
    timeout=timedelta(hours=6),
    # The loader holds a chunk of a CSV plus its staging buffer in memory, so
    # this is the memory-hungry stage.
    requests=Resources(cpu="2", mem="4Gi"),
)
def load_data(source: str, incremental: bool, row_count: int,
              reject_dir: str) -> LoadSummary:
    _ = row_count
    return steps.load(source, incremental, reject_dir or None)


@task(
    retries=_DB_RETRIES,
    timeout=timedelta(hours=2),
    requests=Resources(cpu="2", mem="2Gi"),
)
def transform_data(load_summary: LoadSummary, incremental: bool) -> TransformSummary:
    return steps.transform_loaded(load_summary.run_id, incremental)


@task(
    retries=1,
    timeout=timedelta(minutes=30),
    requests=Resources(cpu="1", mem="1Gi"),
)
def quality_gate(load_summary: LoadSummary, transform_summary: TransformSummary,
                 fail_on_error: bool) -> QualityVerdict:
    """Run the checks; raise on an error-severity failure to fail the workflow.

    Placed after the transformations because several checks assert on the
    aggregates -- for example, that every day carrying revenue-bearing orders
    also has rows in the daily rollup.
    """
    _ = transform_summary
    try:
        return steps.check_quality(load_summary.run_id, fail_on_error=fail_on_error)
    except steps.DataQualityError as exc:
        steps.mark_run_failed(load_summary.run_id, str(exc))
        raise


@task(retries=1, timeout=timedelta(minutes=10))
def finalise(load_summary: LoadSummary, verdict: QualityVerdict) -> str:
    """Close the run and emit the summary line monitoring keys off."""
    return steps.finalise_run(load_summary.run_id, load_summary, verdict)


@workflow
def ecommerce_etl(
    source: str = "/app/data/sample",
    incremental: bool = True,
    fail_on_quality_error: bool = True,
    reject_dir: str = "",
) -> str:
    """Load the CSVs, rebuild the aggregates and gate on data quality.

    Args:
        source: directory holding the CSV files.
        incremental: only ingest rows past the stored watermark.
        fail_on_quality_error: fail the run when an error-severity check fails.
        reject_dir: where to quarantine rejected rows; empty disables it.
    """
    schema_version = ensure_schema()
    row_count = verify_source(source=source, schema_version=schema_version)

    load_summary = load_data(
        source=source, incremental=incremental,
        row_count=row_count, reject_dir=reject_dir,
    )
    transform_summary = transform_data(
        load_summary=load_summary, incremental=incremental
    )
    verdict = quality_gate(
        load_summary=load_summary, transform_summary=transform_summary,
        fail_on_error=fail_on_quality_error,
    )
    return finalise(load_summary=load_summary, verdict=verdict)


@workflow
def ecommerce_backfill(source: str = "/app/data/full") -> str:
    """Full, non-incremental reload.

    Kept as its own entry point so a backfill cannot be triggered by flipping a
    parameter on the nightly schedule by accident.
    """
    return ecommerce_etl(
        source=source, incremental=False, fail_on_quality_error=True, reject_dir="",
    )


def main(argv: list[str] | None = None) -> int:
    """Run the workflow locally, without a Flyte cluster."""
    parser = argparse.ArgumentParser(description="Run the ETL workflow locally")
    parser.add_argument("--source", default=None)
    parser.add_argument("--incremental", action="store_true")
    parser.add_argument("--reject-dir", default="")
    parser.add_argument(
        "--no-fail-on-quality-error", dest="fail_on_quality", action="store_false"
    )
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args(argv)

    configure_logging(args.log_level, force=args.log_level is not None)
    source = args.source or str(get_settings().data_dir)

    if not FLYTEKIT_AVAILABLE:
        logger.warning(
            "flytekit is not installed; running the stages directly. "
            "Install it with: pip install -e '.[flyte]'"
        )

    logging.getLogger("flytekit").setLevel(logging.WARNING)

    try:
        result = ecommerce_etl(
            source=source,
            incremental=args.incremental,
            fail_on_quality_error=args.fail_on_quality,
            reject_dir=args.reject_dir,
        )
    except steps.DataQualityError as exc:
        logger.error("workflow failed the quality gate: %s", exc)
        return 3
    except Exception:
        logger.exception("workflow failed")
        return 1

    print(f"\n{result}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
