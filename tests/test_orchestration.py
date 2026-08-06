"""Tests for the orchestration layer.

The Flyte tasks are thin wrappers, so these exercise the stages in steps.py
directly, plus the workflow's end-to-end behaviour when run locally.
"""

from __future__ import annotations

import psycopg
import pytest

from ecommerce_pipeline.config import get_settings
from ecommerce_pipeline.orchestration import steps
from ecommerce_pipeline.orchestration.workflows import ecommerce_etl

pytestmark = pytest.mark.integration

generate_data = pytest.importorskip("generate_data", reason="datagen extra not installed")

_TABLES = (
    "order_items", "orders", "daily_sales_aggregation", "customer_metrics",
    "products", "customers", "product_categories",
)


def _scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


@pytest.fixture(scope="module")
def conn(db_available):
    if not db_available:
        pytest.skip("no PostgreSQL reachable")
    connection = psycopg.connect(get_settings().dsn, autocommit=True)
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory, db_available):
    if not db_available:
        pytest.skip("no PostgreSQL reachable")
    out = tmp_path_factory.mktemp("wf_csv")
    generate_data.main(["--scale", "tiny", "--output", str(out), "--seed", "23"])
    return out


@pytest.fixture(scope="module", autouse=True)
def _restore_sample_data(conn):
    """Reload data/sample afterwards; this module truncates shared tables."""
    yield
    from pathlib import Path

    from ecommerce_pipeline.etl.pipeline import run_pipeline

    sample = Path(__file__).resolve().parents[1] / "data" / "sample"
    if sample.is_dir():
        conn.execute(f"TRUNCATE {', '.join(_TABLES)} RESTART IDENTITY CASCADE")
        conn.execute("DELETE FROM etl_watermark")
        run_pipeline(sample, workflow="pytest_restore")


# --- individual stages -----------------------------------------------------


def test_apply_migrations_is_idempotent(conn):
    """Runs on every execution, so a no-op must stay a no-op."""
    assert steps.apply_migrations() == 0


def test_verify_source_counts_rows(fixture_dir):
    total = steps.verify_source(str(fixture_dir))
    scale = generate_data.SCALES["tiny"]
    assert total > scale.customers + scale.products


def test_verify_source_rejects_a_missing_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        steps.verify_source(str(tmp_path / "nope"))


def test_verify_source_rejects_a_malformed_header(fixture_dir, tmp_path):
    """Catching a bad header before the load turns a late failure into an
    immediate one."""
    broken = tmp_path / "broken"
    broken.mkdir()
    for path in fixture_dir.glob("*.csv"):
        (broken / path.name).write_bytes(path.read_bytes())

    target = broken / "orders.csv"
    lines = target.read_text().splitlines()
    lines[0] = lines[0].replace("order_date", "date_of_order")
    target.write_text("\n".join(lines))

    with pytest.raises(Exception, match="missing column"):
        steps.verify_source(str(broken))


# --- the workflow end to end -----------------------------------------------


@pytest.fixture(scope="module")
def workflow_run(fixture_dir, conn):
    conn.execute(f"TRUNCATE {', '.join(_TABLES)} RESTART IDENTITY CASCADE")
    conn.execute("DELETE FROM etl_watermark")
    result = ecommerce_etl(
        source=str(fixture_dir), incremental=False,
        fail_on_quality_error=True, reject_dir="",
    )
    return result


def test_workflow_completes_and_summarises(workflow_run):
    assert "extracted=" in workflow_run
    assert "checks_passed=" in workflow_run


def test_workflow_loads_the_data(workflow_run, conn):
    scale = generate_data.SCALES["tiny"]
    assert _scalar(conn, "SELECT count(*) FROM customers") == scale.customers
    assert _scalar(conn, "SELECT count(*) FROM orders") > 0
    assert _scalar(conn, "SELECT count(*) FROM daily_sales_aggregation") > 0


def test_workflow_records_a_succeeded_run(workflow_run, conn):
    status = _scalar(
        conn, "SELECT status FROM etl_run WHERE workflow = 'flyte_etl' "
              "ORDER BY run_id DESC LIMIT 1"
    )
    assert status == "succeeded"


def test_incremental_workflow_reloads_nothing(workflow_run, fixture_dir, conn):
    before = _scalar(conn, "SELECT count(*) FROM orders")
    ecommerce_etl(
        source=str(fixture_dir), incremental=True,
        fail_on_quality_error=True, reject_dir="",
    )
    assert _scalar(conn, "SELECT count(*) FROM orders") == before


# --- the quality gate ------------------------------------------------------


def test_quality_gate_fails_the_workflow_and_records_it(workflow_run, fixture_dir, conn):
    """A row that misses every declared partition is an error-severity
    failure: the data is not lost, but it is neither pruned nor aligned."""
    conn.execute("""
        INSERT INTO orders (customer_id, order_date, status, payment_method,
                            shipping_address, shipping_city, shipping_state,
                            shipping_zip, shipping_country, total_amount)
        SELECT customer_id, '2045-01-01 00:00:00+00', 'Delivered', 'PayPal',
               '1 St', 'T', 'CA', '90210', 'US', 10.00
        FROM customers LIMIT 1
    """)
    assert _scalar(conn, "SELECT COALESCE(SUM(row_count),0) FROM count_default_partition_rows()") == 1

    try:
        with pytest.raises(Exception) as exc:
            ecommerce_etl(
                source=str(fixture_dir), incremental=True,
                fail_on_quality_error=True, reject_dir="",
            )
        assert "default_partition_rows" in str(exc.value)

        # The run must read as failed. The load stage closes its own etl_run
        # row as succeeded, so a later gate failure has to overwrite it --
        # otherwise the history reports success for a run that failed.
        row = _scalar(
            conn, "SELECT status FROM etl_run WHERE workflow = 'flyte_etl' "
                  "ORDER BY run_id DESC LIMIT 1"
        )
        assert row == "failed"

        error = _scalar(
            conn, "SELECT error_message FROM etl_run WHERE workflow = 'flyte_etl' "
                  "ORDER BY run_id DESC LIMIT 1"
        )
        assert "default_partition_rows" in error
    finally:
        conn.execute("DELETE FROM orders WHERE order_date >= '2040-01-01'")


def test_quality_gate_can_be_made_non_blocking(workflow_run, fixture_dir, conn):
    """Useful for a backfill where a known issue is being tolerated."""
    conn.execute("""
        INSERT INTO orders (customer_id, order_date, status, payment_method,
                            shipping_address, shipping_city, shipping_state,
                            shipping_zip, shipping_country, total_amount)
        SELECT customer_id, '2045-01-01 00:00:00+00', 'Delivered', 'PayPal',
               '1 St', 'T', 'CA', '90210', 'US', 10.00
        FROM customers LIMIT 1
    """)
    try:
        result = ecommerce_etl(
            source=str(fixture_dir), incremental=True,
            fail_on_quality_error=False, reject_dir="",
        )
        assert "extracted=" in result
    finally:
        conn.execute("DELETE FROM orders WHERE order_date >= '2040-01-01'")


def test_check_quality_reports_without_raising_when_not_blocking(conn):
    verdict = steps.check_quality(run_id=0, fail_on_error=False)
    assert verdict.passed + verdict.warnings + verdict.errors > 0
    assert isinstance(verdict.ok, bool)


# --- failure bookkeeping ---------------------------------------------------


def test_mark_run_failed_overwrites_a_succeeded_verdict(conn):
    """The load stage closes its run as succeeded before the gate runs."""
    run_id = _scalar(conn, """
        INSERT INTO etl_run (workflow, status, finished_at)
        VALUES ('pytest_marker', 'succeeded', NOW()) RETURNING run_id
    """)
    try:
        steps.mark_run_failed(run_id, "gate failed after load succeeded")
        assert _scalar(conn, "SELECT status FROM etl_run WHERE run_id = %s", (run_id,)) == "failed"
    finally:
        conn.execute("DELETE FROM etl_run WHERE run_id = %s", (run_id,))


def test_finalise_never_resurrects_a_failed_run(conn):
    run_id = _scalar(conn, """
        INSERT INTO etl_run (workflow, status, finished_at, error_message)
        VALUES ('pytest_marker', 'failed', NOW(), 'boom') RETURNING run_id
    """)
    try:
        steps.finalise_run(run_id, steps.LoadSummary(run_id=run_id), steps.QualityVerdict())
        assert _scalar(conn, "SELECT status FROM etl_run WHERE run_id = %s", (run_id,)) == "failed"
    finally:
        conn.execute("DELETE FROM etl_run WHERE run_id = %s", (run_id,))
