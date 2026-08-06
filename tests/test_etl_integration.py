"""End-to-end ETL tests against a live database.

These generate their own tiny fixture set rather than reusing data/sample, so
they stay fast and are unaffected by regeneration of the committed sample.

They truncate the fact and dimension tables, so they mutate shared state. The
schema tests are unaffected because those work inside rolled-back transactions.
"""

from __future__ import annotations

import psycopg
import pytest

from ecommerce_pipeline.config import get_settings
from ecommerce_pipeline.etl import quality
from ecommerce_pipeline.etl.pipeline import run_pipeline

pytestmark = pytest.mark.integration

generate_data = pytest.importorskip("generate_data", reason="datagen extra not installed")

_TABLES = (
    "order_items", "orders", "daily_sales_aggregation", "customer_metrics",
    "products", "customers", "product_categories",
)


def _truncate(conn) -> None:
    conn.execute(f"TRUNCATE {', '.join(_TABLES)} RESTART IDENTITY CASCADE")
    conn.execute("DELETE FROM etl_watermark")


def _scalar(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


@pytest.fixture(scope="module")
def fixture_dir(tmp_path_factory, db_available):
    if not db_available:
        pytest.skip("no PostgreSQL reachable")
    out = tmp_path_factory.mktemp("etl_csv")
    generate_data.main(["--scale", "tiny", "--output", str(out), "--seed", "11"])
    return out


@pytest.fixture(scope="module")
def conn(db_available):
    if not db_available:
        pytest.skip("no PostgreSQL reachable")
    connection = psycopg.connect(get_settings().dsn, autocommit=True)
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def loaded(fixture_dir, conn):
    """A clean full load of the tiny fixture set."""
    _truncate(conn)
    report = run_pipeline(fixture_dir, workflow="pytest_full")
    return report


# --- the happy path --------------------------------------------------------


def test_load_reports_no_rejections(loaded):
    """Generated fixtures satisfy every validation rule and CHECK constraint."""
    assert loaded.rejected == 0


def test_every_source_row_reaches_the_database(loaded, conn):
    scale = generate_data.SCALES["tiny"]
    assert _scalar(conn, "SELECT count(*) FROM product_categories") == scale.categories
    assert _scalar(conn, "SELECT count(*) FROM products") == scale.products
    assert _scalar(conn, "SELECT count(*) FROM customers") == scale.customers
    assert _scalar(conn, "SELECT count(*) FROM orders") == loaded.entities[3].extracted


def test_first_load_is_all_inserts(loaded):
    for entity in loaded.entities:
        assert entity.updated == 0, entity.entity
        assert entity.inserted > 0, entity.entity


def test_quality_checks_all_pass(loaded):
    assert loaded.blocking_failures == []
    passed, warnings, errors = quality.summarise(loaded.quality_results)
    assert errors == 0
    assert passed == len(quality.CHECKS)


def test_run_is_recorded_as_succeeded(loaded, conn):
    status = _scalar(conn, "SELECT status FROM etl_run WHERE run_id = %s", (loaded.run_id,))
    assert status == "succeeded"


def test_quality_results_are_persisted(loaded, conn):
    stored = _scalar(
        conn, "SELECT count(*) FROM data_quality_check WHERE run_id = %s", (loaded.run_id,)
    )
    assert stored == len(quality.CHECKS)


# --- generated column and transformations ----------------------------------


def test_line_revenue_computed_by_the_database(loaded, conn):
    mismatches = _scalar(
        conn,
        "SELECT count(*) FROM order_items "
        "WHERE ABS(line_revenue - (unit_price * quantity - discount)) > 0.001",
    )
    assert mismatches == 0


def test_daily_aggregation_covers_every_selling_day(loaded, conn):
    missing = _scalar(conn, """
        SELECT count(*) FROM (
            SELECT DISTINCT (o.order_date AT TIME ZONE 'UTC')::date AS d
            FROM orders o WHERE is_revenue_bearing(o.status)
            EXCEPT SELECT DISTINCT sale_date FROM daily_sales_aggregation
        ) m
    """)
    assert missing == 0


def test_aggregate_revenue_reconciles_with_line_items(loaded, conn):
    """The rollup must not invent or lose revenue."""
    aggregated = _scalar(conn, "SELECT COALESCE(SUM(net_revenue), 0) FROM daily_sales_aggregation")
    raw = _scalar(conn, """
        SELECT COALESCE(SUM(oi.line_revenue), 0) FROM order_items oi
        JOIN orders o ON o.order_id = oi.order_id AND o.order_date = oi.order_date
        WHERE is_revenue_bearing(o.status)
    """)
    assert abs(float(aggregated) - float(raw)) < 0.02


def test_customer_metrics_exclude_cancelled_orders(loaded, conn):
    ltv = _scalar(conn, "SELECT COALESCE(SUM(lifetime_value), 0) FROM customer_metrics")
    revenue_bearing = _scalar(
        conn,
        "SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE is_revenue_bearing(status)",
    )
    assert abs(float(ltv) - float(revenue_bearing)) < 0.02


def test_materialized_view_is_populated(loaded, conn):
    assert _scalar(conn, "SELECT relispopulated FROM pg_class WHERE oid = 'mv_product_sales_summary'::regclass")
    assert _scalar(conn, "SELECT count(*) FROM mv_product_sales_summary") > 0


# --- idempotency -----------------------------------------------------------


def test_rerun_updates_instead_of_duplicating(loaded, fixture_dir, conn):
    """The whole load is an upsert, so re-running must not change row counts."""
    before = {t: _scalar(conn, f"SELECT count(*) FROM {t}") for t in _TABLES}

    second = run_pipeline(fixture_dir, workflow="pytest_rerun")

    after = {t: _scalar(conn, f"SELECT count(*) FROM {t}") for t in _TABLES}
    assert after == before
    for entity in second.entities:
        assert entity.inserted == 0, f"{entity.entity} inserted rows on re-run"
        assert entity.updated > 0, f"{entity.entity} updated nothing on re-run"


def test_sequences_are_repaired_after_bulk_load(loaded, conn):
    """COPYing explicit ids leaves identity sequences behind; the loader fixes
    them, otherwise the API's first insert collides with an existing key."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO product_categories (name) VALUES ('post-load') RETURNING category_id"
        )
        new_id = cur.fetchone()[0]
        cur.execute("DELETE FROM product_categories WHERE category_id = %s", (new_id,))
    assert new_id > generate_data.SCALES["tiny"].categories


# --- incremental -----------------------------------------------------------


def test_incremental_run_loads_nothing_when_watermark_is_current(loaded, fixture_dir, conn):
    report = run_pipeline(fixture_dir, incremental=True, workflow="pytest_incremental")

    watermarked = {e.entity for e in report.entities
                   if e.entity in {"orders", "order_items", "customers"}}
    for entity in report.entities:
        if entity.entity in watermarked:
            assert entity.inserted == 0 and entity.updated == 0, entity.entity


def test_watermarks_advance_to_the_maximum_loaded_value(loaded, conn):
    stored = _scalar(conn, "SELECT watermark_ts FROM etl_watermark WHERE entity = 'orders'")
    actual_max = _scalar(conn, "SELECT MAX(order_date) FROM orders")
    assert stored == actual_max


# --- referential quarantine ------------------------------------------------


def test_orphan_rows_are_quarantined_rather_than_aborting_the_load(
    fixture_dir, conn, tmp_path
):
    """One order referencing a missing customer must not take the run down.

    The database would reject the whole INSERT; the loader resolves references
    in staging first so only the offending rows are dropped.
    """
    import pandas as pd

    broken = tmp_path / "broken"
    broken.mkdir()
    for name in ("product_categories.csv", "products.csv", "customers.csv",
                 "orders.csv", "order_items.csv"):
        (broken / name).write_bytes((fixture_dir / name).read_bytes())

    orders = pd.read_csv(broken / "orders.csv")
    orders.loc[orders.index[0], "customer_id"] = 999_999   # no such customer
    orders.to_csv(broken / "orders.csv", index=False)

    _truncate(conn)
    report = run_pipeline(broken, workflow="pytest_orphans", reject_dir=tmp_path / "rejects")

    orders_report = next(e for e in report.entities if e.entity == "orders")
    assert orders_report.reject_reasons.get("missing_customer_id") == 1
    # The other orders still loaded.
    assert orders_report.inserted == orders_report.extracted - 1
    # And the quarantined row was written out for replay.
    assert (tmp_path / "rejects" / "orders_rejects.csv").exists()


def test_orphan_line_items_cascade_to_quarantine(fixture_dir, conn, tmp_path):
    """Items belonging to a quarantined order have no parent either."""
    import pandas as pd

    broken = tmp_path / "cascade"
    broken.mkdir()
    for name in ("product_categories.csv", "products.csv", "customers.csv",
                 "orders.csv", "order_items.csv"):
        (broken / name).write_bytes((fixture_dir / name).read_bytes())

    orders = pd.read_csv(broken / "orders.csv")
    dropped_id = int(orders.loc[orders.index[0], "order_id"])
    orders = orders.iloc[1:]
    orders.to_csv(broken / "orders.csv", index=False)

    items = pd.read_csv(broken / "order_items.csv")
    expected_orphans = int((items.order_id == dropped_id).sum())

    _truncate(conn)
    report = run_pipeline(broken, workflow="pytest_cascade")

    items_report = next(e for e in report.entities if e.entity == "order_items")
    assert items_report.reject_reasons.get("missing_order_id_order_date") == expected_orphans


# --- restore ---------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def _restore_sample_data(conn):
    """Reload data/sample on teardown.

    This module truncates shared tables, so without a restore it would leave
    the database holding a few hundred test rows and the API demo would come
    up looking broken. Warns rather than failing if the sample set is not
    mounted -- the tests themselves have already passed at that point.
    """
    yield
    from pathlib import Path

    sample = Path(__file__).resolve().parents[1] / "data" / "sample"
    if not sample.is_dir():
        print(f"\n[warning] {sample} not found; database left holding test fixtures")
        return
    _truncate(conn)
    run_pipeline(sample, workflow="pytest_restore")
