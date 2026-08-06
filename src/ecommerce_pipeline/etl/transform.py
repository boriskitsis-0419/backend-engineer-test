"""Post-load transformations.

These implement the transformation rules that operate on data already in the
database. Doing the aggregation in SQL rather than pulling rows back into
pandas keeps the work next to the data: the daily rollup over 20M line items
is a single grouped scan the planner can parallelise, where the equivalent
round trip would move gigabytes over the wire to compute the same numbers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date

import psycopg

from ..config import get_settings
from ..logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class TransformStats:
    daily_sales_rows: int = 0
    customer_metric_rows: int = 0
    seconds: float = 0.0


def loaded_date_range(conn: psycopg.Connection, run_id: int | None = None
                      ) -> tuple[date, date] | None:
    """Span of order dates currently present, used to scope a full refresh."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (MIN(order_date) AT TIME ZONE 'UTC')::date,"
            "       (MAX(order_date) AT TIME ZONE 'UTC')::date FROM orders"
        )
        low, high = cur.fetchone()
    if low is None or high is None:
        return None
    return low, high


def refresh_daily_sales(conn: psycopg.Connection, start: date, end: date) -> int:
    """Rebuild daily_sales_aggregation for [start, end] (transformation rule 4)."""
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("SELECT refresh_daily_sales(%s, %s)", (start, end))
        rows = cur.fetchone()[0]
    logger.info(
        "daily sales aggregation: %d row(s) for %s..%s in %.1fs",
        rows, start, end, time.perf_counter() - started,
    )
    return rows


def refresh_customer_metrics(conn: psycopg.Connection,
                             customer_ids: list[int] | None = None) -> int:
    """Recompute lifetime value and cadence (transformation rule 3).

    Passing the customers touched by this batch keeps an incremental run
    proportional to the batch rather than to the customer table.
    """
    started = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute("SELECT refresh_customer_metrics(%s)", (customer_ids,))
        rows = cur.fetchone()[0]
    scope = "all customers" if customer_ids is None else f"{len(customer_ids)} customer(s)"
    logger.info(
        "customer metrics: %d row(s) for %s in %.1fs",
        rows, scope, time.perf_counter() - started,
    )
    return rows


def customers_in_date_range(conn: psycopg.Connection, start: date, end: date) -> list[int]:
    """Customers with an order in the range, for a scoped metrics refresh."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT customer_id FROM orders "
            "WHERE order_date >= %s AND order_date < (%s::date + 1)",
            (start, end),
        )
        return [row[0] for row in cur.fetchall()]


def refresh_materialized_views(concurrently: bool = True) -> None:
    """Refresh the analytics materialized views.

    Uses its own autocommit connection: REFRESH MATERIALIZED VIEW CONCURRENTLY
    cannot run inside a transaction block, which is also why it cannot live in
    a plpgsql function. CONCURRENTLY lets the API keep reading the previous
    contents instead of blocking on an AccessExclusiveLock for the duration.
    """
    settings = get_settings()
    started = time.perf_counter()
    views = ("mv_product_sales_summary",)

    with psycopg.connect(settings.dsn, autocommit=True) as conn:
        for view in views:
            # CONCURRENTLY requires the view to have been populated at least
            # once; the views are created WITH NO DATA, so the first refresh
            # of a fresh database must be non-concurrent. Checking
            # relispopulated is deterministic, where catching the resulting
            # error means guessing which SQLSTATE the server raises.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT relispopulated FROM pg_class WHERE oid = %s::regclass",
                    (view,),
                )
                populated = cur.fetchone()[0]

            keyword = "CONCURRENTLY " if (concurrently and populated) else ""
            if not populated:
                logger.info("%s is unpopulated; first refresh cannot be concurrent", view)
            conn.execute(f"REFRESH MATERIALIZED VIEW {keyword}{view}")

    logger.info("materialized views refreshed in %.1fs", time.perf_counter() - started)


def run_transformations(
    conn: psycopg.Connection,
    start: date,
    end: date,
    scope_customers: bool = True,
) -> TransformStats:
    """Run every post-load transformation for a loaded date range."""
    started = time.perf_counter()
    stats = TransformStats()

    stats.daily_sales_rows = refresh_daily_sales(conn, start, end)

    customer_ids = customers_in_date_range(conn, start, end) if scope_customers else None
    stats.customer_metric_rows = refresh_customer_metrics(conn, customer_ids)

    stats.seconds = time.perf_counter() - started
    return stats
