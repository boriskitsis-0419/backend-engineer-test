"""Data-quality checks.

Each check is a named SQL probe with a threshold. Results are written to
data_quality_check so a run's verdict is queryable afterwards rather than
living only in logs, and so trends across runs are visible.

Severity decides what a failure means:

* ``error``   -- the load produced something structurally wrong. The pipeline
                 exits non-zero and the Flyte task fails.
* ``warning`` -- worth investigating but not worth blocking a deployment.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import psycopg

from ..logging_config import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    sql: str
    threshold: float
    severity: str = "error"
    description: str = ""

    def evaluate(self, observed: float) -> bool:
        """Checks are written so that the observed value must not exceed the threshold."""
        return observed <= self.threshold


@dataclass
class CheckResult:
    check: Check
    observed: float
    passed: bool

    @property
    def blocking(self) -> bool:
        return not self.passed and self.check.severity == "error"


# Every check counts something that should be zero (or near zero), so the
# comparison is uniformly "observed <= threshold".
CHECKS: tuple[Check, ...] = (
    Check(
        name="orphan_order_items",
        sql="""
            SELECT count(*) FROM order_items oi
            LEFT JOIN orders o
              ON o.order_id = oi.order_id AND o.order_date = oi.order_date
            WHERE o.order_id IS NULL
        """,
        threshold=0,
        description="Line items whose parent order is missing.",
    ),
    Check(
        name="orders_without_items",
        sql="""
            SELECT count(*) FROM orders o
            WHERE NOT EXISTS (
                SELECT 1 FROM order_items oi
                WHERE oi.order_id = o.order_id AND oi.order_date = o.order_date
            )
        """,
        threshold=0,
        severity="warning",
        description="Orders carrying no line items.",
    ),
    Check(
        name="default_partition_rows",
        sql="SELECT COALESCE(SUM(row_count), 0) FROM count_default_partition_rows()",
        threshold=0,
        description=(
            "Rows that missed every declared range partition. They are not "
            "lost, but they are neither pruned nor aligned with their parent."
        ),
    ),
    Check(
        name="order_total_mismatch",
        sql="""
            SELECT count(*) FROM (
                SELECT o.order_id, o.total_amount, SUM(oi.line_revenue) AS line_sum
                FROM orders o
                JOIN order_items oi
                  ON oi.order_id = o.order_id AND oi.order_date = o.order_date
                GROUP BY o.order_id, o.total_amount
                HAVING ABS(o.total_amount - SUM(oi.line_revenue)) > 0.02
            ) mismatched
        """,
        threshold=0,
        severity="warning",
        description=(
            "Orders whose stated total disagrees with the sum of their "
            "generated line revenues."
        ),
    ),
    Check(
        name="negative_revenue_lines",
        sql="SELECT count(*) FROM order_items WHERE line_revenue < 0",
        threshold=0,
        description="Line items with negative revenue after discount.",
    ),
    Check(
        name="duplicate_customer_emails",
        sql="""
            SELECT count(*) FROM (
                SELECT lower(email) FROM customers
                GROUP BY lower(email) HAVING count(*) > 1
            ) duplicates
        """,
        threshold=0,
        description="Emails colliding once case is normalised.",
    ),
    Check(
        name="products_without_category",
        sql="""
            SELECT count(*) FROM products p
            LEFT JOIN product_categories c ON c.category_id = p.category_id
            WHERE c.category_id IS NULL
        """,
        threshold=0,
        description="Products referencing a missing category.",
    ),
    Check(
        name="future_dated_orders",
        sql="SELECT count(*) FROM orders WHERE order_date > NOW() + INTERVAL '1 day'",
        threshold=0,
        severity="warning",
        description="Orders dated more than a day into the future.",
    ),
    Check(
        name="unaggregated_selling_days",
        sql="""
            SELECT count(*) FROM (
                SELECT DISTINCT (o.order_date AT TIME ZONE 'UTC')::date AS d
                FROM orders o
                WHERE is_revenue_bearing(o.status)
                EXCEPT
                SELECT DISTINCT sale_date FROM daily_sales_aggregation
            ) missing
        """,
        threshold=0,
        severity="warning",
        description=(
            "Days with revenue-bearing orders but no rows in the daily "
            "aggregation, i.e. a rollup that did not cover the loaded range."
        ),
    ),
)


def run_checks(
    conn: psycopg.Connection,
    run_id: int | None = None,
    names: list[str] | None = None,
) -> list[CheckResult]:
    """Execute the checks and persist their results."""
    selected = [c for c in CHECKS if names is None or c.name in names]
    results: list[CheckResult] = []

    for check in selected:
        with conn.cursor() as cur:
            cur.execute(check.sql)
            observed = float(cur.fetchone()[0] or 0)

        result = CheckResult(check=check, observed=observed,
                             passed=check.evaluate(observed))
        results.append(result)

        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO data_quality_check "
                "(run_id, check_name, severity, passed, observed, threshold, details) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (run_id, check.name, check.severity, result.passed,
                 observed, check.threshold, check.description),
            )

        level = logger.info if result.passed else (
            logger.error if check.severity == "error" else logger.warning
        )
        level(
            "  [%s] %-28s observed=%g threshold=%g",
            "pass" if result.passed else check.severity.upper(),
            check.name, observed, check.threshold,
        )

    return results


def summarise(results: list[CheckResult]) -> tuple[int, int, int]:
    """Return (passed, warnings, errors)."""
    passed = sum(1 for r in results if r.passed)
    warnings = sum(1 for r in results if not r.passed and r.check.severity == "warning")
    errors = sum(1 for r in results if r.blocking)
    return passed, warnings, errors


def check_freshness(conn: psycopg.Connection, entity: str, max_age_days: int
                    ) -> CheckResult:
    """Assert the watermark for an entity is not stale.

    A pipeline that silently stops loading looks identical to one with no new
    data; this is the check that distinguishes them.
    """
    check = Check(
        name=f"{entity}_freshness",
        sql="",
        threshold=max_age_days,
        severity="warning",
        description=f"Days since {entity} last advanced its watermark.",
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - watermark_ts)) / 86400, 1e9) "
            "FROM etl_watermark WHERE entity = %s",
            (entity,),
        )
        row = cur.fetchone()
    observed = float(row[0]) if row else 1e9
    return CheckResult(check=check, observed=observed, passed=observed <= max_age_days)


def rows_in_range(conn: psycopg.Connection, start: date, end: date) -> int:
    """Order count in a date range, for reporting what a run actually covered."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM orders WHERE order_date >= %s AND order_date < (%s::date + 1)",
            (start, end),
        )
        return cur.fetchone()[0]
