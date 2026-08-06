"""SQL behind the GraphQL resolvers.

Kept separate from the schema so the queries can be read, tested and tuned
without wading through type definitions. Every statement is parameterised;
the only interpolated fragments are sort clauses, and those are resolved from
a fixed allow-list rather than built from user input.

Reads go to the pre-aggregated tables (daily_sales_aggregation,
customer_metrics, mv_product_sales_summary) wherever possible. Answering "top
products this quarter" from those costs a scan of selling-days x products;
answering it from order_items costs a scan of every line item ever recorded.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from psycopg.rows import dict_row

from ..db import get_async_pool
from ..logging_config import get_logger

logger = get_logger(__name__)


# Interpolating an ORDER BY is unavoidable -- it cannot be a bind parameter --
# so every sortable field maps to a literal SQL fragment here. Anything not in
# the mapping is rejected before it reaches a query.
PRODUCT_SALES_SORTS = {
    "REVENUE": "net_revenue",
    "UNITS": "units_sold",
    "ORDERS": "order_count",
    "NAME": "product_name",
}

PRODUCT_SORTS = {
    "NAME": "p.name",
    "PRICE": "p.price",
    "CREATED_AT": "p.created_at",
    "INVENTORY": "p.inventory_count",
}

CUSTOMER_SORTS = {
    "LIFETIME_VALUE": "lifetime_value",
    "ORDER_COUNT": "order_count",
    "LAST_ORDER": "last_order_date",
    "NAME": "last_name",
}

GRANULARITY = {"DAY": "day", "WEEK": "week", "MONTH": "month", "QUARTER": "quarter", "YEAR": "year"}


@dataclass
class Page:
    rows: list[dict[str, Any]]
    total_count: int
    has_next_page: bool


def _direction(descending: bool) -> str:
    return "DESC" if descending else "ASC"


def _sort_clause(field: str, mapping: dict[str, str], descending: bool,
                 tiebreaker: str) -> str:
    """Resolve a sort enum to SQL, with a deterministic tiebreaker.

    Without the tiebreaker, rows with equal sort keys can be returned in a
    different order on each request, so paginating a tied set silently skips
    and repeats rows.
    """
    try:
        column = mapping[field]
    except KeyError:
        raise ValueError(f"unsupported sort field: {field!r}") from None
    return f"{column} {_direction(descending)} NULLS LAST, {tiebreaker}"


async def _fetch(sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    pool = await get_async_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()


async def _fetch_one(sql: str, params: tuple | dict | None = None) -> dict[str, Any] | None:
    rows = await _fetch(sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# 1. product sales by time period
# ---------------------------------------------------------------------------


async def product_sales(
    *,
    date_from: date,
    date_to: date,
    category_id: int | None = None,
    product_search: str | None = None,
    sort: str = "REVENUE",
    descending: bool = True,
    limit: int = 50,
    offset: int = 0,
) -> Page:
    """Sales per product over a period, from the daily rollup."""
    order_by = _sort_clause(sort, PRODUCT_SALES_SORTS, descending, "product_id ASC")

    filters = ["ds.sale_date BETWEEN %(date_from)s AND %(date_to)s"]
    params: dict[str, Any] = {"date_from": date_from, "date_to": date_to,
                              "limit": limit, "offset": offset}

    if category_id is not None:
        # Match the category or any of its descendants.
        filters.append(
            "ds.category_id IN (SELECT category_id FROM category_hierarchy "
            "WHERE category_id = %(category_id)s OR root_category_id = %(category_id)s)"
        )
        params["category_id"] = category_id

    if product_search:
        # Backed by the trigram index on products.name.
        filters.append("p.name ILIKE %(product_search)s")
        params["product_search"] = f"%{product_search}%"

    where = " AND ".join(filters)

    sql = f"""
        WITH aggregated AS (
            SELECT
                p.product_id,
                p.name          AS product_name,
                p.sku,
                p.category_id,
                pc.name         AS category_name,
                SUM(ds.units_sold)     AS units_sold,
                SUM(ds.net_revenue)    AS net_revenue,
                SUM(ds.gross_revenue)  AS gross_revenue,
                SUM(ds.discount_total) AS discount_total,
                SUM(ds.order_count)    AS order_count
            FROM daily_sales_aggregation ds
            JOIN products p ON p.product_id = ds.product_id
            JOIN product_categories pc ON pc.category_id = p.category_id
            WHERE {where}
            GROUP BY p.product_id, p.name, p.sku, p.category_id, pc.name
        )
        SELECT *, COUNT(*) OVER () AS total_count
        FROM aggregated
        ORDER BY {order_by}
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = await _fetch(sql, params)
    total = rows[0]["total_count"] if rows else 0
    return Page(rows=rows, total_count=total, has_next_page=offset + len(rows) < total)


# ---------------------------------------------------------------------------
# 2. customer purchase history
# ---------------------------------------------------------------------------


async def customer_by_id(customer_id: int) -> dict[str, Any] | None:
    return await _fetch_one(
        "SELECT * FROM v_customer_purchase_summary WHERE customer_id = %s",
        (customer_id,),
    )


async def customers(
    *,
    search: str | None = None,
    country: str | None = None,
    min_lifetime_value: float | None = None,
    sort: str = "LIFETIME_VALUE",
    descending: bool = True,
    limit: int = 50,
    offset: int = 0,
) -> Page:
    order_by = _sort_clause(sort, CUSTOMER_SORTS, descending, "customer_id ASC")

    filters = ["TRUE"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if search:
        filters.append("(email ILIKE %(search)s OR last_name ILIKE %(search)s)")
        params["search"] = f"%{search}%"
    if country:
        filters.append("country = %(country)s")
        params["country"] = country
    if min_lifetime_value is not None:
        filters.append("lifetime_value >= %(min_ltv)s")
        params["min_ltv"] = min_lifetime_value

    sql = f"""
        SELECT *, COUNT(*) OVER () AS total_count
        FROM v_customer_purchase_summary
        WHERE {" AND ".join(filters)}
        ORDER BY {order_by}
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = await _fetch(sql, params)
    total = rows[0]["total_count"] if rows else 0
    return Page(rows=rows, total_count=total, has_next_page=offset + len(rows) < total)


async def customer_orders(
    *,
    customer_id: int,
    first: int = 20,
    after_date: datetime | None = None,
    after_id: int | None = None,
    statuses: list[str] | None = None,
) -> tuple[list[dict[str, Any]], bool]:
    """Orders for one customer, newest first, keyset-paginated.

    Keyset rather than OFFSET: a customer's history is unbounded, and OFFSET
    makes the database walk and discard every skipped row, so page 500 costs
    500 pages of work. Seeking on (order_date, order_id) instead is a single
    index descent per page, and it stays correct when rows are inserted
    between requests. Both columns are needed because order_date alone is not
    unique.
    """
    filters = ["customer_id = %(customer_id)s"]
    params: dict[str, Any] = {"customer_id": customer_id, "limit": first + 1}

    if after_date is not None and after_id is not None:
        filters.append("(order_date, order_id) < (%(after_date)s, %(after_id)s)")
        params["after_date"] = after_date
        params["after_id"] = after_id

    if statuses:
        filters.append("status = ANY(%(statuses)s::order_status[])")
        params["statuses"] = statuses

    sql = f"""
        SELECT order_id, customer_id, order_date, status, payment_method,
               shipping_address, shipping_city, shipping_state, shipping_zip,
               shipping_country, processing_date, shipping_date, delivery_date,
               total_amount
        FROM orders
        WHERE {" AND ".join(filters)}
        ORDER BY order_date DESC, order_id DESC
        LIMIT %(limit)s
    """
    rows = await _fetch(sql, params)

    # One extra row was requested purely to detect a following page.
    has_next = len(rows) > first
    return rows[:first], has_next


# ---------------------------------------------------------------------------
# 3. top-selling products by category
# ---------------------------------------------------------------------------


async def top_products_by_category(
    *,
    date_from: date,
    date_to: date,
    category_id: int | None = None,
    limit_per_category: int = 5,
) -> list[dict[str, Any]]:
    """Top N products within each top-level category.

    A window function ranks within category in one pass, rather than issuing a
    separate top-N query per category.
    """
    params: dict[str, Any] = {
        "date_from": date_from, "date_to": date_to, "limit": limit_per_category,
    }
    category_filter = ""
    if category_id is not None:
        category_filter = "AND ch.root_category_id = %(category_id)s"
        params["category_id"] = category_id

    sql = f"""
        WITH per_product AS (
            SELECT
                ch.root_category_id    AS category_id,
                ch.root_category_name  AS category_name,
                p.product_id,
                p.name                 AS product_name,
                p.sku,
                SUM(ds.units_sold)     AS units_sold,
                SUM(ds.net_revenue)    AS net_revenue,
                SUM(ds.order_count)    AS order_count
            FROM daily_sales_aggregation ds
            JOIN products p ON p.product_id = ds.product_id
            JOIN category_hierarchy ch ON ch.category_id = ds.category_id
            WHERE ds.sale_date BETWEEN %(date_from)s AND %(date_to)s
              {category_filter}
            GROUP BY ch.root_category_id, ch.root_category_name,
                     p.product_id, p.name, p.sku
        ), ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY category_id ORDER BY net_revenue DESC, product_id
            ) AS rank
            FROM per_product
        )
        SELECT * FROM ranked WHERE rank <= %(limit)s
        ORDER BY category_name, rank
    """
    return await _fetch(sql, params)


# ---------------------------------------------------------------------------
# 4. sales trends over time
# ---------------------------------------------------------------------------


async def sales_trends(
    *,
    date_from: date,
    date_to: date,
    granularity: str = "DAY",
    category_id: int | None = None,
) -> list[dict[str, Any]]:
    """Revenue and units per time bucket."""
    try:
        unit = GRANULARITY[granularity]
    except KeyError:
        raise ValueError(f"unsupported granularity: {granularity!r}") from None

    params: dict[str, Any] = {"date_from": date_from, "date_to": date_to}
    filters = ["ds.sale_date BETWEEN %(date_from)s AND %(date_to)s"]

    if category_id is not None:
        filters.append(
            "ds.category_id IN (SELECT category_id FROM category_hierarchy "
            "WHERE category_id = %(category_id)s OR root_category_id = %(category_id)s)"
        )
        params["category_id"] = category_id

    sql = f"""
        SELECT
            date_trunc('{unit}', ds.sale_date)::date AS bucket_start,
            SUM(ds.units_sold)                       AS units_sold,
            SUM(ds.net_revenue)                      AS net_revenue,
            SUM(ds.gross_revenue)                    AS gross_revenue,
            SUM(ds.discount_total)                   AS discount_total,
            SUM(ds.order_count)                      AS order_count,
            COUNT(DISTINCT ds.product_id)            AS distinct_products
        FROM daily_sales_aggregation ds
        WHERE {" AND ".join(filters)}
        GROUP BY 1
        ORDER BY 1
    """
    return await _fetch(sql, params)


# ---------------------------------------------------------------------------
# products
# ---------------------------------------------------------------------------


async def products(
    *,
    search: str | None = None,
    category_id: int | None = None,
    active_only: bool = False,
    min_price: float | None = None,
    max_price: float | None = None,
    sort: str = "NAME",
    descending: bool = False,
    limit: int = 50,
    offset: int = 0,
) -> Page:
    order_by = _sort_clause(sort, PRODUCT_SORTS, descending, "p.product_id ASC")

    filters = ["TRUE"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if search:
        filters.append("p.name ILIKE %(search)s")
        params["search"] = f"%{search}%"
    if category_id is not None:
        filters.append(
            "p.category_id IN (SELECT category_id FROM category_hierarchy "
            "WHERE category_id = %(category_id)s OR root_category_id = %(category_id)s)"
        )
        params["category_id"] = category_id
    if active_only:
        filters.append("p.is_active")
    if min_price is not None:
        filters.append("p.price >= %(min_price)s")
        params["min_price"] = min_price
    if max_price is not None:
        filters.append("p.price <= %(max_price)s")
        params["max_price"] = max_price

    sql = f"""
        SELECT p.*, COUNT(*) OVER () AS total_count
        FROM products p
        WHERE {" AND ".join(filters)}
        ORDER BY {order_by}
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = await _fetch(sql, params)
    total = rows[0]["total_count"] if rows else 0
    return Page(rows=rows, total_count=total, has_next_page=offset + len(rows) < total)


async def product_by_id(product_id: int) -> dict[str, Any] | None:
    return await _fetch_one("SELECT * FROM products WHERE product_id = %s", (product_id,))


# ---------------------------------------------------------------------------
# batch loaders (N+1 avoidance)
# ---------------------------------------------------------------------------


async def categories_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    return await _fetch(
        "SELECT * FROM product_categories WHERE category_id = ANY(%s)", (ids,)
    )


async def products_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    return await _fetch("SELECT * FROM products WHERE product_id = ANY(%s)", (ids,))


async def customers_by_ids(ids: list[int]) -> list[dict[str, Any]]:
    return await _fetch(
        "SELECT * FROM v_customer_purchase_summary WHERE customer_id = ANY(%s)", (ids,)
    )


async def order_items_for_orders(keys: list[tuple[int, datetime]]) -> list[dict[str, Any]]:
    """Line items for many orders in one query.

    Both halves of the composite key are passed so the query can prune to the
    partitions actually involved; filtering on order_id alone would scan every
    partition.
    """
    order_ids = [key[0] for key in keys]
    dates = [key[1] for key in keys]
    return await _fetch(
        """
        SELECT oi.*
        FROM order_items oi
        WHERE (oi.order_id, oi.order_date) IN (
            SELECT * FROM unnest(%s::bigint[], %s::timestamptz[])
        )
        ORDER BY oi.order_id, oi.order_item_id
        """,
        (order_ids, dates),
    )


# ---------------------------------------------------------------------------
# mutation
# ---------------------------------------------------------------------------


class ProductNotFound(LookupError):
    pass


class NoFieldsToUpdate(ValueError):
    pass


async def update_product(product_id: int, changes: dict[str, Any]) -> dict[str, Any]:
    """Apply a partial update to a product.

    Only the fields the caller actually supplied are written, so two clients
    editing different attributes do not overwrite each other with stale copies
    of the fields they never touched.
    """
    allowed = {
        "name", "description", "price", "cost", "category_id",
        "sku", "inventory_count", "weight", "is_active",
    }
    updates = {k: v for k, v in changes.items() if k in allowed and v is not None}
    if not updates:
        raise NoFieldsToUpdate("no updatable fields supplied")

    assignments = ", ".join(f"{col} = %({col})s" for col in updates)
    params = dict(updates, product_id=product_id)

    pool = await get_async_pool()
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"UPDATE products SET {assignments} WHERE product_id = %(product_id)s "
                f"RETURNING *",
                params,
            )
            row = await cur.fetchone()

    if row is None:
        raise ProductNotFound(f"no product with id {product_id}")
    logger.info("updated product %d: %s", product_id, sorted(updates))
    return row
