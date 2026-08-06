"""GraphQL API tests.

Executed against the schema directly rather than over HTTP: that still runs
the real resolvers, DataLoaders and SQL, but without needing a live server.
"""

from __future__ import annotations

import base64
from datetime import date

import pytest

from ecommerce_pipeline.api.loaders import Loaders
from ecommerce_pipeline.api.schema import _decode_cursor, _encode_cursor, schema

pytestmark = pytest.mark.integration


PERIOD = '{fromDate: "2019-01-01", toDate: "2030-12-31"}'


async def _execute(query: str, variables: dict | None = None):
    """Run a document with a fresh set of per-request loaders."""
    result = await schema.execute(
        query, variable_values=variables, context_value={"loaders": Loaders()}
    )
    assert result.errors is None, result.errors
    return result.data


@pytest.fixture
def require_db(db_available):
    if not db_available:
        pytest.skip("no PostgreSQL reachable")


# --- schema shape ----------------------------------------------------------


def test_schema_exposes_the_four_required_queries():
    sdl = schema.as_str()
    for required in ("productSales", "customerPurchaseHistory",
                     "topProductsByCategory", "salesTrends"):
        assert required in sdl, required


def test_schema_exposes_a_mutation():
    assert "updateProduct" in schema.as_str()


# --- 1. product sales by time period ---------------------------------------


@pytest.mark.asyncio
async def test_product_sales_returns_rows_and_paging_metadata(require_db):
    data = await _execute(f"""
        {{ productSales(period: {PERIOD}, page: {{limit: 5}}) {{
            pageInfo {{ totalCount hasNextPage limit offset }}
            items {{ productId productName sku netRevenue unitsSold orderCount }}
        }} }}
    """)
    page = data["productSales"]
    assert len(page["items"]) == 5
    assert page["pageInfo"]["totalCount"] > 5
    assert page["pageInfo"]["hasNextPage"] is True


@pytest.mark.asyncio
async def test_product_sales_sorted_descending_by_revenue(require_db):
    data = await _execute(f"""
        {{ productSales(period: {PERIOD}, sort: REVENUE, descending: true,
                        page: {{limit: 10}}) {{
            items {{ netRevenue }} }} }}
    """)
    revenues = [float(i["netRevenue"]) for i in data["productSales"]["items"]]
    assert revenues == sorted(revenues, reverse=True)


@pytest.mark.asyncio
async def test_product_sales_sort_direction_is_honoured(require_db):
    data = await _execute(f"""
        {{ productSales(period: {PERIOD}, sort: UNITS, descending: false,
                        page: {{limit: 10}}) {{
            items {{ unitsSold }} }} }}
    """)
    units = [i["unitsSold"] for i in data["productSales"]["items"]]
    assert units == sorted(units)


@pytest.mark.asyncio
async def test_product_sales_offset_advances_the_window(require_db):
    first = await _execute(
        f"{{ productSales(period: {PERIOD}, page: {{limit: 3, offset: 0}}) "
        f"{{ items {{ productId }} }} }}"
    )
    second = await _execute(
        f"{{ productSales(period: {PERIOD}, page: {{limit: 3, offset: 3}}) "
        f"{{ items {{ productId }} }} }}"
    )
    ids_first = {i["productId"] for i in first["productSales"]["items"]}
    ids_second = {i["productId"] for i in second["productSales"]["items"]}
    assert ids_first.isdisjoint(ids_second)


@pytest.mark.asyncio
async def test_empty_period_returns_an_empty_page_not_an_error(require_db):
    data = await _execute("""
        { productSales(period: {fromDate: "1990-01-01", toDate: "1990-01-02"}) {
            pageInfo { totalCount } items { productId } } }
    """)
    assert data["productSales"]["items"] == []
    assert data["productSales"]["pageInfo"]["totalCount"] == 0


# --- 2. customer purchase history ------------------------------------------


@pytest.mark.asyncio
async def test_customer_purchase_history_is_newest_first(require_db):
    customers = await _execute(
        "{ customers(sort: ORDER_COUNT, descending: true, page: {limit: 1}) "
        "{ items { customerId } } }"
    )
    customer_id = customers["customers"]["items"][0]["customerId"]

    data = await _execute(
        f"{{ customerPurchaseHistory(customerId: {customer_id}, first: 10) "
        f"{{ items {{ orderId orderDate }} }} }}"
    )
    dates = [i["orderDate"] for i in data["customerPurchaseHistory"]["items"]]
    assert dates == sorted(dates, reverse=True)


@pytest.mark.asyncio
async def test_keyset_pagination_does_not_repeat_or_skip(require_db):
    """Following endCursor must walk a strictly disjoint sequence."""
    customers = await _execute(
        "{ customers(sort: ORDER_COUNT, descending: true, page: {limit: 1}) "
        "{ items { customerId } } }"
    )
    customer_id = customers["customers"]["items"][0]["customerId"]

    seen: list[int] = []
    cursor = None
    for _ in range(4):
        after = f', after: "{cursor}"' if cursor else ""
        data = await _execute(
            f"{{ customerPurchaseHistory(customerId: {customer_id}, first: 2{after}) "
            f"{{ hasNextPage endCursor items {{ orderId }} }} }}"
        )
        page = data["customerPurchaseHistory"]
        seen.extend(i["orderId"] for i in page["items"])
        cursor = page["endCursor"]
        if not page["hasNextPage"]:
            break

    assert len(seen) == len(set(seen)), "keyset pagination repeated a row"


@pytest.mark.asyncio
async def test_purchase_history_status_filter(require_db):
    customers = await _execute(
        "{ customers(sort: ORDER_COUNT, descending: true, page: {limit: 1}) "
        "{ items { customerId } } }"
    )
    customer_id = customers["customers"]["items"][0]["customerId"]

    data = await _execute(
        f"{{ customerPurchaseHistory(customerId: {customer_id}, first: 20, "
        f"statuses: [DELIVERED]) {{ items {{ status }} }} }}"
    )
    statuses = {i["status"] for i in data["customerPurchaseHistory"]["items"]}
    assert statuses <= {"Delivered"}


@pytest.mark.asyncio
async def test_unknown_customer_returns_null_not_an_error(require_db):
    data = await _execute("{ customer(customerId: 99999999) { customerId } }")
    assert data["customer"] is None


def test_cursor_round_trips():
    from datetime import datetime, timezone

    moment = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)
    decoded_date, decoded_id = _decode_cursor(_encode_cursor(moment, 42))
    assert decoded_date == moment
    assert decoded_id == 42


def test_malformed_cursor_is_rejected():
    with pytest.raises(ValueError, match="malformed cursor"):
        _decode_cursor(base64.urlsafe_b64encode(b"garbage").decode())


# --- 3. top products by category -------------------------------------------


@pytest.mark.asyncio
async def test_top_products_respects_limit_per_category(require_db):
    data = await _execute(
        f"{{ topProductsByCategory(period: {PERIOD}, limitPerCategory: 3) "
        f"{{ rank categoryId categoryName productId netRevenue }} }}"
    )
    rows = data["topProductsByCategory"]
    assert rows

    per_category: dict[int, list[int]] = {}
    for row in rows:
        per_category.setdefault(row["categoryId"], []).append(row["rank"])

    for category_id, ranks in per_category.items():
        assert len(ranks) <= 3, category_id
        # Ranks are dense and start at 1 within each category.
        assert ranks == list(range(1, len(ranks) + 1)), category_id


@pytest.mark.asyncio
async def test_top_products_ranked_by_descending_revenue(require_db):
    data = await _execute(
        f"{{ topProductsByCategory(period: {PERIOD}, limitPerCategory: 5) "
        f"{{ categoryId rank netRevenue }} }}"
    )
    by_category: dict[int, list[float]] = {}
    for row in data["topProductsByCategory"]:
        by_category.setdefault(row["categoryId"], []).append(float(row["netRevenue"]))

    for category_id, revenues in by_category.items():
        assert revenues == sorted(revenues, reverse=True), category_id


# --- 4. sales trends -------------------------------------------------------


@pytest.mark.asyncio
async def test_sales_trends_buckets_are_ordered_and_distinct(require_db):
    data = await _execute(
        f"{{ salesTrends(period: {PERIOD}, granularity: MONTH) "
        f"{{ bucketStart netRevenue unitsSold orderCount }} }}"
    )
    buckets = [p["bucketStart"] for p in data["salesTrends"]]
    assert buckets == sorted(buckets)
    assert len(buckets) == len(set(buckets))


@pytest.mark.asyncio
async def test_coarser_granularity_yields_fewer_buckets(require_db):
    daily = await _execute(
        f"{{ salesTrends(period: {PERIOD}, granularity: DAY) {{ bucketStart }} }}"
    )
    yearly = await _execute(
        f"{{ salesTrends(period: {PERIOD}, granularity: YEAR) {{ bucketStart }} }}"
    )
    assert len(yearly["salesTrends"]) < len(daily["salesTrends"])


@pytest.mark.asyncio
async def test_trend_totals_reconcile_across_granularities(require_db):
    """Re-bucketing must not change the totals."""
    daily = await _execute(
        f"{{ salesTrends(period: {PERIOD}, granularity: DAY) {{ netRevenue }} }}"
    )
    monthly = await _execute(
        f"{{ salesTrends(period: {PERIOD}, granularity: MONTH) {{ netRevenue }} }}"
    )
    total_daily = sum(float(p["netRevenue"]) for p in daily["salesTrends"])
    total_monthly = sum(float(p["netRevenue"]) for p in monthly["salesTrends"])
    assert abs(total_daily - total_monthly) < 0.02


# --- nested resolution through DataLoaders ---------------------------------


@pytest.mark.asyncio
async def test_nested_product_and_category_resolve(require_db):
    """Exercises the loader path: sales -> product -> category."""
    data = await _execute(f"""
        {{ productSales(period: {PERIOD}, page: {{limit: 20}}) {{
            items {{ productId product {{ name category {{ categoryId name }} }} }}
        }} }}
    """)
    items = data["productSales"]["items"]
    assert len(items) == 20
    assert all(i["product"] is not None for i in items)
    assert all(i["product"]["category"] is not None for i in items)


@pytest.mark.asyncio
async def test_order_items_resolve_through_the_composite_key_loader(require_db):
    customers = await _execute(
        "{ customers(sort: ORDER_COUNT, descending: true, page: {limit: 1}) "
        "{ items { customerId } } }"
    )
    customer_id = customers["customers"]["items"][0]["customerId"]

    data = await _execute(f"""
        {{ customerPurchaseHistory(customerId: {customer_id}, first: 5) {{
            items {{ orderId totalAmount
                     items {{ orderItemId quantity lineRevenue
                              product {{ productId name }} }} }}
        }} }}
    """)
    orders = data["customerPurchaseHistory"]["items"]
    assert orders
    for order in orders:
        assert order["items"], f"order {order['orderId']} resolved no line items"
        for line in order["items"]:
            assert line["product"] is not None


@pytest.mark.asyncio
async def test_line_revenue_matches_the_generated_column(require_db):
    customers = await _execute(
        "{ customers(sort: ORDER_COUNT, descending: true, page: {limit: 1}) "
        "{ items { customerId } } }"
    )
    customer_id = customers["customers"]["items"][0]["customerId"]

    data = await _execute(f"""
        {{ customerPurchaseHistory(customerId: {customer_id}, first: 3) {{
            items {{ items {{ quantity unitPrice discount lineRevenue }} }} }} }}
    """)
    for order in data["customerPurchaseHistory"]["items"]:
        for line in order["items"]:
            expected = (
                float(line["unitPrice"]) * line["quantity"] - float(line["discount"])
            )
            assert abs(float(line["lineRevenue"]) - expected) < 0.011


# --- guards ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_page_size_is_clamped(require_db):
    """A caller must not be able to request the entire table."""
    data = await _execute(
        "{ products(page: {limit: 100000}) { pageInfo { limit } } }"
    )
    assert data["products"]["pageInfo"]["limit"] == 200


@pytest.mark.asyncio
async def test_negative_offset_is_clamped(require_db):
    data = await _execute(
        "{ products(page: {limit: 5, offset: -10}) { pageInfo { offset } } }"
    )
    assert data["products"]["pageInfo"]["offset"] == 0


@pytest.mark.asyncio
async def test_excessive_query_depth_is_rejected(require_db):
    """GraphQL lets a client nest its way into an arbitrarily expensive
    document; QueryDepthLimiter caps it before any resolver runs."""
    nested = "name"
    for _ in range(15):
        nested = f"category {{ {nested} }}"
    result = await schema.execute(
        "{ products { items { %s } } }" % nested,
        context_value={"loaders": Loaders()},
    )
    assert result.errors is not None
    assert any("depth" in str(e).lower() for e in result.errors)


@pytest.mark.asyncio
async def test_inverted_date_range_is_rejected(require_db):
    result = await schema.execute(
        '{ productSales(period: {fromDate: "2026-12-31", toDate: "2026-01-01"}) '
        "{ pageInfo { totalCount } } }",
        context_value={"loaders": Loaders()},
    )
    assert result.errors is not None


# --- mutation --------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_product_applies_only_supplied_fields(require_db):
    before = await _execute("{ product(productId: 2) { name price inventoryCount } }")
    original = before["product"]

    data = await _execute(
        'mutation { updateProduct(productId: 2, input: {inventoryCount: 4242}) '
        "{ success product { name price inventoryCount } } }"
    )
    result = data["updateProduct"]
    assert result["success"] is True
    assert result["product"]["inventoryCount"] == 4242
    # Untouched fields keep their values.
    assert result["product"]["name"] == original["name"]
    assert result["product"]["price"] == original["price"]

    # Restore.
    await _execute(
        f'mutation {{ updateProduct(productId: 2, input: '
        f'{{inventoryCount: {original["inventoryCount"]}}}) {{ success }} }}'
    )


@pytest.mark.asyncio
async def test_update_product_rejects_constraint_violation_without_leaking_row(require_db):
    """The failure is reported, but the raw DETAIL line -- which contains every
    column value of the offending row -- must not reach the client."""
    data = await _execute(
        'mutation { updateProduct(productId: 2, input: {price: "-1.00"}) '
        "{ success message } }"
    )
    result = data["updateProduct"]
    assert result["success"] is False
    assert "chk_products_price_non_negative" in result["message"]
    assert "DETAIL" not in result["message"]


@pytest.mark.asyncio
async def test_update_unknown_product_reports_failure(require_db):
    data = await _execute(
        'mutation { updateProduct(productId: 99999999, input: {price: "1.00"}) '
        "{ success message product { productId } } }"
    )
    assert data["updateProduct"]["success"] is False
    assert data["updateProduct"]["product"] is None


@pytest.mark.asyncio
async def test_update_with_no_fields_reports_failure(require_db):
    data = await _execute(
        "mutation { updateProduct(productId: 2, input: {}) { success message } }"
    )
    assert data["updateProduct"]["success"] is False
    assert "no updatable fields" in data["updateProduct"]["message"].lower()


@pytest.mark.asyncio
async def test_update_product_rejects_duplicate_sku(require_db):
    existing = await _execute("{ products(page: {limit: 2}) { items { productId sku } } }")
    items = existing["products"]["items"]
    victim, other = items[0], items[1]

    data = await _execute(
        f'mutation {{ updateProduct(productId: {victim["productId"]}, '
        f'input: {{sku: "{other["sku"]}"}}) {{ success message }} }}'
    )
    assert data["updateProduct"]["success"] is False
    assert "sku" in data["updateProduct"]["message"].lower()
