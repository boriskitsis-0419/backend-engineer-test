"""Integration tests asserting the migrated schema behaves as designed.

Each test runs inside a transaction that the `db` fixture rolls back, so they
can insert freely without cleaning up.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


def _scalar(db, sql, params=None):
    with db.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


# --- fixtures -------------------------------------------------------------


def _seed_minimal(db):
    """Insert one row down the full chain and return the order_date used."""
    order_date = "2026-03-15 10:00:00+00"
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO product_categories (name) VALUES ('Root') RETURNING category_id"
        )
        root_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO product_categories (name, parent_id) VALUES ('Child', %s)"
            " RETURNING category_id",
            (root_id,),
        )
        child_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO products (name, price, cost, category_id, sku)"
            " VALUES ('Widget', 100.00, 40.00, %s, 'SKU-TEST-1') RETURNING product_id",
            (child_id,),
        )
        product_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO customers (email, first_name, last_name)"
            " VALUES ('t@example.com', 'Test', 'User') RETURNING customer_id",
            (),
        )
        customer_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO orders (customer_id, order_date, status, payment_method,"
            " shipping_address, shipping_city, shipping_state, shipping_zip,"
            " shipping_country, total_amount)"
            " VALUES (%s, %s, 'Delivered', 'PayPal', '1 St', 'Town', 'CA', '90210',"
            " 'US', 185.00) RETURNING order_id",
            (customer_id, order_date),
        )
        order_id = cur.fetchone()[0]
    return {
        "root_id": root_id,
        "child_id": child_id,
        "product_id": product_id,
        "customer_id": customer_id,
        "order_id": order_id,
        "order_date": order_date,
    }


# --- partitioning ---------------------------------------------------------


def test_orders_primary_key_includes_partition_key(db):
    """The defect that made the reference schema fail to load."""
    cols = _scalar(
        db,
        """
        SELECT array_agg(a.attname ORDER BY a.attname)
        FROM pg_constraint c
        JOIN unnest(c.conkey) AS k(attnum) ON TRUE
        JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
        WHERE c.conrelid = 'orders'::regclass AND c.contype = 'p'
        """,
    )
    assert cols == ["order_date", "order_id"]


def test_orders_and_order_items_partitioned_identically(db):
    orders = _scalar(
        db, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'orders'::regclass"
    )
    items = _scalar(
        db, "SELECT count(*) FROM pg_inherits WHERE inhparent = 'order_items'::regclass"
    )
    assert orders > 100
    assert orders == items


def test_row_routes_to_the_month_partition(db):
    seeded = _seed_minimal(db)
    partition = _scalar(
        db,
        "SELECT tableoid::regclass::text FROM orders WHERE order_id = %s",
        (seeded["order_id"],),
    )
    assert partition == "orders_y2026m03"


def test_default_partitions_are_empty(db):
    """A non-empty default partition means a range partition was missing."""
    offenders = _scalar(
        db, "SELECT count(*) FROM count_default_partition_rows() WHERE row_count > 0"
    )
    assert offenders == 0


def test_ensure_monthly_partitions_is_idempotent(db):
    created = _scalar(
        db, "SELECT ensure_monthly_partitions('orders', '2026-01-01', '2026-03-01')"
    )
    assert created == 0, "partitions for this range already exist"


# --- constraints and generated columns ------------------------------------


def test_line_revenue_is_generated_from_its_inputs(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO order_items (order_id, order_date, product_id, quantity,"
            " unit_price, discount) VALUES (%s, %s, %s, 2, 100.00, 15.00)"
            " RETURNING line_revenue",
            (seeded["order_id"], seeded["order_date"], seeded["product_id"]),
        )
        # 100.00 * 2 - 15.00
        assert cur.fetchone()[0] == pytest.approx(185.00)


def test_order_items_foreign_key_requires_matching_order(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        with pytest.raises(Exception) as exc:
            cur.execute(
                "INSERT INTO order_items (order_id, order_date, product_id, quantity,"
                " unit_price, discount) VALUES (%s, %s, %s, 1, 10.00, 0)",
                (999_999_999, seeded["order_date"], seeded["product_id"]),
            )
        assert "foreign key" in str(exc.value).lower()


def test_discount_cannot_exceed_line_total(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        with pytest.raises(Exception) as exc:
            cur.execute(
                "INSERT INTO order_items (order_id, order_date, product_id, quantity,"
                " unit_price, discount) VALUES (%s, %s, %s, 1, 10.00, 99.00)",
                (seeded["order_id"], seeded["order_date"], seeded["product_id"]),
            )
        assert "chk_order_items_discount_valid" in str(exc.value)


def test_fulfilment_dates_must_not_run_backwards(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        with pytest.raises(Exception) as exc:
            cur.execute(
                "INSERT INTO orders (customer_id, order_date, payment_method,"
                " shipping_address, shipping_city, shipping_state, shipping_zip,"
                " shipping_country, total_amount, processing_date)"
                " VALUES (%s, '2026-03-15+00', 'PayPal', '1 St', 'T', 'CA', '90210',"
                " 'US', 10.00, '2026-03-01+00')",
                (seeded["customer_id"],),
            )
        assert "chk_orders_date_sequence" in str(exc.value)


# --- aggregation and derived attributes -----------------------------------


def test_refresh_daily_sales_is_idempotent_and_correct(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO order_items (order_id, order_date, product_id, quantity,"
            " unit_price, discount) VALUES (%s, %s, %s, 2, 100.00, 15.00)",
            (seeded["order_id"], seeded["order_date"], seeded["product_id"]),
        )

    # Rebuilding the same range twice must produce the same row count. The
    # count is not asserted to be 1: other data may already be loaded, so the
    # value-level assertions below are scoped to this test's own product.
    first = _scalar(db, "SELECT refresh_daily_sales('2026-03-01', '2026-03-31')")
    second = _scalar(db, "SELECT refresh_daily_sales('2026-03-01', '2026-03-31')")
    assert first == second

    with db.cursor() as cur:
        cur.execute(
            "SELECT units_sold, gross_revenue, net_revenue, discount_total"
            " FROM daily_sales_aggregation WHERE product_id = %s",
            (seeded["product_id"],),
        )
        units, gross, net, discount = cur.fetchone()
    assert units == 2
    assert gross == pytest.approx(200.00)
    assert net == pytest.approx(185.00)
    assert discount == pytest.approx(15.00)


def test_cancelled_orders_are_excluded_from_revenue(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        cur.execute(
            "UPDATE orders SET status = 'Cancelled' WHERE order_id = %s",
            (seeded["order_id"],),
        )
        cur.execute(
            "INSERT INTO order_items (order_id, order_date, product_id, quantity,"
            " unit_price, discount) VALUES (%s, %s, %s, 2, 100.00, 0)",
            (seeded["order_id"], seeded["order_date"], seeded["product_id"]),
        )

    _scalar(db, "SELECT refresh_daily_sales('2026-03-01', '2026-03-31')")
    # Scoped to this test's own product: the cancelled order contributed no
    # rollup row, regardless of what else is loaded in the database.
    rows = _scalar(
        db,
        "SELECT count(*) FROM daily_sales_aggregation WHERE product_id = %s",
        (seeded["product_id"],),
    )
    assert rows == 0


def test_avg_days_between_orders_is_null_for_single_order(db):
    seeded = _seed_minimal(db)
    _scalar(db, "SELECT refresh_customer_metrics(ARRAY[%s]::bigint[])", (seeded["customer_id"],))
    value = _scalar(
        db,
        "SELECT avg_days_between_orders FROM customer_metrics WHERE customer_id = %s",
        (seeded["customer_id"],),
    )
    assert value is None


def test_avg_days_between_orders_uses_n_minus_one_gaps(db):
    seeded = _seed_minimal(db)
    with db.cursor() as cur:
        # Two more orders, 10 and 20 days after the first: span 20 days across
        # 3 orders = 2 gaps = 10.0 days mean.
        for offset in ("2026-03-25 10:00:00+00", "2026-04-04 10:00:00+00"):
            cur.execute(
                "INSERT INTO orders (customer_id, order_date, status, payment_method,"
                " shipping_address, shipping_city, shipping_state, shipping_zip,"
                " shipping_country, total_amount)"
                " VALUES (%s, %s, 'Delivered', 'PayPal', '1 St', 'T', 'CA', '90210',"
                " 'US', 100.00)",
                (seeded["customer_id"], offset),
            )

    _scalar(db, "SELECT refresh_customer_metrics(ARRAY[%s]::bigint[])", (seeded["customer_id"],))
    with db.cursor() as cur:
        cur.execute(
            "SELECT total_orders, lifetime_value, avg_days_between_orders"
            " FROM customer_metrics WHERE customer_id = %s",
            (seeded["customer_id"],),
        )
        orders, ltv, gap = cur.fetchone()
    assert orders == 3
    assert ltv == pytest.approx(385.00)
    assert gap == pytest.approx(10.0)


# --- dimension and helper functions ---------------------------------------


def test_dim_time_holidays(db):
    # 2026: MLK is the 3rd Monday of January, Thanksgiving the 4th Thursday.
    assert _scalar(db, "SELECT holiday_name FROM dim_time WHERE date = '2026-01-19'") == (
        "Martin Luther King Jr. Day"
    )
    assert _scalar(db, "SELECT holiday_name FROM dim_time WHERE date = '2026-11-26'") == (
        "Thanksgiving Day"
    )
    assert _scalar(db, "SELECT is_weekend FROM dim_time WHERE date = '2026-03-15'") is True


def test_category_hierarchy_rolls_up_to_root(db):
    seeded = _seed_minimal(db)
    root = _scalar(
        db,
        "SELECT root_category_id FROM category_hierarchy WHERE category_id = %s",
        (seeded["child_id"],),
    )
    depth = _scalar(
        db,
        "SELECT depth FROM category_hierarchy WHERE category_id = %s",
        (seeded["child_id"],),
    )
    assert root == seeded["root_id"]
    assert depth == 2


def test_sync_identity_sequences_prevents_key_collision(db):
    """Reproduces the bulk-load footgun: explicit ids leave sequences behind."""
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO product_categories (category_id, name) VALUES (900001, 'Explicit')"
        )
        # Sequence still points at 1, so the next implicit insert would collide.
        cur.execute("SELECT sync_identity_sequences()")
        cur.execute(
            "INSERT INTO product_categories (name) VALUES ('Implicit') RETURNING category_id"
        )
        assert cur.fetchone()[0] == 900002


def test_updated_at_trigger_semantics(db_committed):
    """A real change bumps updated_at; a no-op update leaves it alone.

    Both halves need separate transactions: NOW() is the transaction
    timestamp, so a same-transaction re-read can never observe a difference.
    """
    conn = db_committed
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO product_categories (name) VALUES ('trigger-test')"
            " RETURNING category_id"
        )
        category_id = cur.fetchone()[0]

    product_id = None
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO products (name, price, category_id, sku)"
                " VALUES ('Trigger Widget', 10.00, %s, 'SKU-TRIGGER-TEST')"
                " RETURNING product_id, updated_at",
                (category_id,),
            )
            product_id, initial = cur.fetchone()

        # Separate transaction: a genuine change must move updated_at forward.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET price = price + 1 WHERE product_id = %s", (product_id,)
            )
            cur.execute(
                "SELECT updated_at FROM products WHERE product_id = %s", (product_id,)
            )
            after_real_update = cur.fetchone()[0]
        assert after_real_update > initial

        # Separate transaction again: a no-op must leave updated_at untouched,
        # so the column stays usable as a change-detection watermark.
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE products SET name = name WHERE product_id = %s", (product_id,)
            )
            cur.execute(
                "SELECT updated_at FROM products WHERE product_id = %s", (product_id,)
            )
            assert cur.fetchone()[0] == after_real_update
    finally:
        with conn.cursor() as cur:
            if product_id is not None:
                cur.execute("DELETE FROM products WHERE product_id = %s", (product_id,))
            cur.execute(
                "DELETE FROM product_categories WHERE category_id = %s", (category_id,)
            )


def test_watermark_never_moves_backwards(db):
    _scalar(db, "SELECT advance_watermark('test_entity', '2026-03-15+00')")
    result = _scalar(db, "SELECT advance_watermark('test_entity', '2026-01-01+00')")
    assert result.isoformat().startswith("2026-03-15")
