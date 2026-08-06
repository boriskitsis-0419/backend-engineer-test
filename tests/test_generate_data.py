"""Invariants the generated fixtures must hold.

These guard the properties the loader and the schema depend on. A fixture that
violates one of them would surface later as a confusing constraint violation
during COPY, so they are asserted at the source.
"""

from __future__ import annotations

import pandas as pd
import pytest

generate_data = pytest.importorskip(
    "generate_data", reason="datagen extra not installed (pip install -e '.[datagen]')"
)


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("fixtures")
    exit_code = generate_data.main(
        ["--scale", "tiny", "--output", str(out), "--seed", "7"]
    )
    assert exit_code == 0
    return {
        "categories": pd.read_csv(out / "product_categories.csv"),
        "products": pd.read_csv(out / "products.csv"),
        "customers": pd.read_csv(out / "customers.csv"),
        "orders": pd.read_csv(out / "orders.csv"),
        "items": pd.read_csv(out / "order_items.csv"),
        "path": out,
    }


# --- referential integrity ------------------------------------------------


def test_all_foreign_keys_resolve(generated):
    products, categories = generated["products"], generated["categories"]
    orders, customers = generated["orders"], generated["customers"]
    items = generated["items"]

    assert products.category_id.isin(categories.category_id).all()
    assert orders.customer_id.isin(customers.customer_id).all()
    assert items.order_id.isin(orders.order_id).all()
    assert items.product_id.isin(products.product_id).all()


def test_category_parents_are_top_level(generated):
    """Two-level hierarchy only, so category_hierarchy can never cycle."""
    categories = generated["categories"]
    parents = categories.parent_id.dropna().astype(int)
    top_level = set(categories[categories.parent_id.isna()].category_id)
    assert set(parents) <= top_level


def test_natural_keys_are_unique(generated):
    """Emails and SKUs back UNIQUE constraints; duplicates abort the load."""
    customers, products = generated["customers"], generated["products"]
    assert customers.email.nunique() == len(customers)
    assert products.sku.nunique() == len(products)
    assert customers.customer_id.nunique() == len(customers)


# --- the composite foreign key ---------------------------------------------


def test_item_order_date_matches_its_order(generated):
    """order_items references orders on (order_id, order_date).

    If the denormalised date drifted from the parent by even a second, every
    line item would fail the foreign key.
    """
    merged = generated["items"].merge(
        generated["orders"][["order_id", "order_date"]],
        on="order_id",
        suffixes=("_item", "_order"),
    )
    assert len(merged) == len(generated["items"])
    assert (merged.order_date_item == merged.order_date_order).all()


# --- CHECK constraints in the schema ---------------------------------------


def test_quantities_are_positive(generated):
    assert (generated["items"].quantity > 0).all()


def test_discount_never_exceeds_line_gross(generated):
    """Satisfies chk_order_items_discount_valid after independent rounding."""
    items = generated["items"]
    gross = items.price * items.quantity
    assert (items.discount >= 0).all()
    assert (items.discount <= gross + 1e-9).all()


def test_fulfilment_dates_are_monotonic(generated):
    """Satisfies chk_orders_date_sequence."""
    orders = generated["orders"]
    assert (orders.processing_date >= orders.order_date).all()
    assert (orders.shipping_date >= orders.processing_date).all()
    assert (orders.delivery_date >= orders.shipping_date).all()


def test_orders_never_precede_customer_registration(generated):
    merged = generated["orders"].merge(
        generated["customers"][["customer_id", "registration_date"]], on="customer_id"
    )
    assert (merged.order_date >= merged.registration_date).all()


def test_money_is_non_negative(generated):
    products, orders = generated["products"], generated["orders"]
    assert (products.price >= 0).all()
    assert (products.cost >= 0).all()
    assert (orders.total_amount >= 0).all()


# --- business rules --------------------------------------------------------


def test_line_total_equals_price_times_quantity_minus_discount(generated):
    items = generated["items"]
    expected = items.price * items.quantity - items.discount
    assert (expected - items.total).abs().max() < 0.011


def test_order_total_equals_sum_of_its_lines(generated):
    line_sums = generated["items"].groupby("order_id").total.sum()
    order_totals = generated["orders"].set_index("order_id").total_amount
    assert (line_sums - order_totals.loc[line_sums.index]).abs().max() < 0.011


def test_every_order_has_at_least_one_line(generated):
    assert set(generated["orders"].order_id) == set(generated["items"].order_id)


def test_statuses_are_valid_enum_members(generated):
    valid = {
        "Pending", "Processing", "Shipped", "In Transit",
        "Delivered", "Cancelled", "Returned",
    }
    assert set(generated["orders"].status) <= valid
    assert set(generated["orders"].payment_method) <= set(generate_data.PAYMENT_METHODS)


def test_dates_fall_inside_the_declared_partition_range(generated):
    """Anything outside 2019-2030 would land in the DEFAULT partition."""
    dates = generated["orders"].order_date
    assert dates.min() >= "2019-01-01"
    assert dates.max() < "2031-01-01"


# --- determinism -----------------------------------------------------------


def test_same_seed_reproduces_identical_output(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    for out in (first, second):
        generate_data.main(["--scale", "tiny", "--output", str(out), "--seed", "99"])

    for name in ("products.csv", "customers.csv", "orders.csv", "order_items.csv"):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


def test_different_seeds_produce_different_output(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    generate_data.main(["--scale", "tiny", "--output", str(first), "--seed", "1"])
    generate_data.main(["--scale", "tiny", "--output", str(second), "--seed", "2"])
    assert (first / "orders.csv").read_bytes() != (second / "orders.csv").read_bytes()


# --- scale honouring -------------------------------------------------------


def test_row_counts_match_the_requested_scale(generated):
    scale = generate_data.SCALES["tiny"]
    assert len(generated["categories"]) == scale.categories
    assert len(generated["products"]) == scale.products
    assert len(generated["customers"]) == scale.customers
    # Orders is exact unless the item cap binds first.
    assert len(generated["orders"]) <= scale.orders
    assert len(generated["items"]) <= scale.order_items


def test_items_per_order_matches_the_briefs_ratio(generated):
    """5M orders to 20M items implies ~4 lines per order."""
    ratio = len(generated["items"]) / len(generated["orders"])
    assert 3.5 < ratio < 4.7
