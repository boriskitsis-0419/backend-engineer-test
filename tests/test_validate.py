"""Unit tests for validation and cleaning. No database required.

Each test asserts both halves of a rule: that a bad row is rejected with the
expected reason, and that the neighbouring good rows survive. A validator that
quarantines everything would otherwise pass a rejection-only test.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ecommerce_pipeline.etl import validate
from ecommerce_pipeline.etl.spec import ENTITIES_BY_NAME


# --- fixtures -------------------------------------------------------------


def _categories(**overrides) -> pd.DataFrame:
    base = {"category_id": ["1", "2"], "name": ["Electronics", "Audio"],
            "description": ["d1", "d2"], "parent_id": [None, "1"]}
    base.update(overrides)
    return pd.DataFrame(base)


def _products(**overrides) -> pd.DataFrame:
    base = {"product_id": ["1", "2"], "name": ["Widget", "Gadget"],
            "description": ["d", "d"], "price": ["10.00", "20.00"],
            "cost": ["4.00", "8.00"], "category_id": ["1", "1"],
            "sku": ["SKU-1", "SKU-2"], "inventory_count": ["5", "6"],
            "weight": ["1.0", "2.0"], "is_active": ["true", "false"]}
    base.update(overrides)
    return pd.DataFrame(base)


def _customers(**overrides) -> pd.DataFrame:
    base = {"customer_id": ["1", "2"], "email": ["A@Example.COM", "b@example.com"],
            "first_name": ["Ada", "Bob"], "last_name": ["Lovelace", "Ross"],
            "street_address": ["1 St", "2 St"], "city": ["X", "Y"],
            "state": ["CA", "NY"], "zip_code": ["90210", "10001"],
            "country": ["US", "US"], "phone": ["555", "556"],
            "registration_date": ["2024-01-01T00:00:00", "2024-01-02T00:00:00"],
            "last_login": ["2025-01-01T00:00:00", "2025-01-02T00:00:00"]}
    base.update(overrides)
    return pd.DataFrame(base)


def _orders(**overrides) -> pd.DataFrame:
    base = {"order_id": ["1", "2"], "customer_id": ["1", "2"],
            "order_date": ["2026-03-15T10:00:00", "2026-03-16T10:00:00"],
            "status": ["Delivered", "Shipped"],
            "payment_method": ["PayPal", "Credit Card"],
            "shipping_address": ["1 St", "2 St"], "shipping_city": ["X", "Y"],
            "shipping_state": ["CA", "NY"], "shipping_zip": ["90210", "10001"],
            "shipping_country": ["US", "US"],
            "processing_date": ["2026-03-15T12:00:00", "2026-03-16T12:00:00"],
            "shipping_date": ["2026-03-16T12:00:00", "2026-03-17T12:00:00"],
            "delivery_date": ["2026-03-18T12:00:00", "2026-03-19T12:00:00"],
            "total_amount": ["100.00", "200.00"]}
    base.update(overrides)
    return pd.DataFrame(base)


def _items(**overrides) -> pd.DataFrame:
    base = {"order_item_id": ["1", "2"], "order_id": ["1", "2"],
            "order_date": ["2026-03-15T10:00:00", "2026-03-16T10:00:00"],
            "product_id": ["1", "2"], "quantity": ["2", "1"],
            "unit_price": ["50.00", "200.00"], "discount": ["0.00", "0.00"],
            "total": ["100.00", "200.00"]}
    base.update(overrides)
    return pd.DataFrame(base)


def _reasons(result: validate.ValidationResult) -> list[str]:
    return list(result.rejected[validate.REJECT_REASON_COLUMN])


# --- clean input passes through -------------------------------------------


@pytest.mark.parametrize("entity,frame", [
    ("product_categories", _categories()),
    ("products", _products()),
    ("customers", _customers()),
    ("orders", _orders()),
    ("order_items", _items()),
])
def test_valid_rows_are_not_rejected(entity, frame):
    result = validate.validate(entity, frame)
    assert result.rejected_count == 0
    assert result.clean_count == len(frame)


# --- categories -----------------------------------------------------------


def test_category_null_parent_is_valid():
    """A top-level category legitimately has no parent."""
    result = validate.validate("product_categories", _categories())
    assert result.clean.parent_id.isna().sum() == 1


def test_category_cannot_be_its_own_parent():
    result = validate.validate("product_categories", _categories(parent_id=[None, "2"]))
    assert _reasons(result) == ["parent_is_self"]


def test_category_blank_name_rejected():
    result = validate.validate("product_categories", _categories(name=["", "Audio"]))
    assert _reasons(result) == ["name_blank"]
    assert result.clean_count == 1


# --- products -------------------------------------------------------------


def test_negative_price_rejected():
    result = validate.validate("products", _products(price=["-1.00", "20.00"]))
    assert _reasons(result) == ["price_invalid"]


def test_zero_price_allowed():
    """Free items are legitimate; the CHECK constraint permits price = 0."""
    result = validate.validate("products", _products(price=["0.00", "20.00"]))
    assert result.rejected_count == 0


def test_blank_sku_rejected():
    result = validate.validate("products", _products(sku=["", "SKU-2"]))
    assert _reasons(result) == ["sku_blank"]


def test_null_cost_and_weight_allowed():
    result = validate.validate(
        "products", _products(cost=[None, "8.00"], weight=[None, "2.0"])
    )
    assert result.rejected_count == 0


def test_unparseable_price_rejected():
    result = validate.validate("products", _products(price=["not-a-number", "20.00"]))
    assert _reasons(result) == ["price_invalid"]


def test_is_active_parses_common_spellings():
    frame = _products(is_active=["TRUE", "f"])
    result = validate.validate("products", frame)
    assert list(result.clean.is_active) == [True, False]


# --- customers ------------------------------------------------------------


def test_email_is_lowercased():
    """The UNIQUE constraint is case-sensitive, so normalisation happens here."""
    result = validate.validate("customers", _customers())
    assert list(result.clean.email) == ["a@example.com", "b@example.com"]


@pytest.mark.parametrize("bad", ["not-an-email", "no@tld", "@example.com", "a b@x.com"])
def test_malformed_emails_rejected(bad):
    result = validate.validate("customers", _customers(email=[bad, "b@example.com"]))
    assert _reasons(result) == ["email_invalid"]


def test_login_before_registration_rejected():
    result = validate.validate(
        "customers", _customers(last_login=["2000-01-01T00:00:00", "2025-01-02T00:00:00"])
    )
    assert _reasons(result) == ["last_login_before_registration"]


def test_null_last_login_allowed():
    result = validate.validate("customers", _customers(last_login=[None, None]))
    assert result.rejected_count == 0


def test_timestamps_are_localised_to_utc():
    result = validate.validate("customers", _customers())
    assert str(result.clean.registration_date.dt.tz) == "UTC"


# --- orders ---------------------------------------------------------------


def test_unknown_status_rejected():
    """Statuses map onto a PostgreSQL enum; an unknown value aborts the COPY."""
    result = validate.validate("orders", _orders(status=["Teleported", "Shipped"]))
    assert _reasons(result) == ["status_unknown"]


def test_unknown_payment_method_rejected():
    result = validate.validate("orders", _orders(payment_method=["Barter", "PayPal"]))
    assert _reasons(result) == ["payment_method_unknown"]


def test_negative_total_rejected():
    result = validate.validate("orders", _orders(total_amount=["-1.00", "200.00"]))
    assert _reasons(result) == ["total_amount_invalid"]


def test_backwards_fulfilment_dates_rejected():
    """Mirrors chk_orders_date_sequence, which would otherwise abort the load."""
    result = validate.validate(
        "orders", _orders(delivery_date=["2000-01-01T00:00:00", "2026-03-19T12:00:00"])
    )
    assert _reasons(result) == ["date_sequence_invalid"]


def test_null_fulfilment_dates_allowed():
    """A pending order has no shipping or delivery date yet."""
    result = validate.validate(
        "orders",
        _orders(processing_date=[None, None], shipping_date=[None, None],
                delivery_date=[None, None]),
    )
    assert result.rejected_count == 0


def test_unparseable_order_date_rejected():
    result = validate.validate("orders", _orders(order_date=["nonsense", "2026-03-16T10:00:00"]))
    assert _reasons(result) == ["order_date_invalid"]


# --- order items ----------------------------------------------------------


def test_zero_quantity_rejected():
    result = validate.validate("order_items", _items(quantity=["0", "1"]))
    assert _reasons(result) == ["quantity_invalid"]


def test_discount_above_line_gross_rejected():
    """Mirrors chk_order_items_discount_valid: discount <= unit_price * quantity."""
    result = validate.validate("order_items", _items(discount=["500.00", "0.00"]))
    assert _reasons(result) == ["discount_invalid"]


def test_discount_equal_to_gross_allowed():
    """A 100% discount is a legitimate promotion, not a data error."""
    result = validate.validate("order_items", _items(discount=["100.00", "0.00"]))
    assert result.rejected_count == 0


def test_negative_discount_rejected():
    result = validate.validate("order_items", _items(discount=["-1.00", "0.00"]))
    assert _reasons(result) == ["discount_invalid"]


def test_missing_discount_defaults_to_zero():
    result = validate.validate("order_items", _items(discount=[None, None]))
    assert result.rejected_count == 0
    assert list(result.clean.discount) == [0.0, 0.0]


def test_revenue_mismatch_is_reported_not_rejected():
    """The database generates line_revenue itself; a disagreeing source total
    is a quality signal, not a reason to drop the row."""
    frame = _items(total=["999.99", "200.00"])
    result = validate.validate("order_items", frame)
    assert result.rejected_count == 0
    assert int(validate.revenue_mismatch(result.clean).sum()) == 1


# --- rejection accounting -------------------------------------------------


def test_row_attributed_to_first_failing_rule_only():
    """A row breaking several rules is counted once, so reason totals are
    readable as 'what most needs fixing'."""
    frame = _items(quantity=["0", "1"], discount=["9999.00", "0.00"])
    result = validate.validate("order_items", frame)
    assert result.rejected_count == 1
    assert _reasons(result) == ["quantity_invalid"]
    assert sum(result.reasons.values()) == 1


def test_clean_and_rejected_partition_the_input():
    frame = _orders(status=["Teleported", "Shipped"])
    result = validate.validate("orders", frame)
    assert result.clean_count + result.rejected_count == len(frame)


def test_empty_input_is_handled():
    result = validate.validate("orders", _orders().iloc[0:0])
    assert result.clean_count == 0
    assert result.rejected_count == 0


def test_unknown_entity_raises():
    with pytest.raises(ValueError, match="no validator registered"):
        validate.validate("nope", pd.DataFrame())


# --- spec consistency -----------------------------------------------------


def test_every_entity_has_a_validator():
    assert set(ENTITIES_BY_NAME) == set(validate.VALIDATORS)


def test_order_items_does_not_load_the_generated_column():
    """line_revenue is GENERATED ALWAYS; writing to it is an error."""
    assert "line_revenue" not in ENTITIES_BY_NAME["order_items"].columns


def test_partitioned_specs_include_partition_key_in_conflict_target():
    for name in ("orders", "order_items"):
        assert "order_date" in ENTITIES_BY_NAME[name].conflict_key


def test_source_columns_reverse_the_rename():
    spec = ENTITIES_BY_NAME["order_items"]
    assert "price" in spec.source_columns
    assert "unit_price" not in spec.source_columns
    assert "total" in spec.source_columns
