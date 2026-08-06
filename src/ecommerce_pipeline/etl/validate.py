"""Validation and cleaning.

Every rule is expressed as a boolean mask over a chunk. Rows failing a rule are
quarantined with the name of the first rule they broke, rather than aborting
the batch: one malformed row in a 20M-row file should cost that row, not the
run. Reject counts and reasons are reported by the pipeline and recorded
against the ETL run so the failure is visible rather than silent.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

import pandas as pd

from ..logging_config import get_logger

logger = get_logger(__name__)

REJECT_REASON_COLUMN = "_reject_reason"

# Mirrors chk_customers_email_shape in migration 003.
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$")

ORDER_STATUSES = {
    "Pending", "Processing", "Shipped", "In Transit",
    "Delivered", "Cancelled", "Returned",
}
PAYMENT_METHODS = {
    "Credit Card", "PayPal", "Apple Pay", "Google Pay", "Gift Card",
    "Bank Transfer",
}


@dataclass
class ValidationResult:
    clean: pd.DataFrame
    rejected: pd.DataFrame
    reasons: Counter = field(default_factory=Counter)

    @property
    def clean_count(self) -> int:
        return len(self.clean)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _to_datetime(series: pd.Series) -> pd.Series:
    """Parse to UTC-aware timestamps.

    Source timestamps are naive ISO strings representing UTC instants; the
    database columns are TIMESTAMPTZ and the database runs in UTC, so they are
    localised here rather than relying on the server's interpretation.
    """
    parsed = pd.to_datetime(series, errors="coerce", format="ISO8601")
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize("UTC")
    else:
        parsed = parsed.dt.tz_convert("UTC")
    return parsed


def _to_bool(series: pd.Series) -> pd.Series:
    mapping = {
        "true": True, "t": True, "1": True, "yes": True, "True": True,
        "false": False, "f": False, "0": False, "no": False, "False": False,
    }
    return series.astype(str).str.strip().str.lower().map(mapping)


def _strip(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _non_blank(series: pd.Series) -> pd.Series:
    return series.notna() & (series.astype("string").str.len() > 0)


def _positive_int(series: pd.Series) -> pd.Series:
    return series.notna() & (series > 0) & (series == series.round(0))


def _apply(df: pd.DataFrame, rules: list[tuple[str, pd.Series]]) -> ValidationResult:
    """Split a chunk into clean and rejected rows.

    A row is attributed to the first rule it fails, so the reason counts read
    as "what most needs fixing" rather than double-counting a row that breaks
    several rules at once.
    """
    reason = pd.Series(pd.NA, index=df.index, dtype="object")

    for name, ok in rules:
        # Treat NA in a mask as failure: an unparseable value is not valid.
        failed = ~ok.fillna(False) & reason.isna()
        reason[failed] = name

    bad = reason.notna()
    clean = df.loc[~bad].copy()
    rejected = df.loc[bad].copy()
    if not rejected.empty:
        rejected[REJECT_REASON_COLUMN] = reason[bad]

    return ValidationResult(
        clean=clean,
        rejected=rejected,
        reasons=Counter(reason[bad].tolist()),
    )


# ---------------------------------------------------------------------------
# per-entity validators
# ---------------------------------------------------------------------------


def validate_product_categories(df: pd.DataFrame) -> ValidationResult:
    df = df.copy()
    df["category_id"] = _to_number(df["category_id"])
    df["parent_id"] = _to_number(df["parent_id"])
    df["name"] = _strip(df["name"])
    df["description"] = _strip(df["description"])

    return _apply(df, [
        ("category_id_invalid", _positive_int(df["category_id"])),
        ("name_blank", _non_blank(df["name"])),
        # NULL parent is valid (top-level category); a present one must be a
        # positive integer and must not point at the row itself.
        ("parent_id_invalid", df["parent_id"].isna() | _positive_int(df["parent_id"])),
        ("parent_is_self", df["parent_id"].isna() | (df["parent_id"] != df["category_id"])),
    ])


def validate_products(df: pd.DataFrame) -> ValidationResult:
    df = df.copy()
    for col in ("product_id", "category_id", "inventory_count"):
        df[col] = _to_number(df[col])
    for col in ("price", "cost", "weight"):
        df[col] = _to_number(df[col])
    for col in ("name", "description", "sku"):
        df[col] = _strip(df[col])
    df["is_active"] = _to_bool(df["is_active"]).fillna(True)

    return _apply(df, [
        ("product_id_invalid", _positive_int(df["product_id"])),
        ("name_blank", _non_blank(df["name"])),
        ("sku_blank", _non_blank(df["sku"])),
        ("category_id_invalid", _positive_int(df["category_id"])),
        ("price_invalid", df["price"].notna() & (df["price"] >= 0)),
        ("cost_invalid", df["cost"].isna() | (df["cost"] >= 0)),
        ("inventory_invalid", df["inventory_count"].isna() | (df["inventory_count"] >= 0)),
        ("weight_invalid", df["weight"].isna() | (df["weight"] >= 0)),
    ])


def validate_customers(df: pd.DataFrame) -> ValidationResult:
    df = df.copy()
    df["customer_id"] = _to_number(df["customer_id"])
    for col in ("email", "first_name", "last_name", "street_address", "city",
                "state", "zip_code", "country", "phone"):
        df[col] = _strip(df[col])
    # Lower-cased so the UNIQUE constraint on email behaves case-insensitively
    # without needing a citext column or a functional index.
    df["email"] = df["email"].str.lower()
    df["registration_date"] = _to_datetime(df["registration_date"])
    df["last_login"] = _to_datetime(df["last_login"])

    email_ok = df["email"].notna() & df["email"].str.match(_EMAIL_RE, na=False)

    return _apply(df, [
        ("customer_id_invalid", _positive_int(df["customer_id"])),
        ("email_invalid", email_ok),
        ("first_name_blank", _non_blank(df["first_name"])),
        ("last_name_blank", _non_blank(df["last_name"])),
        ("registration_date_invalid", df["registration_date"].notna()),
        ("last_login_before_registration",
         df["last_login"].isna() | (df["last_login"] >= df["registration_date"])),
    ])


def validate_orders(df: pd.DataFrame) -> ValidationResult:
    df = df.copy()
    for col in ("order_id", "customer_id"):
        df[col] = _to_number(df[col])
    df["total_amount"] = _to_number(df["total_amount"])
    for col in ("order_date", "processing_date", "shipping_date", "delivery_date"):
        df[col] = _to_datetime(df[col])
    for col in ("status", "payment_method", "shipping_address", "shipping_city",
                "shipping_state", "shipping_zip", "shipping_country"):
        df[col] = _strip(df[col])

    # Mirrors chk_orders_date_sequence: a violation would abort the whole COPY,
    # so it is caught per row here instead.
    sequence_ok = (
        (df["processing_date"].isna() | (df["processing_date"] >= df["order_date"]))
        & (df["shipping_date"].isna() | df["processing_date"].isna()
           | (df["shipping_date"] >= df["processing_date"]))
        & (df["delivery_date"].isna() | df["shipping_date"].isna()
           | (df["delivery_date"] >= df["shipping_date"]))
    )

    return _apply(df, [
        ("order_id_invalid", _positive_int(df["order_id"])),
        ("customer_id_invalid", _positive_int(df["customer_id"])),
        ("order_date_invalid", df["order_date"].notna()),
        ("status_unknown", df["status"].isin(ORDER_STATUSES)),
        ("payment_method_unknown", df["payment_method"].isin(PAYMENT_METHODS)),
        ("total_amount_invalid", df["total_amount"].notna() & (df["total_amount"] >= 0)),
        ("shipping_address_blank", _non_blank(df["shipping_address"])),
        ("date_sequence_invalid", sequence_ok),
    ])


def validate_order_items(df: pd.DataFrame) -> ValidationResult:
    df = df.copy()
    for col in ("order_item_id", "order_id", "product_id", "quantity"):
        df[col] = _to_number(df[col])
    for col in ("unit_price", "discount", "total"):
        df[col] = _to_number(df[col])
    df["order_date"] = _to_datetime(df["order_date"])
    df["discount"] = df["discount"].fillna(0)

    gross = df["unit_price"] * df["quantity"]

    return _apply(df, [
        ("order_item_id_invalid", _positive_int(df["order_item_id"])),
        ("order_id_invalid", _positive_int(df["order_id"])),
        ("product_id_invalid", _positive_int(df["product_id"])),
        ("order_date_invalid", df["order_date"].notna()),
        ("quantity_invalid", _positive_int(df["quantity"])),
        ("unit_price_invalid", df["unit_price"].notna() & (df["unit_price"] >= 0)),
        # Mirrors chk_order_items_discount_valid. The tolerance absorbs the
        # cent-level rounding difference between the source's own arithmetic
        # and ours; anything larger is a genuine data problem.
        ("discount_invalid",
         (df["discount"] >= 0) & (df["discount"] <= gross + 0.01)),
    ])


VALIDATORS = {
    "product_categories": validate_product_categories,
    "products": validate_products,
    "customers": validate_customers,
    "orders": validate_orders,
    "order_items": validate_order_items,
}


def validate(entity: str, df: pd.DataFrame) -> ValidationResult:
    try:
        validator = VALIDATORS[entity]
    except KeyError:
        raise ValueError(f"no validator registered for entity {entity!r}") from None
    return validator(df)


def revenue_mismatch(df: pd.DataFrame, tolerance: float = 0.011) -> pd.Series:
    """Rows whose source `total` disagrees with price x quantity - discount.

    A data-quality signal rather than a rejection rule: the database computes
    line_revenue itself via a generated column, so a mismatch means the source
    disagrees with the business rule, which is worth reporting but not worth
    dropping the row over.
    """
    if "total" not in df.columns:
        return pd.Series(False, index=df.index)
    expected = df["unit_price"] * df["quantity"] - df["discount"]
    return (expected - df["total"]).abs() > tolerance
