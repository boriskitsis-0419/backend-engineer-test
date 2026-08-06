"""Declarative description of each entity the pipeline loads.

Keeping the source file, target table, column mapping and conflict key in one
place means extract, validate and load all agree on the shape of an entity
without repeating literals across three modules.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ForeignKey:
    """A referential dependency the loader checks before the upsert.

    The database enforces these anyway, but a violation there aborts the whole
    statement and takes the good rows down with the bad one. Checking in
    staging lets the offending rows be quarantined individually.
    """

    columns: tuple[str, ...]
    parent_table: str
    parent_columns: tuple[str, ...]
    # NULL is a legitimate value for an optional reference (a top-level
    # category has no parent), so those rows are exempt from the check.
    nullable: bool = False


@dataclass(frozen=True)
class EntitySpec:
    """How one CSV maps onto one table."""

    name: str
    filename: str
    table: str

    # Target columns loaded, in COPY order.
    columns: tuple[str, ...]

    # Source CSV column -> target column, where the names differ.
    rename: dict[str, str] = field(default_factory=dict)

    # Columns forming the ON CONFLICT target. Must match a unique index; for
    # partitioned tables that means the partition key is included.
    conflict_key: tuple[str, ...] = ()

    # Columns overwritten when a conflicting row already exists. Anything not
    # listed keeps its existing value (e.g. created_at).
    update_columns: tuple[str, ...] = ()

    # Source columns read but not loaded -- used for validation or
    # cross-checking only.
    extra_source_columns: tuple[str, ...] = ()

    # Column carrying the incremental watermark, if the entity supports one.
    watermark_column: str | None = None

    # Entities that must be loaded first, to satisfy foreign keys.
    depends_on: tuple[str, ...] = ()

    # References validated against staging before the upsert.
    foreign_keys: tuple[ForeignKey, ...] = ()

    @property
    def source_columns(self) -> tuple[str, ...]:
        """Column names as they appear in the CSV header."""
        reverse = {v: k for k, v in self.rename.items()}
        return tuple(reverse.get(c, c) for c in self.columns) + self.extra_source_columns


PRODUCT_CATEGORIES = EntitySpec(
    name="product_categories",
    filename="product_categories.csv",
    table="product_categories",
    columns=("category_id", "name", "description", "parent_id"),
    conflict_key=("category_id",),
    update_columns=("name", "description", "parent_id"),
    # Self-referencing: a subcategory's parent may be elsewhere in the same
    # batch, so the check considers staging as well as the target.
    foreign_keys=(
        ForeignKey(("parent_id",), "product_categories", ("category_id",), nullable=True),
    ),
)

PRODUCTS = EntitySpec(
    name="products",
    filename="products.csv",
    table="products",
    columns=(
        "product_id", "name", "description", "price", "cost",
        "category_id", "sku", "inventory_count", "weight", "is_active",
    ),
    conflict_key=("product_id",),
    update_columns=(
        "name", "description", "price", "cost", "category_id",
        "sku", "inventory_count", "weight", "is_active",
    ),
    depends_on=("product_categories",),
    foreign_keys=(
        ForeignKey(("category_id",), "product_categories", ("category_id",)),
    ),
)

CUSTOMERS = EntitySpec(
    name="customers",
    filename="customers.csv",
    table="customers",
    columns=(
        "customer_id", "email", "first_name", "last_name", "street_address",
        "city", "state", "zip_code", "country", "phone",
        "registration_date", "last_login",
    ),
    conflict_key=("customer_id",),
    update_columns=(
        "email", "first_name", "last_name", "street_address", "city", "state",
        "zip_code", "country", "phone", "registration_date", "last_login",
    ),
    watermark_column="registration_date",
)

ORDERS = EntitySpec(
    name="orders",
    filename="orders.csv",
    table="orders",
    columns=(
        "order_id", "customer_id", "order_date", "status", "payment_method",
        "shipping_address", "shipping_city", "shipping_state", "shipping_zip",
        "shipping_country", "processing_date", "shipping_date",
        "delivery_date", "total_amount",
    ),
    # The partition key must be part of the conflict target on a partitioned
    # table, which is exactly why the primary key is composite.
    conflict_key=("order_id", "order_date"),
    update_columns=(
        "customer_id", "status", "payment_method", "shipping_address",
        "shipping_city", "shipping_state", "shipping_zip", "shipping_country",
        "processing_date", "shipping_date", "delivery_date", "total_amount",
    ),
    watermark_column="order_date",
    depends_on=("customers",),
    foreign_keys=(
        ForeignKey(("customer_id",), "customers", ("customer_id",)),
    ),
)

ORDER_ITEMS = EntitySpec(
    name="order_items",
    filename="order_items.csv",
    table="order_items",
    # line_revenue is GENERATED ALWAYS and must not be written to.
    columns=(
        "order_item_id", "order_id", "order_date", "product_id",
        "quantity", "unit_price", "discount",
    ),
    rename={"price": "unit_price"},
    # `total` is read to cross-check the generated line_revenue, not loaded.
    extra_source_columns=("total",),
    conflict_key=("order_item_id", "order_date"),
    update_columns=("order_id", "product_id", "quantity", "unit_price", "discount"),
    watermark_column="order_date",
    depends_on=("orders", "products"),
    foreign_keys=(
        # Composite, matching the orders primary key.
        ForeignKey(("order_id", "order_date"), "orders", ("order_id", "order_date")),
        ForeignKey(("product_id",), "products", ("product_id",)),
    ),
)

# Load order matters: foreign keys are enforced, so parents come first.
ENTITIES: tuple[EntitySpec, ...] = (
    PRODUCT_CATEGORIES,
    PRODUCTS,
    CUSTOMERS,
    ORDERS,
    ORDER_ITEMS,
)

ENTITIES_BY_NAME = {spec.name: spec for spec in ENTITIES}
