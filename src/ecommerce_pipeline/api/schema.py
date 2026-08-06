"""GraphQL schema: queries, the mutation, and query-cost guards."""

from __future__ import annotations

import base64
from datetime import date, datetime, timedelta

import psycopg
import strawberry
from strawberry.extensions import MaxAliasesLimiter, QueryDepthLimiter

from ..config import get_settings
from ..logging_config import get_logger
from . import repository
from .types import (
    Customer,
    CustomerFilter,
    CustomerPage,
    CustomerSort,
    DateRange,
    Granularity,
    Order,
    OrderCursorPage,
    OrderStatus,
    PageInfo,
    PageInput,
    Product,
    ProductFilter,
    ProductPage,
    ProductSales,
    ProductSalesPage,
    ProductSalesSort,
    ProductSort,
    RankedProduct,
    TrendPoint,
    UpdateProductInput,
    UpdateProductResult,
)

logger = get_logger(__name__)


def _clamp(page: PageInput | None) -> tuple[int, int]:
    """Bound the page size so one query cannot ask for a whole table."""
    settings = get_settings()
    page = page or PageInput()
    limit = max(1, min(page.limit, settings.api_max_page_size))
    offset = max(0, page.offset)
    return limit, offset


def _default_range(period: DateRange | None) -> tuple[date, date]:
    """Default to the last 90 days.

    An unbounded default would make the cheapest possible query the most
    expensive one to serve.
    """
    if period is not None:
        if period.to_date < period.from_date:
            raise ValueError("toDate must not precede fromDate")
        return period.from_date, period.to_date
    today = date.today()
    return today - timedelta(days=90), today


def _encode_cursor(order_date: datetime, order_id: int) -> str:
    return base64.urlsafe_b64encode(
        f"{order_date.isoformat()}|{order_id}".encode()
    ).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, int]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        timestamp, order_id = raw.rsplit("|", 1)
        return datetime.fromisoformat(timestamp), int(order_id)
    except Exception:
        raise ValueError("malformed cursor") from None


@strawberry.type
class Query:
    # -- 1. product sales by time period ---------------------------------

    @strawberry.field(description="Aggregated sales per product over a period.")
    async def product_sales(
        self,
        period: DateRange | None = None,
        category_id: int | None = None,
        search: str | None = None,
        sort: ProductSalesSort = ProductSalesSort.REVENUE,
        descending: bool = True,
        page: PageInput | None = None,
    ) -> ProductSalesPage:
        date_from, date_to = _default_range(period)
        limit, offset = _clamp(page)

        result = await repository.product_sales(
            date_from=date_from, date_to=date_to, category_id=category_id,
            product_search=search, sort=sort.value, descending=descending,
            limit=limit, offset=offset,
        )
        return ProductSalesPage(
            items=[ProductSales.from_row(r) for r in result.rows],
            page_info=PageInfo(
                total_count=result.total_count, has_next_page=result.has_next_page,
                limit=limit, offset=offset,
            ),
        )

    # -- 2. customer purchase history ------------------------------------

    @strawberry.field(description="A single customer with lifetime metrics.")
    async def customer(self, customer_id: int) -> Customer | None:
        row = await repository.customer_by_id(customer_id)
        return Customer.from_row(row) if row else None

    @strawberry.field(description="Customers, filterable and sortable.")
    async def customers(
        self,
        filter: CustomerFilter | None = None,
        sort: CustomerSort = CustomerSort.LIFETIME_VALUE,
        descending: bool = True,
        page: PageInput | None = None,
    ) -> CustomerPage:
        limit, offset = _clamp(page)
        filter = filter or CustomerFilter()

        result = await repository.customers(
            search=filter.search, country=filter.country,
            min_lifetime_value=filter.min_lifetime_value,
            sort=sort.value, descending=descending, limit=limit, offset=offset,
        )
        return CustomerPage(
            items=[Customer.from_row(r) for r in result.rows],
            page_info=PageInfo(
                total_count=result.total_count, has_next_page=result.has_next_page,
                limit=limit, offset=offset,
            ),
        )

    @strawberry.field(
        description="A customer's orders, newest first, keyset-paginated."
    )
    async def customer_purchase_history(
        self,
        customer_id: int,
        first: int = 20,
        after: str | None = None,
        statuses: list[OrderStatus] | None = None,
    ) -> OrderCursorPage:
        settings = get_settings()
        first = max(1, min(first, settings.api_max_page_size))

        after_date, after_id = (None, None)
        if after:
            after_date, after_id = _decode_cursor(after)

        rows, has_next = await repository.customer_orders(
            customer_id=customer_id, first=first,
            after_date=after_date, after_id=after_id,
            statuses=[s.value for s in statuses] if statuses else None,
        )

        end_cursor = (
            _encode_cursor(rows[-1]["order_date"], rows[-1]["order_id"]) if rows else None
        )
        return OrderCursorPage(
            items=[Order.from_row(r) for r in rows],
            has_next_page=has_next,
            end_cursor=end_cursor,
        )

    # -- 3. top-selling products by category ------------------------------

    @strawberry.field(description="Top products within each top-level category.")
    async def top_products_by_category(
        self,
        period: DateRange | None = None,
        category_id: int | None = None,
        limit_per_category: int = 5,
    ) -> list[RankedProduct]:
        date_from, date_to = _default_range(period)
        limit_per_category = max(1, min(limit_per_category, 50))

        rows = await repository.top_products_by_category(
            date_from=date_from, date_to=date_to, category_id=category_id,
            limit_per_category=limit_per_category,
        )
        return [RankedProduct.from_row(r) for r in rows]

    # -- 4. sales trends over time ----------------------------------------

    @strawberry.field(description="Revenue and units bucketed over time.")
    async def sales_trends(
        self,
        period: DateRange | None = None,
        granularity: Granularity = Granularity.DAY,
        category_id: int | None = None,
    ) -> list[TrendPoint]:
        date_from, date_to = _default_range(period)
        rows = await repository.sales_trends(
            date_from=date_from, date_to=date_to,
            granularity=granularity.value, category_id=category_id,
        )
        return [TrendPoint.from_row(r) for r in rows]

    # -- catalogue --------------------------------------------------------

    @strawberry.field(description="Products, filterable and sortable.")
    async def products(
        self,
        filter: ProductFilter | None = None,
        sort: ProductSort = ProductSort.NAME,
        descending: bool = False,
        page: PageInput | None = None,
    ) -> ProductPage:
        limit, offset = _clamp(page)
        filter = filter or ProductFilter()

        result = await repository.products(
            search=filter.search, category_id=filter.category_id,
            active_only=filter.active_only, min_price=filter.min_price,
            max_price=filter.max_price, sort=sort.value, descending=descending,
            limit=limit, offset=offset,
        )
        return ProductPage(
            items=[Product.from_row(r) for r in result.rows],
            page_info=PageInfo(
                total_count=result.total_count, has_next_page=result.has_next_page,
                limit=limit, offset=offset,
            ),
        )

    @strawberry.field
    async def product(self, product_id: int) -> Product | None:
        row = await repository.product_by_id(product_id)
        return Product.from_row(row) if row else None


@strawberry.type
class Mutation:
    @strawberry.mutation(
        description="Update a product. Omitted fields are left unchanged."
    )
    async def update_product(
        self, product_id: int, input: UpdateProductInput
    ) -> UpdateProductResult:
        changes = {
            "name": input.name,
            "description": input.description,
            "price": input.price,
            "cost": input.cost,
            "category_id": input.category_id,
            "sku": input.sku,
            "inventory_count": input.inventory_count,
            "weight": input.weight,
            "is_active": input.is_active,
        }

        try:
            row = await repository.update_product(product_id, changes)
        except repository.NoFieldsToUpdate:
            return UpdateProductResult(
                product=None, success=False, message="No updatable fields supplied.",
            )
        except repository.ProductNotFound:
            return UpdateProductResult(
                product=None, success=False,
                message=f"No product with id {product_id}.",
            )
        except psycopg.errors.CheckViolation as exc:
            # A raw psycopg error carries a DETAIL line containing every column
            # value of the offending row, so it must not be echoed to the
            # client. The constraint name alone tells the caller what to fix.
            logger.warning("update_product(%d) violated a constraint: %s", product_id, exc)
            return UpdateProductResult(
                product=None, success=False,
                message=f"Update rejected: {exc.diag.constraint_name or 'check constraint'}.",
            )
        except psycopg.errors.UniqueViolation as exc:
            logger.warning("update_product(%d) duplicate value: %s", product_id, exc)
            return UpdateProductResult(
                product=None, success=False,
                message="Update rejected: that SKU is already in use.",
            )
        except psycopg.errors.ForeignKeyViolation as exc:
            logger.warning("update_product(%d) bad reference: %s", product_id, exc)
            return UpdateProductResult(
                product=None, success=False,
                message="Update rejected: the referenced category does not exist.",
            )
        except Exception:
            # Anything unanticipated is logged in full but reported opaquely.
            logger.exception("update_product(%d) failed", product_id)
            return UpdateProductResult(
                product=None, success=False,
                message="Update failed due to an internal error.",
            )

        return UpdateProductResult(
            product=Product.from_row(row), success=True, message="Product updated.",
        )


schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    extensions=[
        # GraphQL lets a client nest its way into an arbitrarily expensive
        # query. These cap the blast radius of a hostile or careless document
        # before any resolver runs. Passed as factories so each request gets a
        # fresh instance rather than sharing mutable extension state.
        lambda: QueryDepthLimiter(max_depth=12),
        lambda: MaxAliasesLimiter(max_alias_count=30),
    ],
)
