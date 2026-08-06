"""DataLoaders, which exist to stop the N+1 query problem.

A query like

    { productSales(...) { items { product { category { name } } } } }

resolves `category` once per product. Done naively that is one query per row:
50 products means 51 round trips, and the cost grows with page size rather
than staying constant.

A DataLoader defers each request for a key until the end of the event-loop
tick, then issues a single query for every key gathered. The 51 queries become
2. Loaders are created per request rather than per process, so a cached row
cannot leak across requests or serve stale data to a later one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from strawberry.dataloader import DataLoader

from . import repository


def _index_by(rows: list[dict[str, Any]], key: str) -> dict[Any, dict[str, Any]]:
    return {row[key]: row for row in rows}


async def _load_categories(ids: list[int]) -> list[dict[str, Any] | None]:
    by_id = _index_by(await repository.categories_by_ids(ids), "category_id")
    # DataLoader requires the results in the same order as the keys, with None
    # for anything missing -- returning a shorter list silently misaligns them.
    return [by_id.get(i) for i in ids]


async def _load_products(ids: list[int]) -> list[dict[str, Any] | None]:
    by_id = _index_by(await repository.products_by_ids(ids), "product_id")
    return [by_id.get(i) for i in ids]


async def _load_customers(ids: list[int]) -> list[dict[str, Any] | None]:
    by_id = _index_by(await repository.customers_by_ids(ids), "customer_id")
    return [by_id.get(i) for i in ids]


async def _load_order_items(
    keys: list[tuple[int, datetime]]
) -> list[list[dict[str, Any]]]:
    """One-to-many: each key maps to a list, empty rather than None."""
    rows = await repository.order_items_for_orders(keys)

    grouped: dict[tuple[int, datetime], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["order_id"], row["order_date"]), []).append(row)

    return [grouped.get(key, []) for key in keys]


class Loaders:
    """The set of loaders belonging to a single request."""

    def __init__(self) -> None:
        self.category = DataLoader(load_fn=_load_categories)
        self.product = DataLoader(load_fn=_load_products)
        self.customer = DataLoader(load_fn=_load_customers)
        self.order_items = DataLoader(load_fn=_load_order_items)
