#!/usr/bin/env python3
"""Generate the e-commerce CSV fixtures.

Replaces the ``data-generator.py`` supplied with the brief, which could not
finish at the stated scale. Its inner loop did

    customers_df[customers_df['customer_id'] == customer_id]['street_address']

four times per order, inside a loop over 5,000,000 orders, against a
1,000,000-row DataFrame -- a full linear scan each time. It also accumulated
all 20,000,000 order-item dicts in memory before writing anything, and drew
1,000,000 values from ``fake.unique.email()``, whose retry pool degrades badly
at that size.

This version:

* is vectorised with numpy -- attribute lookups are array indexing by id
  rather than DataFrame scans;
* streams orders and order items to disk in bounded chunks, so peak memory is
  set by ``--chunk-size`` and not by the total row count;
* builds emails as ``first.last<id>@domain``, unique by construction;
* is deterministic for a given ``--seed`` and ``--end-date``;
* emits data that satisfies the schema's CHECK constraints (positive
  quantities, discount within the line total, monotonic fulfilment dates) so a
  load failure means a real pipeline bug rather than a bad fixture.

Usage
-----
    python scripts/generate_data.py --scale sample --output data/sample
    python scripts/generate_data.py --scale full   --output data/full
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

try:
    from faker import Faker
except ImportError:  # pragma: no cover - dependency guard
    print(
        "faker is required: pip install -e '.[datagen]'",
        file=sys.stderr,
    )
    raise SystemExit(1)

logger = logging.getLogger("generate_data")

SECONDS_PER_DAY = 86_400

# Must match the enum values in migrations/001_extensions_and_types.sql.
PAYMENT_METHODS = ["Credit Card", "PayPal", "Apple Pay", "Google Pay", "Gift Card"]
PAYMENT_WEIGHTS = [0.40, 0.25, 0.15, 0.12, 0.08]

MAIN_CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Beauty", "Sports & Outdoors",
    "Books", "Toys", "Automotive", "Health", "Grocery", "Pet Supplies",
    "Office Products", "Garden", "Baby", "Furniture", "Industrial",
    "Music", "Movies", "Art", "Jewelry", "Handmade",
]

# Line items per order. The brief's own figures -- 5M orders against 20M order
# items -- imply a mean of 4 lines per order, so the distribution is centred
# there (mean 4.10) rather than on the single-item basket the reference
# generator produced (mean 1.88, which would yield only ~9.4M items).
ITEMS_PER_ORDER = np.array([1, 2, 3, 4, 5, 6, 7, 8])
ITEMS_PER_ORDER_P = np.array([0.08, 0.12, 0.18, 0.22, 0.18, 0.12, 0.06, 0.04])

# Units per line stay long-tailed: most lines are a single unit.
UNITS_PER_ITEM = np.array([1, 2, 3, 4, 5])
UNITS_PER_ITEM_P = np.array([0.70, 0.15, 0.08, 0.05, 0.02])
DISCOUNT_PCT = np.array([0, 5, 10, 15, 20])
DISCOUNT_PCT_P = np.array([0.80, 0.10, 0.05, 0.03, 0.02])


@dataclass(frozen=True)
class Scale:
    categories: int
    products: int
    customers: int
    orders: int
    order_items: int


SCALES = {
    # Small enough for the test suite to generate and validate in-process.
    "tiny": Scale(categories=30, products=50, customers=100,
                  orders=300, order_items=2_000),
    # Committed to the repository and used by the demo and the tests.
    "sample": Scale(categories=500, products=1_000, customers=2_000,
                    orders=8_000, order_items=40_000),
    # Mid-size, for local performance work without waiting for the full set.
    "medium": Scale(categories=500, products=20_000, customers=50_000,
                    orders=250_000, order_items=1_000_000),
    # The scale quoted in the brief (~1GB).
    "full": Scale(categories=500, products=100_000, customers=1_000_000,
                  orders=5_000_000, order_items=20_000_000),
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _iso(epoch_seconds: np.ndarray) -> np.ndarray:
    """Vectorised epoch-seconds -> 'YYYY-MM-DDTHH:MM:SS' strings.

    Converting via numpy's datetime64 is an order of magnitude faster than
    calling strftime per row, which matters at 20M rows.
    """
    return epoch_seconds.astype("datetime64[s]").astype(str)


def _choice(rng: np.random.Generator, values: np.ndarray, probs: np.ndarray,
            size: int) -> np.ndarray:
    return rng.choice(values, size=size, p=probs)


def _round_money(values: np.ndarray) -> np.ndarray:
    """Round to cents, away from zero at the halfway point."""
    return np.round(values + 1e-9, 2)


def _build_pools(fake: Faker, rng: np.random.Generator) -> dict[str, np.ndarray]:
    """Pre-draw value pools once, then sample them by index.

    Faker is comparatively slow per call. Drawing a few thousand values up
    front and sampling from them keeps the distributions realistic while
    turning per-row generation into array indexing.
    """
    logger.info("building value pools")
    return {
        "first_name": np.array([fake.first_name() for _ in range(2_000)]),
        "last_name": np.array([fake.last_name() for _ in range(2_000)]),
        "street": np.array([fake.street_address() for _ in range(5_000)]),
        "city": np.array([fake.city() for _ in range(2_000)]),
        "state": np.array([fake.state_abbr() for _ in range(60)]),
        "zip": np.array([fake.zipcode() for _ in range(5_000)]),
        "word": np.array([fake.word().capitalize() for _ in range(1_000)]),
    }


# ---------------------------------------------------------------------------
# generators
# ---------------------------------------------------------------------------


def generate_categories(scale: Scale, fake: Faker, rng: np.random.Generator,
                        out: Path) -> np.ndarray:
    """Write product_categories.csv. Returns the category id array."""
    logger.info("generating %d categories", scale.categories)
    n_main = min(len(MAIN_CATEGORIES), scale.categories)
    rows = []

    for i in range(scale.categories):
        category_id = i + 1
        if i < n_main:
            name = MAIN_CATEGORIES[i]
            parent_id = ""
        else:
            # Subcategories hang off a top-level category, so the hierarchy is
            # exactly two levels deep and can never form a cycle.
            parent_index = int(rng.integers(0, n_main))
            name = f"{MAIN_CATEGORIES[parent_index]} - {fake.word().capitalize()}"
            parent_id = parent_index + 1
        rows.append({
            "category_id": category_id,
            "name": name[:100],
            "description": fake.sentence()[:200],
            "parent_id": parent_id,
        })

    _write_csv(out / "product_categories.csv",
               ["category_id", "name", "description", "parent_id"], rows)
    return np.arange(1, scale.categories + 1, dtype=np.int32)


def generate_products(scale: Scale, pools: dict, rng: np.random.Generator,
                      category_ids: np.ndarray, out: Path) -> np.ndarray:
    """Write products.csv. Returns the per-product price array."""
    logger.info("generating %d products", scale.products)
    n = scale.products

    product_ids = np.arange(1, n + 1, dtype=np.int32)
    cat = rng.choice(category_ids, size=n)
    price = _round_money(rng.uniform(5.99, 499.99, size=n))
    # Cost is 40-80% of price, so margin is always positive.
    cost = _round_money(price * rng.uniform(0.40, 0.80, size=n))
    inventory = rng.integers(0, 501, size=n)
    weight = _round_money(rng.uniform(0.10, 20.0, size=n))
    is_active = rng.random(size=n) > 0.10

    w1 = rng.choice(pools["word"], size=n)
    w2 = rng.choice(pools["word"], size=n)

    # SKUs must be unique: the product id guarantees it, the random prefix
    # keeps them looking like real codes.
    prefix = rng.choice(np.array(list("ABCDEFGHJKLMNPQRSTUVWXYZ")), size=(n, 2))

    with (out / "products.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["product_id", "name", "description", "price", "cost",
                         "category_id", "sku", "inventory_count", "weight",
                         "is_active"])
        for i in range(n):
            writer.writerow([
                product_ids[i],
                f"{w1[i]} {w2[i]} {product_ids[i]}"[:255],
                f"Quality {w2[i].lower()} for everyday use.",
                f"{price[i]:.2f}",
                f"{cost[i]:.2f}",
                cat[i],
                f"SKU-{prefix[i, 0]}{prefix[i, 1]}-{product_ids[i]:07d}",
                inventory[i],
                f"{weight[i]:.2f}",
                "true" if is_active[i] else "false",
            ])

    return price


def generate_customers(scale: Scale, pools: dict, rng: np.random.Generator,
                       end_ts: int, out: Path) -> np.ndarray:
    """Write customers.csv. Returns registration timestamps by customer index."""
    logger.info("generating %d customers", scale.customers)
    n = scale.customers

    # Registration skewed towards recent dates, within the last 5 years, and
    # never before 2019 so every order lands in a declared partition.
    earliest = int(datetime(2019, 6, 1, tzinfo=timezone.utc).timestamp())
    span = min(5 * 365 * SECONDS_PER_DAY, end_ts - earliest)
    days_ago = (rng.power(0.5, size=n) * span).astype(np.int64)
    registration = end_ts - days_ago

    # Last login falls between registration and now.
    last_login = registration + (rng.random(size=n) * (end_ts - registration)).astype(np.int64)

    first = rng.choice(pools["first_name"], size=n)
    last = rng.choice(pools["last_name"], size=n)
    street = rng.choice(pools["street"], size=n)
    city = rng.choice(pools["city"], size=n)
    state = rng.choice(pools["state"], size=n)
    zips = rng.choice(pools["zip"], size=n)

    reg_iso = _iso(registration)
    login_iso = _iso(last_login)

    with (out / "customers.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["customer_id", "email", "first_name", "last_name",
                         "street_address", "city", "state", "zip_code",
                         "country", "phone", "registration_date", "last_login"])
        for i in range(n):
            customer_id = i + 1
            # Unique by construction -- no retry pool, no exhaustion.
            email = f"{first[i]}.{last[i]}{customer_id}@example.com".lower()
            writer.writerow([
                customer_id, email, first[i], last[i], street[i], city[i],
                state[i], zips[i], "US",
                f"555-{customer_id % 10000:04d}",
                reg_iso[i], login_iso[i],
            ])

    return registration


def generate_orders_and_items(
    scale: Scale,
    pools: dict,
    rng: np.random.Generator,
    registration: np.ndarray,
    prices: np.ndarray,
    end_ts: int,
    chunk_size: int,
    out: Path,
) -> tuple[int, int]:
    """Stream orders.csv and order_items.csv. Returns (orders, items) written.

    Orders are produced in chunks: within each chunk every column is computed
    as a whole-array operation, then the rows are appended to disk and the
    arrays released. Peak memory tracks chunk_size, not scale.orders.
    """
    logger.info("generating up to %d orders / %d items",
                scale.orders, scale.order_items)

    n_customers = scale.customers

    # Per-customer propensity to order follows a Pareto distribution, so a
    # minority of customers account for most orders. Capped so no single
    # customer dominates, and 5% get weight 0 -- registered, never ordered.
    weights = (rng.pareto(1.5, size=n_customers) + 1).astype(np.float64)
    np.clip(weights, 1, 50, out=weights)
    weights[rng.random(size=n_customers) < 0.05] = 0.0

    # Sample one customer per order using those weights, rather than repeating
    # each customer id `weights[i]` times. Repeating makes the total order
    # count a property of the distribution -- it undershot the requested
    # 8,000 by nearly half -- whereas sampling produces exactly scale.orders
    # while preserving the same skew.
    total_orders = scale.orders
    customer_for_order = rng.choice(
        np.arange(1, n_customers + 1, dtype=np.int32),
        size=total_orders,
        replace=True,
        p=weights / weights.sum(),
    )

    order_fields = ["order_id", "customer_id", "order_date", "status",
                    "payment_method", "shipping_address", "shipping_city",
                    "shipping_state", "shipping_zip", "shipping_country",
                    "processing_date", "shipping_date", "delivery_date",
                    "total_amount"]
    item_fields = ["order_item_id", "order_id", "order_date", "product_id",
                   "quantity", "price", "discount", "total"]

    orders_path = out / "orders.csv"
    items_path = out / "order_items.csv"

    orders_written = 0
    items_written = 0
    next_item_id = 1

    with orders_path.open("w", newline="", encoding="utf-8") as of, \
         items_path.open("w", newline="", encoding="utf-8") as itf:
        owriter = csv.writer(of)
        iwriter = csv.writer(itf)
        owriter.writerow(order_fields)
        iwriter.writerow(item_fields)

        start = 0
        while start < total_orders and items_written < scale.order_items:
            stop = min(start + chunk_size, total_orders)
            n = stop - start
            order_ids = np.arange(start + 1, stop + 1, dtype=np.int64)
            customers = customer_for_order[start:stop]

            # --- order timing -------------------------------------------
            # Uniform within [registration, now), skewed recent, so an order
            # can never precede the customer's registration.
            reg = registration[customers - 1]
            frac = rng.power(0.7, size=n)
            order_ts = (end_ts - (frac * (end_ts - reg))).astype(np.int64)

            processing = order_ts + rng.integers(0, 3, size=n) * SECONDS_PER_DAY
            shipping = processing + rng.integers(1, 8, size=n) * SECONDS_PER_DAY
            delivery = shipping + rng.integers(1, 4, size=n) * SECONDS_PER_DAY

            # --- line items ---------------------------------------------
            counts = _choice(rng, ITEMS_PER_ORDER, ITEMS_PER_ORDER_P, n).astype(np.int64)

            # Respect the global item cap without truncating an order midway:
            # keep only whole orders whose items fit in the remaining budget.
            remaining = scale.order_items - items_written
            if counts.sum() > remaining:
                keep = np.searchsorted(np.cumsum(counts), remaining, side="right")
                if keep == 0:
                    break
                n = int(keep)
                stop = start + n
                order_ids, customers = order_ids[:n], customers[:n]
                order_ts, processing = order_ts[:n], processing[:n]
                shipping, delivery = shipping[:n], delivery[:n]
                counts = counts[:n]

            total_items = int(counts.sum())
            item_order_ids = np.repeat(order_ids, counts)
            item_order_ts = np.repeat(order_ts, counts)

            product_ids = rng.integers(1, scale.products + 1, size=total_items)
            quantity = _choice(rng, UNITS_PER_ITEM, UNITS_PER_ITEM_P, total_items)
            # Historic price drifts slightly from the current catalogue price.
            unit_price = _round_money(
                prices[product_ids - 1] * rng.uniform(0.95, 1.05, size=total_items)
            )
            pct = _choice(rng, DISCOUNT_PCT, DISCOUNT_PCT_P, total_items)
            gross = _round_money(unit_price * quantity)
            discount = _round_money(gross * pct / 100.0)
            # Keeps the schema's chk_order_items_discount_valid satisfied even
            # after independent rounding of both terms.
            np.minimum(discount, gross, out=discount)
            line_total = _round_money(gross - discount)

            # --- order totals from their own lines -----------------------
            # reduceat sums each order's slice in one pass; the reference
            # implementation accumulated this inside a Python loop.
            offsets = np.zeros(len(counts), dtype=np.int64)
            np.cumsum(counts[:-1], out=offsets[1:])
            totals = np.add.reduceat(line_total, offsets) if total_items else np.zeros(len(counts))

            # --- status from the fulfilment timeline ---------------------
            status = np.full(len(order_ids), "Delivered", dtype=object)
            status[delivery > end_ts] = "In Transit"
            status[shipping > end_ts] = "Shipped"
            status[processing > end_ts] = "Processing"
            status[order_ts > end_ts] = "Pending"
            # A small share of completed orders is cancelled or returned, so
            # the revenue-exclusion logic has something to exclude.
            roll = rng.random(size=len(order_ids))
            status[(roll < 0.03)] = "Cancelled"
            status[(roll >= 0.03) & (roll < 0.05)] = "Returned"

            payment = _choice(rng, np.array(PAYMENT_METHODS),
                              np.array(PAYMENT_WEIGHTS), len(order_ids))
            street = rng.choice(pools["street"], size=len(order_ids))
            city = rng.choice(pools["city"], size=len(order_ids))
            state = rng.choice(pools["state"], size=len(order_ids))
            zips = rng.choice(pools["zip"], size=len(order_ids))

            order_iso = _iso(order_ts)
            proc_iso = _iso(processing)
            ship_iso = _iso(shipping)
            deliv_iso = _iso(delivery)
            item_order_iso = _iso(item_order_ts)

            owriter.writerows(
                (
                    order_ids[i], customers[i], order_iso[i], status[i],
                    payment[i], street[i], city[i], state[i], zips[i], "US",
                    proc_iso[i], ship_iso[i], deliv_iso[i], f"{totals[i]:.2f}",
                )
                for i in range(len(order_ids))
            )
            iwriter.writerows(
                (
                    next_item_id + i, item_order_ids[i], item_order_iso[i],
                    product_ids[i], quantity[i], f"{unit_price[i]:.2f}",
                    f"{discount[i]:.2f}", f"{line_total[i]:.2f}",
                )
                for i in range(total_items)
            )

            orders_written += len(order_ids)
            items_written += total_items
            next_item_id += total_items
            start = stop

            logger.info("  %d orders / %d items written", orders_written, items_written)

    return orders_written, items_written


def _write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scale", choices=sorted(SCALES), default="sample")
    parser.add_argument("--output", type=Path, default=Path("data/sample"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--end-date",
        type=lambda s: date.fromisoformat(s),
        default=date(2026, 6, 30),
        help="latest possible order date; fixed by default so output is reproducible",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=250_000,
        help="orders generated per chunk; bounds peak memory",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    scale = SCALES[args.scale]
    args.output.mkdir(parents=True, exist_ok=True)

    end_ts = int(datetime.combine(
        args.end_date, datetime.min.time(), tzinfo=timezone.utc
    ).timestamp())

    fake = Faker()
    Faker.seed(args.seed)
    rng = np.random.default_rng(args.seed)

    started = time.perf_counter()
    pools = _build_pools(fake, rng)

    category_ids = generate_categories(scale, fake, rng, args.output)
    prices = generate_products(scale, pools, rng, category_ids, args.output)
    registration = generate_customers(scale, pools, rng, end_ts, args.output)
    orders, items = generate_orders_and_items(
        scale, pools, rng, registration, prices, end_ts, args.chunk_size, args.output
    )

    elapsed = time.perf_counter() - started
    logger.info("done in %.1fs -> %s", elapsed, args.output)
    logger.info(
        "categories=%d products=%d customers=%d orders=%d order_items=%d",
        scale.categories, scale.products, scale.customers, orders, items,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
