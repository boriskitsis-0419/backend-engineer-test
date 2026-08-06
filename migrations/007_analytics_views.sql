-- Analytics views serving the GraphQL queries.

-- ---------------------------------------------------------------------------
-- mv_product_sales_summary
-- ---------------------------------------------------------------------------
-- The reference version aggregated straight off order_items joined to orders,
-- i.e. a full pass over the largest table in the database on every refresh.
-- This one reads daily_sales_aggregation instead, which is already rolled up
-- by (day, product), so the refresh cost scales with selling-days x products
-- rather than with raw line items.
CREATE MATERIALIZED VIEW mv_product_sales_summary AS
SELECT
    p.product_id,
    p.name                      AS product_name,
    p.sku,
    p.category_id,
    pc.name                     AS category_name,
    ch.root_category_id,
    ch.root_category_name,
    COALESCE(SUM(ds.units_sold), 0)     AS total_units_sold,
    COALESCE(SUM(ds.net_revenue), 0)    AS total_revenue,
    COALESCE(SUM(ds.discount_total), 0) AS total_discount,
    COALESCE(SUM(ds.order_count), 0)    AS order_count,
    MIN(ds.sale_date)                   AS first_sale_date,
    MAX(ds.sale_date)                   AS last_sale_date
FROM products p
JOIN product_categories pc ON pc.category_id = p.category_id
LEFT JOIN category_hierarchy ch ON ch.category_id = p.category_id
LEFT JOIN daily_sales_aggregation ds ON ds.product_id = p.product_id
GROUP BY
    p.product_id, p.name, p.sku, p.category_id,
    pc.name, ch.root_category_id, ch.root_category_name
WITH NO DATA;

COMMENT ON MATERIALIZED VIEW mv_product_sales_summary IS
    'All-time sales totals per product. Refreshed by the ETL after each load.';

-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY, which lets readers keep
-- querying the previous contents while the refresh runs.
CREATE UNIQUE INDEX idx_mv_product_sales_summary_product
    ON mv_product_sales_summary (product_id);

CREATE INDEX idx_mv_product_sales_summary_category_revenue
    ON mv_product_sales_summary (category_id, total_revenue DESC);
CREATE INDEX idx_mv_product_sales_summary_root_category_revenue
    ON mv_product_sales_summary (root_category_id, total_revenue DESC);

-- ---------------------------------------------------------------------------
-- v_customer_purchase_summary
-- ---------------------------------------------------------------------------
-- A plain view is enough here: customer_metrics is already materialised by the
-- ETL, so this only joins it back to the customer master.
CREATE VIEW v_customer_purchase_summary AS
SELECT
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.country,
    c.state,
    c.city,
    c.registration_date,
    COALESCE(m.total_orders, 0)    AS order_count,
    COALESCE(m.lifetime_value, 0)  AS lifetime_value,
    COALESCE(m.avg_order_value, 0) AS avg_order_value,
    m.first_order_date,
    m.last_order_date,
    m.avg_days_between_orders
FROM customers c
LEFT JOIN customer_metrics m ON m.customer_id = c.customer_id;

COMMENT ON VIEW v_customer_purchase_summary IS
    'Customer master joined to precomputed lifetime metrics.';

-- ---------------------------------------------------------------------------
-- v_daily_category_sales
-- ---------------------------------------------------------------------------
-- Category-level daily trend, rolled up to the top-level category so that
-- "sales by category over time" does not have to walk the hierarchy per row.
CREATE VIEW v_daily_category_sales AS
SELECT
    ds.sale_date,
    ds.date_key,
    ch.root_category_id   AS category_id,
    ch.root_category_name AS category_name,
    SUM(ds.units_sold)    AS units_sold,
    SUM(ds.net_revenue)   AS net_revenue,
    SUM(ds.order_count)   AS order_count
FROM daily_sales_aggregation ds
JOIN category_hierarchy ch ON ch.category_id = ds.category_id
GROUP BY ds.sale_date, ds.date_key, ch.root_category_id, ch.root_category_name;

COMMENT ON VIEW v_daily_category_sales IS
    'Daily sales rolled up to top-level category.';

-- Non-concurrent refresh helper. REFRESH ... CONCURRENTLY cannot run inside a
-- transaction block and therefore cannot live in a plpgsql function; the ETL
-- issues that form directly on an autocommit connection.
CREATE FUNCTION refresh_analytics_views()
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_product_sales_summary;
END;
$$;
