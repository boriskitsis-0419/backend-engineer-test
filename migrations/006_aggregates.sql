-- Derived aggregate tables: daily sales rollups and customer lifetime metrics.
--
-- The reference schema created daily_sales_aggregation inside a plpgsql
-- function body, then declared indexes on it at the top level. Those index
-- statements referenced a relation that did not exist at load time and failed
-- with "relation daily_sales_aggregation does not exist". The table is DDL and
-- belongs in a migration; only the refresh logic belongs in a function.

-- ---------------------------------------------------------------------------
-- daily_sales_aggregation  (transformation rule 4)
-- ---------------------------------------------------------------------------
-- Pre-aggregating by (day, product) collapses ~20M line items into roughly one
-- row per product per selling day, which is what the "sales trends" and
-- "top sellers" queries actually read. Partitioned monthly for the same
-- retention and pruning reasons as the fact tables.
CREATE TABLE daily_sales_aggregation (
    sale_date      DATE           NOT NULL,
    date_key       INTEGER        NOT NULL,
    product_id     INTEGER        NOT NULL,
    category_id    INTEGER        NOT NULL,
    units_sold     BIGINT         NOT NULL DEFAULT 0,
    gross_revenue  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    discount_total NUMERIC(14, 2) NOT NULL DEFAULT 0,
    net_revenue    NUMERIC(14, 2) NOT NULL DEFAULT 0,
    order_count    INTEGER        NOT NULL DEFAULT 0,
    avg_unit_price NUMERIC(10, 2) NOT NULL DEFAULT 0,
    refreshed_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_daily_sales PRIMARY KEY (sale_date, product_id)
) PARTITION BY RANGE (sale_date);

COMMENT ON TABLE daily_sales_aggregation IS
    'Daily sales rolled up by product and category. Rebuilt per date range by '
    'refresh_daily_sales(), which is idempotent.';

-- DATE partition key, so the boundary literals carry no time or offset.
SELECT ensure_monthly_partitions('daily_sales_aggregation',
                                 '2019-01-01'::date, '2030-12-31'::date, '');

CREATE TABLE daily_sales_aggregation_default
    PARTITION OF daily_sales_aggregation DEFAULT;

CREATE INDEX idx_daily_sales_category_date
    ON daily_sales_aggregation (category_id, sale_date);
CREATE INDEX idx_daily_sales_product_date
    ON daily_sales_aggregation (product_id, sale_date);
CREATE INDEX idx_daily_sales_date_key
    ON daily_sales_aggregation (date_key);

-- Rebuilds the rollup for a closed date range. DELETE + INSERT over the range
-- rather than an upsert, so rows for products that stopped selling are removed
-- instead of lingering with stale figures. Safe to re-run for any range.
CREATE FUNCTION refresh_daily_sales(p_from DATE, p_to DATE)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    IF p_to < p_from THEN
        RAISE EXCEPTION 'refresh_daily_sales: end % precedes start %', p_to, p_from;
    END IF;

    DELETE FROM daily_sales_aggregation
    WHERE sale_date BETWEEN p_from AND p_to;

    INSERT INTO daily_sales_aggregation (
        sale_date, date_key, product_id, category_id,
        units_sold, gross_revenue, discount_total, net_revenue,
        order_count, avg_unit_price
    )
    SELECT
        d.sale_date,
        to_char(d.sale_date, 'YYYYMMDD')::int,
        d.product_id,
        p.category_id,
        d.units_sold,
        d.gross_revenue,
        d.discount_total,
        d.net_revenue,
        d.order_count,
        -- Guard against a zero denominator even though quantity > 0 is a
        -- CHECK constraint; a defensive NULLIF costs nothing here.
        ROUND(d.gross_revenue / NULLIF(d.units_sold, 0), 2)
    FROM (
        SELECT
            (oi.order_date AT TIME ZONE 'UTC')::date       AS sale_date,
            oi.product_id,
            SUM(oi.quantity)                               AS units_sold,
            SUM(oi.unit_price * oi.quantity)               AS gross_revenue,
            SUM(oi.discount)                               AS discount_total,
            SUM(oi.line_revenue)                           AS net_revenue,
            COUNT(DISTINCT oi.order_id)                    AS order_count
        FROM order_items oi
        JOIN orders o
          ON o.order_id = oi.order_id
         AND o.order_date = oi.order_date
        WHERE oi.order_date >= p_from::timestamptz
          AND oi.order_date < (p_to + 1)::timestamptz
          AND is_revenue_bearing(o.status)
        GROUP BY 1, 2
    ) d
    JOIN products p ON p.product_id = d.product_id;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;

-- ---------------------------------------------------------------------------
-- customer_metrics  (transformation rule 3)
-- ---------------------------------------------------------------------------
-- Derived attributes live in their own table rather than as columns on
-- customers: they are recomputed by the ETL on a different cadence from the
-- customer master, and keeping them separate means a metrics refresh never
-- contends with, or dirties updated_at on, the source-of-truth rows.
CREATE TABLE customer_metrics (
    customer_id            INTEGER PRIMARY KEY REFERENCES customers (customer_id) ON DELETE CASCADE,
    total_orders           INTEGER        NOT NULL DEFAULT 0,
    lifetime_value         NUMERIC(14, 2) NOT NULL DEFAULT 0,
    avg_order_value        NUMERIC(12, 2) NOT NULL DEFAULT 0,
    first_order_date       TIMESTAMPTZ,
    last_order_date        TIMESTAMPTZ,
    -- Mean gap in days between consecutive orders. The reference view divided
    -- the full span by the order count and returned an interval labelled
    -- "days"; the correct denominator is (orders - 1), and NULL is the honest
    -- answer for a single-order customer.
    avg_days_between_orders NUMERIC(10, 2),
    refreshed_at           TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE customer_metrics IS
    'Per-customer derived attributes (lifetime value, order cadence). '
    'Refreshed by refresh_customer_metrics().';

CREATE INDEX idx_customer_metrics_lifetime_value
    ON customer_metrics (lifetime_value DESC);
CREATE INDEX idx_customer_metrics_last_order
    ON customer_metrics (last_order_date DESC NULLS LAST);

-- Recomputes metrics. Passing a customer_id array restricts the refresh to
-- customers touched by the current batch, which is what the incremental ETL
-- does; passing NULL rebuilds everything.
CREATE FUNCTION refresh_customer_metrics(p_customer_ids BIGINT[] DEFAULT NULL)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows INTEGER;
BEGIN
    INSERT INTO customer_metrics AS cm (
        customer_id, total_orders, lifetime_value, avg_order_value,
        first_order_date, last_order_date, avg_days_between_orders, refreshed_at
    )
    SELECT
        c.customer_id,
        COALESCE(agg.total_orders, 0),
        COALESCE(agg.lifetime_value, 0),
        COALESCE(agg.avg_order_value, 0),
        agg.first_order_date,
        agg.last_order_date,
        CASE
            WHEN COALESCE(agg.total_orders, 0) > 1 THEN
                ROUND(
                    EXTRACT(EPOCH FROM (agg.last_order_date - agg.first_order_date))::numeric
                    / 86400 / (agg.total_orders - 1),
                    2
                )
            ELSE NULL
        END,
        NOW()
    FROM customers c
    LEFT JOIN (
        SELECT
            o.customer_id,
            COUNT(*)               AS total_orders,
            SUM(o.total_amount)    AS lifetime_value,
            ROUND(AVG(o.total_amount), 2) AS avg_order_value,
            MIN(o.order_date)      AS first_order_date,
            MAX(o.order_date)      AS last_order_date
        FROM orders o
        WHERE is_revenue_bearing(o.status)
          AND (p_customer_ids IS NULL OR o.customer_id = ANY (p_customer_ids))
        GROUP BY o.customer_id
    ) agg ON agg.customer_id = c.customer_id
    WHERE p_customer_ids IS NULL OR c.customer_id = ANY (p_customer_ids)
    ON CONFLICT (customer_id) DO UPDATE SET
        total_orders            = EXCLUDED.total_orders,
        lifetime_value          = EXCLUDED.lifetime_value,
        avg_order_value         = EXCLUDED.avg_order_value,
        first_order_date        = EXCLUDED.first_order_date,
        last_order_date         = EXCLUDED.last_order_date,
        avg_days_between_orders = EXCLUDED.avg_days_between_orders,
        refreshed_at            = EXCLUDED.refreshed_at;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END;
$$;
