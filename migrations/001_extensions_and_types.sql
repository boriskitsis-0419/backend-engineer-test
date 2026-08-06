-- Extensions and enumerated types.
--
-- Timestamps are stored as TIMESTAMPTZ throughout and the database runs in UTC
-- (see 009_etl_metadata.sql, which asserts it). Partition bounds are therefore
-- written as explicit UTC literals, and anywhere a timestamp is reduced to a
-- day we use `(ts AT TIME ZONE 'UTC')::date`, which is IMMUTABLE and so is
-- usable in indexes and generated columns.
--
-- The reference schema enabled uuid-ossp but never used a UUID; it is dropped
-- here rather than carried forward.

CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- trigram indexes for product name search

-- Order lifecycle. An enum is 4 bytes on disk versus a text value or an extra
-- join, which matters across millions of order rows. The trade-off is that
-- adding a state needs an ALTER TYPE; that is acceptable for a value set this
-- stable, and the ETL treats an unknown status as a validation error rather
-- than silently coercing it.
CREATE TYPE order_status AS ENUM (
    'Pending',
    'Processing',
    'Shipped',
    'In Transit',
    'Delivered',
    'Cancelled',
    'Returned'
);

CREATE TYPE payment_method AS ENUM (
    'Credit Card',
    'PayPal',
    'Apple Pay',
    'Google Pay',
    'Gift Card',
    'Bank Transfer'
);

-- Statuses that must not contribute to revenue. Centralised here so the
-- aggregation table, the materialized views and the API all apply the same
-- rule instead of repeating a NOT IN (...) list that can drift.
CREATE FUNCTION is_revenue_bearing(p_status order_status)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT p_status NOT IN ('Cancelled'::order_status, 'Returned'::order_status);
$$;

COMMENT ON FUNCTION is_revenue_bearing IS
    'True when an order status should count towards revenue metrics.';
