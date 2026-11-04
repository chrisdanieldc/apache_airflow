-- ClickHouse Analytics Tables
-- These tables store transformed data from PostgreSQL and MySQL

-- Order Analytics Table
-- Combines order, customer, payment, and product data
CREATE TABLE IF NOT EXISTS order_analytics (
    order_id UInt32,
    order_date DateTime,
    order_status String,

    -- Customer information
    customer_id UInt32,
    customer_email String,
    customer_name String,
    customer_city String,
    customer_country String,

    -- Order financials
    order_total Decimal(10, 2),

    -- Payment information
    payment_id Nullable(UInt32),
    payment_method Nullable(String),
    payment_status Nullable(String),
    payment_date Nullable(DateTime),

    -- Metrics
    item_count UInt16,

    -- Timestamps
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (order_date, order_id)
PARTITION BY toYYYYMM(order_date);

-- Product Performance Table
-- Aggregated product sales metrics
CREATE TABLE IF NOT EXISTS product_performance (
    product_id UInt32,
    product_name String,
    category String,

    -- Sales metrics
    total_quantity_sold UInt32,
    total_revenue Decimal(12, 2),
    order_count UInt32,
    avg_price Decimal(10, 2),

    -- Date range
    first_sale_date Nullable(DateTime),
    last_sale_date Nullable(DateTime),

    -- Updated timestamp
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

-- Customer Lifetime Value Table
-- Aggregated customer metrics
CREATE TABLE IF NOT EXISTS customer_ltv (
    customer_id UInt32,
    customer_email String,
    customer_name String,

    -- Order metrics
    total_orders UInt32,
    total_spent Decimal(12, 2),
    avg_order_value Decimal(10, 2),

    -- Dates
    first_order_date Nullable(DateTime),
    last_order_date Nullable(DateTime),

    -- Payment metrics
    successful_payments UInt32,
    failed_payments UInt32,

    -- Updated timestamp
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id;
