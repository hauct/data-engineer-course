-- File: week-03-04-python-etl/sql/04-create-etl-tables.sql
-- ETL 3-Layer Architecture: raw, stg, prod schemas

-- =============================================================================
-- PROD SCHEMA (need to create first for references)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS prod;
GRANT ALL PRIVILEGES ON SCHEMA prod TO dataengineer;

-- =============================================================================
-- RAW SCHEMA TABLES
-- Data is kept exactly as ingested, with metadata columns
-- =============================================================================

-- Raw Customers
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id INTEGER,
    customer_name VARCHAR(200),
    email VARCHAR(200),
    country VARCHAR(100),
    signup_date DATE,
    customer_segment VARCHAR(20),
    -- Metadata columns
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file VARCHAR(500),
    _partition_date DATE
);

COMMENT ON TABLE raw.customers IS 'Raw customer data - immutable, includes data quality issues';

-- Raw Products  
CREATE TABLE IF NOT EXISTS raw.products (
    product_id INTEGER,
    product_name VARCHAR(200),
    category VARCHAR(50),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    -- Metadata columns
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file VARCHAR(500),
    _partition_date DATE
);

COMMENT ON TABLE raw.products IS 'Raw product data - immutable';

-- Raw Orders
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    order_status VARCHAR(20),
    total_amount DECIMAL(10,2),
    -- Metadata columns
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file VARCHAR(500),
    _partition_date DATE
);

COMMENT ON TABLE raw.orders IS 'Raw order data - immutable';

-- Raw Order Items
CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    -- Metadata columns
    _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file VARCHAR(500),
    _partition_date DATE
);

COMMENT ON TABLE raw.order_items IS 'Raw order items data - immutable';

-- =============================================================================
-- STAGING (STG) SCHEMA TABLES
-- Data is cleaned, deduplicated, validated
-- =============================================================================

-- Staging Customers (cleaned)
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(200) UNIQUE NOT NULL,
    country VARCHAR(100),
    signup_date DATE NOT NULL,
    customer_segment VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging.customers IS 'Cleaned customer data - deduped, validated, standardized';

-- Staging Products (cleaned)
CREATE TABLE IF NOT EXISTS staging.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    cost DECIMAL(10,2) NOT NULL CHECK (cost >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging.products IS 'Cleaned product data - deduped, validated';

-- Staging Orders (cleaned)
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES staging.customers(customer_id),
    order_date DATE NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging.orders IS 'Cleaned order data - deduped, validated';

-- Staging Order Items (cleaned)
CREATE TABLE IF NOT EXISTS staging.order_items (
    order_item_id INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES staging.orders(order_id),
    product_id INTEGER NOT NULL REFERENCES staging.products(product_id),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    discount_percent DECIMAL(5,2) DEFAULT 0 CHECK (discount_percent >= 0 AND discount_percent <= 100),
    line_total DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price * (1 - discount_percent/100)) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE staging.order_items IS 'Cleaned order items - deduped, validated';

-- =============================================================================
-- PROD SCHEMA TABLES
-- Aggregated metrics, denormalized, business-ready
-- =============================================================================

-- Daily Sales Summary
CREATE TABLE IF NOT EXISTS prod.daily_sales (
    order_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    total_customers INTEGER NOT NULL,
    avg_order_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE prod.daily_sales IS 'Daily aggregated sales metrics';

-- Monthly Sales Summary
CREATE TABLE IF NOT EXISTS prod.monthly_sales (
    year_month VARCHAR(7) PRIMARY KEY,  -- Format: 'YYYY-MM'
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    total_orders INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    total_customers INTEGER NOT NULL,
    avg_order_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE prod.monthly_sales IS 'Monthly aggregated sales metrics';

-- Daily Category Metrics
CREATE TABLE IF NOT EXISTS prod.daily_category_metrics (
    order_date DATE NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_orders INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    unique_customers INTEGER NOT NULL,
    unique_products INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_date, category)
);

COMMENT ON TABLE prod.daily_category_metrics IS 'Daily sales metrics by product category';

-- Daily Product Metrics
CREATE TABLE IF NOT EXISTS prod.daily_product_metrics (
    order_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(50),
    total_orders INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    unique_customers INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_date, product_id)
);

COMMENT ON TABLE prod.daily_product_metrics IS 'Daily sales metrics by product';

-- Customer Lifetime Metrics
CREATE TABLE IF NOT EXISTS prod.customer_metrics (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200),
    customer_segment VARCHAR(20),
    first_order_date DATE,
    last_order_date DATE,
    total_orders INTEGER NOT NULL,
    total_items INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    avg_order_value DECIMAL(10,2),
    days_since_first_order INTEGER,
    days_since_last_order INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE prod.customer_metrics IS 'Customer lifetime value and behavior metrics';

-- =============================================================================
-- INDEXES for better query performance
-- =============================================================================

-- Raw tables indexes (for ETL lookups)
CREATE INDEX IF NOT EXISTS idx_raw_customers_partition ON raw.customers(_partition_date);
CREATE INDEX IF NOT EXISTS idx_raw_products_partition ON raw.products(_partition_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_partition ON raw.orders(_partition_date);
CREATE INDEX IF NOT EXISTS idx_raw_order_items_partition ON raw.order_items(_partition_date);

-- Staging indexes
CREATE INDEX IF NOT EXISTS idx_stg_orders_customer ON staging.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_date ON staging.orders(order_date);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_order ON staging.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_order_items_product ON staging.order_items(product_id);

-- Prod indexes
CREATE INDEX IF NOT EXISTS idx_prod_category_date ON prod.daily_category_metrics(category, order_date);
CREATE INDEX IF NOT EXISTS idx_prod_product_date ON prod.daily_product_metrics(product_id, order_date);

-- =============================================================================
-- Grant permissions
-- =============================================================================
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO dataengineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO dataengineer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dataengineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO dataengineer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA prod TO dataengineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA prod TO dataengineer;

-- Verify setup
DO $$
BEGIN
    RAISE NOTICE 'ETL Schema setup completed successfully!';
    RAISE NOTICE 'Created tables in: raw, staging, prod schemas';
END $$;
