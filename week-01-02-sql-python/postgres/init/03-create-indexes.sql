-- File: week-01-02-sql-python/postgres/init/03-create-indexes.sql

SET search_path TO analytics;

-- Customers indexes
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_country ON customers(country);
CREATE INDEX idx_customers_signup_date ON customers(signup_date);
CREATE INDEX idx_customers_segment ON customers(customer_segment);

-- Products indexes
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_active ON products(is_active);

-- Orders indexes
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

-- Order items indexes
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);

-- Categories indexes
CREATE INDEX idx_categories_parent ON categories(parent_category_id);

-- Analyze tables for query planner
ANALYZE customers;
ANALYZE products;
ANALYZE orders;
ANALYZE order_items;
ANALYZE categories;