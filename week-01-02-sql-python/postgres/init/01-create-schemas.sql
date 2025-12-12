-- File: week-01-02-sql-python/postgres/init/01-create-schemas.sql

-- Create schemas
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS raw;

-- Set default search path
ALTER DATABASE data_engineer SET search_path TO analytics, public;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA analytics TO dataengineer;
GRANT ALL PRIVILEGES ON SCHEMA staging TO dataengineer;
GRANT ALL PRIVILEGES ON SCHEMA raw TO dataengineer;

-- Grant all on all tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO dataengineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA analytics TO dataengineer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dataengineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO dataengineer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO dataengineer;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO dataengineer;

-- Create audit table
CREATE TABLE IF NOT EXISTS analytics.audit_log (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    operation VARCHAR(10),
    user_name VARCHAR(100),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_data JSONB,
    new_data JSONB
);

COMMENT ON TABLE analytics.audit_log IS 'Audit log for tracking data changes';

-- Create a simple test table to verify setup
CREATE TABLE IF NOT EXISTS analytics.setup_test (
    id SERIAL PRIMARY KEY,
    test_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO analytics.setup_test (test_message) 
VALUES ('Database setup completed successfully!');