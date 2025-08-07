-- Simple database setup
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Simple raw data table
CREATE TABLE raw.ecommerce_data (
    invoice_no VARCHAR(50),
    stock_code VARCHAR(50),
    description TEXT,
    quantity INTEGER,
    invoice_date TIMESTAMP,
    unit_price DECIMAL(10,2),
    customer_id INTEGER,
    country VARCHAR(100)
);
