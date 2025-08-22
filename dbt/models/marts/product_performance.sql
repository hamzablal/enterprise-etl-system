{{ config(materialized='table', schema='analytics') }}

-- Product performance analytics
SELECT
    stock_code,
    product_description,
    SUM(quantity) as total_units_sold,
    SUM(line_total) as total_revenue,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT invoice_no) as total_orders,
    AVG(unit_price) as avg_selling_price,
    -- Performance metrics
    ROUND(SUM(line_total) / SUM(quantity), 2) as revenue_per_unit,
    ROUND(SUM(line_total) / COUNT(DISTINCT customer_id), 2) as revenue_per_customer
FROM {{ ref('stg_cleaned_transactions') }}
WHERE transaction_type = 'sale'
GROUP BY stock_code, product_description
HAVING SUM(quantity) > 0
ORDER BY total_revenue DESC
