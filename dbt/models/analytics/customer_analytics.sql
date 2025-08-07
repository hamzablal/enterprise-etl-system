{{ config(materialized='table', schema='analytics') }}

-- Customer analytics for business intelligence
-- Materialized as table for fast BI tool queries
WITH customer_metrics AS (
    SELECT
        customer_id,
        country,
        COUNT(DISTINCT invoice_no) as total_orders,
        COUNT(*) as total_line_items,
        SUM(line_total) as total_spent,
        AVG(line_total) as avg_line_value,
        MIN(transaction_date) as first_purchase,
        MAX(transaction_date) as last_purchase,
        COUNT(DISTINCT stock_code) as unique_products_purchased
    FROM {{ ref('stg_cleaned_transactions') }}
    WHERE transaction_type = 'sale'
      AND customer_id != -1
    GROUP BY customer_id, country
)

SELECT
    *,
    total_spent / total_orders as avg_order_value,
    CURRENT_DATE - last_purchase::date as days_since_last_purchase,
    -- Customer segmentation for business users
    CASE 
        WHEN total_spent >= 5000 THEN 'VIP'
        WHEN total_spent >= 2000 THEN 'High Value'
        WHEN total_spent >= 500 THEN 'Medium Value'
        ELSE 'Standard'
    END as customer_tier,
    
    -- Customer lifecycle status
    CASE 
        WHEN CURRENT_DATE - last_purchase::date <= 30 THEN 'Active'
        WHEN CURRENT_DATE - last_purchase::date <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END as lifecycle_status
FROM customer_metrics
ORDER BY total_spent DESC
