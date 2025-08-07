{{ config(materialized='view', schema='staging') }}

-- Clean and standardize data in cloud warehouse
-- This follows ELT pattern - data is already loaded, now we transform
SELECT
    invoice_no,
    stock_code,
    TRIM(description) as product_description,
    quantity,
    invoice_date::timestamp as transaction_date,
    unit_price,
    CASE 
        WHEN customer_id IS NULL OR customer_id = 0 THEN -1
        ELSE customer_id 
    END as customer_id,
    UPPER(TRIM(country)) as country,
    quantity * unit_price as line_total,
    load_timestamp,
    batch_id,
    -- Data quality flags
    CASE 
        WHEN quantity < 0 THEN 'return'
        WHEN quantity = 0 THEN 'cancelled'  
        ELSE 'sale'
    END as transaction_type
FROM {{ source('raw_data', 'ecommerce_transactions') }}
WHERE invoice_no IS NOT NULL
