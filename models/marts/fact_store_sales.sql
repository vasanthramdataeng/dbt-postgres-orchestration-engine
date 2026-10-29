{{ config(materialized='table') }}

WITH staged_sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

customer_dimension AS (
    SELECT customer_key, natural_customer_id FROM {{ ref('dim_customers') }}
),

product_dimension AS (
    SELECT product_key, natural_product_id FROM {{ ref('dim_products') }}
)

SELECT
    md5(cast(s.transaction_id as text)) AS sales_fact_key,
    s.transaction_id,
    
    -- Map foreign keys back to surrogate keys
    c.customer_key,
    p.product_key,
    
    s.sold_quantity,
    s.gross_revenue_amount,
    s.transacted_at,
    
    -- Calculated factual metrics
    (s.gross_revenue_amount / NULLIF(s.sold_quantity, 0)) AS calculated_average_unit_price
    
FROM staged_sales s
LEFT JOIN customer_dimension c ON s.customer_id = c.natural_customer_id
LEFT JOIN product_dimension p ON s.product_id = p.natural_product_id

