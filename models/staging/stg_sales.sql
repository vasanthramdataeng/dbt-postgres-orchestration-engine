{{ config(materialized='view') }}

WITH base_sales AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        CAST(quantity AS INT) AS sold_quantity,
        CAST(total_amount AS NUMERIC(12, 2)) AS gross_revenue_amount,
        CAST(transaction_date AS TIMESTAMP) AS transacted_at
    FROM {{ source('raw_source', 'src_sales') }}
)

SELECT * FROM base_sales

