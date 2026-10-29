{{ config(materialized='view') }}

WITH base_products AS (
    SELECT
        product_id,
        TRIM(product_name) AS product_name,
        category AS product_category,
        CAST(price AS NUMERIC(10, 2)) AS unit_price_amount,
        COALESCE(is_active, TRUE) AS is_active_flag
    FROM {{ source('raw_source', 'src_products') }}
)

SELECT * FROM base_products

