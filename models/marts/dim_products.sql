{{ config(materialized='table') }}

WITH staged_products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    md5(cast(product_id as text)) AS product_key,
    product_id AS natural_product_id,
    product_name,
    product_category,
    unit_price_amount,
    is_active_flag
FROM staged_products

