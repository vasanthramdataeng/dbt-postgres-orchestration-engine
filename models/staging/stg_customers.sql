{{ config(materialized='view') }}

WITH raw_customer_source AS (
    SELECT 
        customer_id,
        UPPER(customer_name) AS sanitized_customer_name,
        email_address,
        COALESCE(country, 'UNKNOWN') AS country_code,
        CAST(registration_date AS DATE) AS registered_at_date
    FROM {{ source('raw_source', 'src_customers') }}
)

SELECT * FROM raw_customer_source
