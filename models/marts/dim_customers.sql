{{ config(materialized='table') }}

WITH staged_customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    -- Generate an enterprise-safe deterministic surrogate key
    md5(cast(customer_id as text)) AS customer_key,
    customer_id AS natural_customer_id,
    sanitized_customer_name AS customer_name,
    email_address,
    country_code,
    registered_at_date
FROM staged_customers

