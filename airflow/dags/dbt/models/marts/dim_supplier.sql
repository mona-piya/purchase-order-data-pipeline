-- Type-1 supplier dimension (natural key = supplier_code).
SELECT DISTINCT
    supplier_code,
    supplier_name
FROM {{ ref('stg_purchase_orders') }}
WHERE supplier_code IS NOT NULL
