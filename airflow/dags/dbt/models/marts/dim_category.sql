-- Type-1 category dimension from order lines.
SELECT DISTINCT
    category_code,
    category_name
FROM {{ ref('stg_purchase_order_lines') }}
WHERE category_code IS NOT NULL
