-- Type-1 cost centre dimension (faculties CC11xx + professional services CC21xx).
SELECT DISTINCT
    cost_center_code,
    cost_center_name
FROM {{ ref('stg_purchase_order_line_coding') }}
WHERE cost_center_code IS NOT NULL
