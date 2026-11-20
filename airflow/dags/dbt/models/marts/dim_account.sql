-- Type-1 GL account dimension from coding rows.
SELECT DISTINCT
    account_code,
    account_name
FROM {{ ref('stg_purchase_order_line_coding') }}
WHERE account_code IS NOT NULL
