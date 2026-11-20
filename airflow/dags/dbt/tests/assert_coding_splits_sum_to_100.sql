-- Every line's coding SplitPercent values must sum to 100.
SELECT
    order_number,
    line_number,
    SUM(split_percent) AS split_sum
FROM {{ ref('stg_purchase_order_line_coding') }}
GROUP BY 1, 2
HAVING ABS(SUM(split_percent) - 100) > 0.01
