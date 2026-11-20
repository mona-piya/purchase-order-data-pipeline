-- Receipt grain: one row per GoodsReceiptNumber with lag vs order / desired delivery.
SELECT
    g.goods_receipt_number,
    g.order_number,
    g.line_number,
    g.reversed_goods_receipt_number,
    g.is_reversal,
    g.receiving_date,
    g.received_qty,
    g.received_net_amount,
    g.recipient,
    o.creation_date,
    l.desired_delivery_date,
    o.supplier_code,
    l.category_code,
    DATEDIFF('day', o.creation_date, g.receiving_date) AS days_from_order_to_receipt,
    DATEDIFF('day', l.desired_delivery_date, g.receiving_date) AS days_vs_desired_delivery
FROM {{ ref('stg_purchase_order_line_goods_receipts') }} AS g
INNER JOIN {{ ref('stg_purchase_order_lines') }} AS l
    ON
        g.order_number = l.order_number
        AND g.line_number = l.line_number
INNER JOIN {{ ref('stg_purchase_orders') }} AS o
    ON g.order_number = o.order_number
