-- Line grain: ordered amounts + receipt aggregates; derives fulfilment status.
WITH receipts AS (
    SELECT
        order_number,
        line_number,
        SUM(received_qty) AS net_received_qty,
        SUM(received_net_amount) AS net_received_amount
    FROM {{ ref('stg_purchase_order_line_goods_receipts') }}
    GROUP BY 1, 2
),

joined AS (
    SELECT
        l.order_line_key,
        l.order_number,
        l.line_number,
        l.line_number_int,
        l.line_status,
        l.line_description,
        l.category_code,
        o.supplier_code,
        o.order_status AS source_order_status,
        o.creation_date,
        l.desired_delivery_date,
        l.ordered_qty,
        l.net_unit_price,
        l.ordered_net_amount,
        l.ordered_tax_amount,
        l.ordered_gross_amount,
        CASE WHEN l.line_status = 'Open' THEN l.ordered_net_amount ELSE 0 END AS active_net_amount,
        CASE WHEN l.line_status = 'Cancelled' THEN l.ordered_net_amount ELSE 0 END AS cancelled_net_amount,
        COALESCE(r.net_received_qty, 0) AS net_received_qty,
        COALESCE(r.net_received_amount, 0) AS net_received_amount
    FROM {{ ref('stg_purchase_order_lines') }} AS l
    INNER JOIN {{ ref('stg_purchase_orders') }} AS o
        ON l.order_number = o.order_number
    LEFT JOIN receipts AS r
        ON
            l.order_number = r.order_number
            AND l.line_number = r.line_number
)

SELECT
    order_line_key,
    order_number,
    line_number,
    line_number_int,
    line_status,
    line_description,
    category_code,
    supplier_code,
    source_order_status,
    creation_date,
    desired_delivery_date,
    ordered_qty,
    net_unit_price,
    ordered_net_amount,
    ordered_tax_amount,
    ordered_gross_amount,
    active_net_amount,
    cancelled_net_amount,
    net_received_qty,
    net_received_amount,
    CASE
        WHEN line_status = 'Open' THEN ordered_qty - net_received_qty
        ELSE 0
    END AS open_unreceived_qty,
    CASE
        WHEN line_status = 'Cancelled' THEN 'cancelled'
        WHEN COALESCE(net_received_qty, 0) = 0 THEN 'unreceived'
        WHEN net_received_qty > ordered_qty THEN 'over_received'
        WHEN net_received_qty = ordered_qty THEN 'complete'
        ELSE 'partial'
    END AS derived_line_fulfilment
FROM joined
