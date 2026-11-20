-- Coding-split grain: line amounts × allocation_ratio (SplitPercent / 100).
SELECT
    c.coding_key,
    l.order_line_key,
    c.order_number,
    c.line_number,
    c.row_index,
    c.row_index_int,
    c.split_percent,
    c.allocation_ratio,
    c.account_code,
    c.cost_center_code,
    c.project_code,
    l.category_code,
    l.supplier_code,
    l.line_status,
    l.ordered_net_amount * c.allocation_ratio AS allocated_ordered_net_amount,
    l.active_net_amount * c.allocation_ratio AS allocated_active_net_amount,
    l.cancelled_net_amount * c.allocation_ratio AS allocated_cancelled_net_amount,
    l.ordered_gross_amount * c.allocation_ratio AS allocated_gross_amount,
    l.net_received_amount * c.allocation_ratio AS allocated_received_net_amount
FROM {{ ref('stg_purchase_order_line_coding') }} AS c
INNER JOIN {{ ref('fct_purchase_order_lines') }} AS l
    ON
        c.order_number = l.order_number
        AND c.line_number = l.line_number
