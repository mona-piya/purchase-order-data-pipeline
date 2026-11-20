-- Category active net must equal cost-centre allocated active net (no allocation fan-out).
WITH category_total AS (
    SELECT COALESCE(SUM(active_net_amount), 0) AS total_active_net
    FROM {{ ref('fct_purchase_order_lines') }}
),
cost_center_total AS (
    SELECT COALESCE(SUM(allocated_active_net_amount), 0) AS total_allocated_active_net
    FROM {{ ref('fct_po_cost_allocations') }}
)
SELECT
    c.total_active_net,
    a.total_allocated_active_net
FROM category_total c
CROSS JOIN cost_center_total a
WHERE ABS(c.total_active_net - a.total_allocated_active_net) > 0.05
