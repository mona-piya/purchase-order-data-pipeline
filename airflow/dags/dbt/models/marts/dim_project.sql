-- Type-1 project dimension including synthetic UNASSIGNED for null ProjectCode.
SELECT DISTINCT
    project_code,
    CASE
        WHEN project_code = 'UNASSIGNED' THEN 'Unassigned'
        ELSE project_code
    END AS project_name
FROM {{ ref('stg_purchase_order_line_coding') }}
