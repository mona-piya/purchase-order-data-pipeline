-- Staging: cost-allocation coding rows. Null project → UNASSIGNED for dim_project.
SELECT
    ordernumber AS order_number,
    linenumber AS line_number,
    rowindex AS row_index,
    ordernumber || '|' || linenumber || '|' || rowindex AS coding_key,
    TRY_TO_NUMBER(rowindex) AS row_index_int,
    splitpercent AS split_percent,
    splitpercent / 100.0 AS allocation_ratio,
    accountcode AS account_code,
    accountname AS account_name,
    costcentercode AS cost_center_code,
    costcentername AS cost_center_name,
    COALESCE(projectcode, 'UNASSIGNED') AS project_code,
    projectcode AS project_code_raw
FROM {{ source('core', 'PURCHASE_ORDER_LINE_CODING_ROW') }}
