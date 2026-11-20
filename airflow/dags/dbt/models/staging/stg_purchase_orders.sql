-- Staging: CORE purchase order headers (snake_case; timestamps as-is).
SELECT
    ordernumber AS order_number,
    ordertype AS order_type,
    orderstatus AS order_status,
    creationtime AS creation_time,
    CAST(creationtime AS DATE) AS creation_date,
    lastupdated AS last_updated,
    suppliercode AS supplier_code,
    suppliername AS supplier_name,
    creator,
    purchasecurrency AS purchase_currency,
    netsum AS header_net_sum,
    taxsum AS header_tax_sum,
    grosssum AS header_gross_sum
FROM {{ source('core', 'PURCHASE_ORDER') }}
