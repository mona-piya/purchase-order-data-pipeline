-- Staging: CORE lines. Cast quantity / line_number for mart measures and sort.
SELECT
    ordernumber AS order_number,
    linenumber AS line_number,
    ordernumber || '|' || linenumber AS order_line_key,
    TRY_TO_NUMBER(linenumber) AS line_number_int,
    linestatus AS line_status,
    description AS line_description,
    categorycode AS category_code,
    categoryname AS category_name,
    quantity AS quantity_text,
    TRY_TO_NUMBER(quantity, 18, 4) AS ordered_qty,
    quantityunit AS quantity_unit,
    netunitprice AS net_unit_price,
    netsum AS ordered_net_amount,
    taxcode AS tax_code,
    taxpercent AS tax_percent,
    taxtotal AS ordered_tax_amount,
    grosssum AS ordered_gross_amount,
    desireddeliverydate AS desired_delivery_date
FROM {{ source('core', 'PURCHASE_ORDER_LINE') }}
