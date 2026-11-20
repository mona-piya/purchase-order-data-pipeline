-- Staging: goods receipts (reversals keep negative quantities / amounts).
SELECT
    goodsreceiptnumber AS goods_receipt_number,
    ordernumber AS order_number,
    linenumber AS line_number,
    reversedgoodsreceiptnumber AS reversed_goods_receipt_number,
    receivingdate AS receiving_date,
    receivedquantity AS received_qty,
    receivednetsum AS received_net_amount,
    recipient,
    (reversedgoodsreceiptnumber IS NOT NULL) AS is_reversal
FROM {{ source('core', 'PURCHASE_ORDER_LINE_GOODS_RECEIPT') }}
