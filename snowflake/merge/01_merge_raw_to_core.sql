-- Merge one Kafka/Airflow batch from RAW into the four CORE tables.
-- Spec: docs/data-analysis.md §3.3
--
-- Before running: set batch_id to a value from
--   SELECT * FROM RAW.PURCHASE_ORDER_PAYLOADS_BATCH_COUNTS;
-- Expected after initial load: 100 / 215 / 275 / 181

USE DATABASE PURCHASE_ORDER_DB;
USE WAREHOUSE PO_WH;

-- >>> edit this <<<
SET batch_id = 'initial-21cea962';

BEGIN;

-- 1) One payload per OrderNumber in this batch (newest LastUpdated; load_dts tie-break)
CREATE OR REPLACE TEMPORARY TABLE merge_deduped AS
SELECT
    order_number,
    last_updated,
    payload
FROM RAW.PURCHASE_ORDER_PAYLOADS
WHERE batch_id = $batch_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_number
    ORDER BY last_updated DESC, load_dts DESC
) = 1;

-- 2) LastUpdated guard: apply when new, or incoming >= stored (equal = replay no-op)
CREATE OR REPLACE TEMPORARY TABLE merge_eligible AS
SELECT d.order_number, d.last_updated, d.payload
FROM merge_deduped d
LEFT JOIN CORE.PURCHASE_ORDER c
    ON c.OrderNumber = d.order_number
WHERE c.OrderNumber IS NULL
   OR d.last_updated >= c.LastUpdated;

-- 3) Header MERGE
MERGE INTO CORE.PURCHASE_ORDER AS t
USING (
    SELECT
        payload:OrderNumber::STRING                         AS OrderNumber,
        payload:OrderType::STRING                           AS OrderType,
        payload:OrderStatus::STRING                         AS OrderStatus,
        TO_TIMESTAMP_TZ(payload:CreationTime::STRING)       AS CreationTime,
        TO_TIMESTAMP_TZ(payload:LastUpdated::STRING)        AS LastUpdated,
        payload:SupplierCode::STRING                        AS SupplierCode,
        payload:SupplierName::STRING                        AS SupplierName,
        payload:Creator::STRING                             AS Creator,
        payload:PurchaseCurrency::STRING                    AS PurchaseCurrency,
        payload:NetSum::NUMBER(18, 2)                       AS NetSum,
        payload:TaxSum::NUMBER(18, 2)                       AS TaxSum,
        payload:GrossSum::NUMBER(18, 2)                     AS GrossSum
    FROM merge_eligible
) AS s
ON t.OrderNumber = s.OrderNumber
WHEN MATCHED THEN UPDATE SET
    OrderType        = s.OrderType,
    OrderStatus      = s.OrderStatus,
    CreationTime     = s.CreationTime,
    LastUpdated      = s.LastUpdated,
    SupplierCode     = s.SupplierCode,
    SupplierName     = s.SupplierName,
    Creator          = s.Creator,
    PurchaseCurrency = s.PurchaseCurrency,
    NetSum           = s.NetSum,
    TaxSum           = s.TaxSum,
    GrossSum         = s.GrossSum
WHEN NOT MATCHED THEN INSERT (
    OrderNumber, OrderType, OrderStatus, CreationTime, LastUpdated,
    SupplierCode, SupplierName, Creator, PurchaseCurrency,
    NetSum, TaxSum, GrossSum
) VALUES (
    s.OrderNumber, s.OrderType, s.OrderStatus, s.CreationTime, s.LastUpdated,
    s.SupplierCode, s.SupplierName, s.Creator, s.PurchaseCurrency,
    s.NetSum, s.TaxSum, s.GrossSum
);

-- 4) Replace children for eligible orders (delete bottom-up for FKs)
DELETE FROM CORE.PURCHASE_ORDER_LINE_CODING_ROW
WHERE OrderNumber IN (SELECT order_number FROM merge_eligible);

DELETE FROM CORE.PURCHASE_ORDER_LINE_GOODS_RECEIPT
WHERE OrderNumber IN (SELECT order_number FROM merge_eligible);

DELETE FROM CORE.PURCHASE_ORDER_LINE
WHERE OrderNumber IN (SELECT order_number FROM merge_eligible);

-- 5) Insert lines
INSERT INTO CORE.PURCHASE_ORDER_LINE (
    OrderNumber, LineNumber, LineStatus, Description,
    CategoryCode, CategoryName, Quantity, QuantityUnit,
    NetUnitPrice, NetSum, TaxCode, TaxPercent, TaxTotal, GrossSum,
    DesiredDeliveryDate
)
SELECT
    e.order_number,
    l.value:LineNumber::STRING,
    l.value:LineStatus::STRING,
    l.value:Description::STRING,
    l.value:CategoryCode::STRING,
    l.value:CategoryName::STRING,
    l.value:Quantity::STRING,
    l.value:QuantityUnit::STRING,
    l.value:NetUnitPrice::NUMBER(18, 4),
    l.value:NetSum::NUMBER(18, 2),
    l.value:TaxCode::STRING,
    l.value:TaxPercent::NUMBER(5, 2),
    l.value:TaxTotal::NUMBER(18, 2),
    l.value:GrossSum::NUMBER(18, 2),
    TRY_TO_DATE(l.value:DesiredDeliveryDate::STRING)
FROM merge_eligible e,
LATERAL FLATTEN(input => e.payload:OrderLines) l;

-- 6) Insert coding rows (cost allocation splits)
INSERT INTO CORE.PURCHASE_ORDER_LINE_CODING_ROW (
    OrderNumber, LineNumber, RowIndex, SplitPercent,
    AccountCode, AccountName, CostCenterCode, CostCenterName, ProjectCode
)
SELECT
    e.order_number,
    l.value:LineNumber::STRING,
    c.value:RowIndex::STRING,
    c.value:SplitPercent::NUMBER(5, 2),
    c.value:AccountCode::STRING,
    c.value:AccountName::STRING,
    c.value:CostCenterCode::STRING,
    c.value:CostCenterName::STRING,
    c.value:ProjectCode::STRING
FROM merge_eligible e,
LATERAL FLATTEN(input => e.payload:OrderLines) l,
LATERAL FLATTEN(input => l.value:CodingRows) c;

-- 7a) Goods receipts — originals first (ReversedGoodsReceiptNumber IS NULL)
INSERT INTO CORE.PURCHASE_ORDER_LINE_GOODS_RECEIPT (
    GoodsReceiptNumber, OrderNumber, LineNumber, ReversedGoodsReceiptNumber,
    ReceivingDate, ReceivedQuantity, ReceivedNetSum, Recipient
)
SELECT
    g.value:GoodsReceiptNumber::STRING,
    e.order_number,
    l.value:LineNumber::STRING,
    g.value:ReversedGoodsReceiptNumber::STRING,
    TRY_TO_DATE(g.value:ReceivingDate::STRING),
    g.value:ReceivedQuantity::NUMBER(18, 4),
    g.value:ReceivedNetSum::NUMBER(18, 2),
    g.value:Recipient::STRING
FROM merge_eligible e,
LATERAL FLATTEN(input => e.payload:OrderLines) l,
LATERAL FLATTEN(input => l.value:GoodsReceipts) g
WHERE g.value:GoodsReceiptNumber IS NOT NULL
  AND g.value:ReversedGoodsReceiptNumber IS NULL;

-- 7b) Then reversals (self-FK to original receipt)
INSERT INTO CORE.PURCHASE_ORDER_LINE_GOODS_RECEIPT (
    GoodsReceiptNumber, OrderNumber, LineNumber, ReversedGoodsReceiptNumber,
    ReceivingDate, ReceivedQuantity, ReceivedNetSum, Recipient
)
SELECT
    g.value:GoodsReceiptNumber::STRING,
    e.order_number,
    l.value:LineNumber::STRING,
    g.value:ReversedGoodsReceiptNumber::STRING,
    TRY_TO_DATE(g.value:ReceivingDate::STRING),
    g.value:ReceivedQuantity::NUMBER(18, 4),
    g.value:ReceivedNetSum::NUMBER(18, 2),
    g.value:Recipient::STRING
FROM merge_eligible e,
LATERAL FLATTEN(input => e.payload:OrderLines) l,
LATERAL FLATTEN(input => l.value:GoodsReceipts) g
WHERE g.value:GoodsReceiptNumber IS NOT NULL
  AND g.value:ReversedGoodsReceiptNumber IS NOT NULL;

COMMIT;

-- Counts (expect 100 / 215 / 275 / 181 after a clean initial merge)
SELECT 'PURCHASE_ORDER' AS table_name, COUNT(*) AS row_count FROM CORE.PURCHASE_ORDER
UNION ALL SELECT 'PURCHASE_ORDER_LINE', COUNT(*) FROM CORE.PURCHASE_ORDER_LINE
UNION ALL SELECT 'PURCHASE_ORDER_LINE_CODING_ROW', COUNT(*) FROM CORE.PURCHASE_ORDER_LINE_CODING_ROW
UNION ALL SELECT 'PURCHASE_ORDER_LINE_GOODS_RECEIPT', COUNT(*) FROM CORE.PURCHASE_ORDER_LINE_GOODS_RECEIPT
ORDER BY 1;
