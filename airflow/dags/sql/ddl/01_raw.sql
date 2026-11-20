-- First landing place for purchase orders in Snowflake.
-- One row per ingested Kafka message (at-least-once; duplicates are OK here).

USE DATABASE PURCHASE_ORDER_DB;
USE SCHEMA RAW;
USE WAREHOUSE PO_WH;

CREATE TABLE IF NOT EXISTS PURCHASE_ORDER_PAYLOADS (
    order_number      VARCHAR(20)   NOT NULL,
    batch_id          VARCHAR(100)  NOT NULL,
    load_type         VARCHAR(20)   NOT NULL,
    extracted_at      TIMESTAMP_TZ  NOT NULL,
    last_updated      TIMESTAMP_TZ  NOT NULL,
    payload           VARIANT       NOT NULL,
    kafka_topic       VARCHAR(200),
    kafka_partition   NUMBER,
    kafka_offset      NUMBER,
    source_file       VARCHAR(500),
    load_dts       TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP()
);

-- Handy for wait_for_snowflake_raw and demos
CREATE OR REPLACE VIEW PURCHASE_ORDER_PAYLOADS_BATCH_COUNTS AS
SELECT
    batch_id,
    COUNT(*)                    AS row_count,
    COUNT(DISTINCT order_number) AS distinct_orders
FROM PURCHASE_ORDER_PAYLOADS
GROUP BY batch_id;
