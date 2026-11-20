-- Four ER tables (the contract). Run after 00_setup.sql (CORE schema exists).
-- Source: docs/data-analysis.md §3.2

USE DATABASE PURCHASE_ORDER_DB;
USE SCHEMA CORE;
USE WAREHOUSE PO_WH;

CREATE TABLE IF NOT EXISTS PURCHASE_ORDER (
    OrderNumber       VARCHAR(20)    NOT NULL,
    OrderType         VARCHAR(20),
    OrderStatus       VARCHAR(30)    NOT NULL,
    CreationTime      TIMESTAMP_TZ   NOT NULL,
    LastUpdated       TIMESTAMP_TZ   NOT NULL,
    SupplierCode      VARCHAR(20)    NOT NULL,
    SupplierName      VARCHAR(200),
    Creator           VARCHAR(100),
    PurchaseCurrency  VARCHAR(10),
    NetSum            NUMBER(18, 2),
    TaxSum            NUMBER(18, 2),
    GrossSum          NUMBER(18, 2),
    CONSTRAINT PK_PURCHASE_ORDER PRIMARY KEY (OrderNumber)
);

CREATE TABLE IF NOT EXISTS PURCHASE_ORDER_LINE (
    OrderNumber          VARCHAR(20)    NOT NULL,
    LineNumber           VARCHAR(10)    NOT NULL,
    LineStatus           VARCHAR(30)    NOT NULL,
    Description          VARCHAR(500),
    CategoryCode         VARCHAR(20),
    CategoryName         VARCHAR(200),
    Quantity             VARCHAR(30)    NOT NULL,
    QuantityUnit         VARCHAR(10),
    NetUnitPrice         NUMBER(18, 4),
    NetSum               NUMBER(18, 2),
    TaxCode              VARCHAR(20),
    TaxPercent           NUMBER(5, 2),
    TaxTotal             NUMBER(18, 2),
    GrossSum             NUMBER(18, 2),
    DesiredDeliveryDate  DATE,
    CONSTRAINT PK_PURCHASE_ORDER_LINE PRIMARY KEY (OrderNumber, LineNumber),
    CONSTRAINT FK_POL_PO FOREIGN KEY (OrderNumber)
        REFERENCES PURCHASE_ORDER (OrderNumber)
);

CREATE TABLE IF NOT EXISTS PURCHASE_ORDER_LINE_CODING_ROW (
    OrderNumber      VARCHAR(20)   NOT NULL,
    LineNumber       VARCHAR(10)   NOT NULL,
    RowIndex         VARCHAR(10)   NOT NULL,
    SplitPercent     NUMBER(5, 2)  NOT NULL,
    AccountCode      VARCHAR(20)   NOT NULL,
    AccountName      VARCHAR(200),
    CostCenterCode   VARCHAR(20)   NOT NULL,
    CostCenterName   VARCHAR(200),
    ProjectCode      VARCHAR(30),
    CONSTRAINT PK_POL_CODING_ROW PRIMARY KEY (OrderNumber, LineNumber, RowIndex),
    CONSTRAINT FK_POLCR_POL FOREIGN KEY (OrderNumber, LineNumber)
        REFERENCES PURCHASE_ORDER_LINE (OrderNumber, LineNumber)
);

CREATE TABLE IF NOT EXISTS PURCHASE_ORDER_LINE_GOODS_RECEIPT (
    GoodsReceiptNumber          VARCHAR(20)    NOT NULL,
    OrderNumber                 VARCHAR(20)    NOT NULL,
    LineNumber                  VARCHAR(10)    NOT NULL,
    ReversedGoodsReceiptNumber  VARCHAR(20),
    ReceivingDate               DATE           NOT NULL,
    ReceivedQuantity            NUMBER(18, 4)  NOT NULL,
    ReceivedNetSum              NUMBER(18, 2),
    Recipient                   VARCHAR(100),
    CONSTRAINT PK_POL_GOODS_RECEIPT PRIMARY KEY (GoodsReceiptNumber),
    CONSTRAINT FK_POLGR_POL FOREIGN KEY (OrderNumber, LineNumber)
        REFERENCES PURCHASE_ORDER_LINE (OrderNumber, LineNumber),
    CONSTRAINT FK_POLGR_REVERSAL FOREIGN KEY (ReversedGoodsReceiptNumber)
        REFERENCES PURCHASE_ORDER_LINE_GOODS_RECEIPT (GoodsReceiptNumber)
);
