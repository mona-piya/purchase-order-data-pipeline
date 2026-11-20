from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


# Parse an ISO timestamp (Z or offset) into UTC; reject naive values.
def parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Timestamp must include a timezone offset')
    return value.astimezone(timezone.utc)


# Base model: reject unknown fields so bad extracts fail loudly.
class SourceModel(BaseModel):
    model_config = ConfigDict(extra='forbid')


# One cost-allocation split on a line (account / cost centre / project + %).
class CodingRow(SourceModel):
    RowIndex: str
    SplitPercent: Decimal
    AccountCode: str
    AccountName: str | None = None
    CostCenterCode: str
    CostCenterName: str | None = None
    ProjectCode: str | None = None


# One goods receipt (or reversal) on a line.
class GoodsReceipt(SourceModel):
    GoodsReceiptNumber: str
    ReversedGoodsReceiptNumber: str | None = None
    ReceivingDate: date
    ReceivedQuantity: Decimal
    ReceivedNetSum: Decimal
    Recipient: str | None = None


# One purchase-order line, with nested coding rows and receipts.
class OrderLine(SourceModel):
    LineNumber: str
    LineStatus: str
    Description: str | None = None
    CategoryCode: str | None = None
    CategoryName: str | None = None
    Quantity: str
    QuantityUnit: str | None = None
    NetUnitPrice: Decimal
    NetSum: Decimal
    TaxCode: str | None = None
    TaxPercent: Decimal
    TaxTotal: Decimal
    GrossSum: Decimal
    DesiredDeliveryDate: date | None = None
    CodingRows: list[CodingRow] = Field(default_factory=list)
    GoodsReceipts: list[GoodsReceipt] = Field(default_factory=list)


# Full purchase order header + lines (the Kafka / API document).
class PurchaseOrder(SourceModel):
    OrderNumber: str
    OrderType: str | None = None
    OrderStatus: str
    CreationTime: datetime
    LastUpdated: datetime
    SupplierCode: str
    SupplierName: str | None = None
    Creator: str | None = None
    PurchaseCurrency: str | None = None
    NetSum: Decimal
    TaxSum: Decimal
    GrossSum: Decimal
    OrderLines: list[OrderLine] = Field(default_factory=list)

    # Normalise CreationTime / LastUpdated to UTC with offset.
    @field_validator('CreationTime', 'LastUpdated', mode='before')
    @classmethod
    def utc_timestamps(cls, value: str | datetime) -> datetime:
        return parse_timestamp(value)


# Wrapper for a source extract file (initial or incremental).
class PurchaseOrderExtract(SourceModel):
    source: str
    load_type: Literal['initial', 'incremental']
    extracted_at: datetime
    record_count: int
    purchased_orders: list[PurchaseOrder]

    # Normalise extracted_at to UTC with offset.
    @field_validator('extracted_at', mode='before')
    @classmethod
    def utc_extracted_at(cls, value: str | datetime) -> datetime:
        return parse_timestamp(value)

    # Ensure record_count matches the orders list length.
    @model_validator(mode='after')
    def check_record_count(self):
        if self.record_count != len(self.purchased_orders):
            raise ValueError('record_count does not match purchased_orders length')
        return self
