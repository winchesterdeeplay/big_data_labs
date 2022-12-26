CREATE TABLE IF NOT EXISTS shop.invoice
(

    `ID` UInt32,

    `StorageID` UInt32,

    `InvoiceDateTime` DateTime
)
ENGINE = MergeTree
ORDER BY ID
SETTINGS index_granularity = 8192;
