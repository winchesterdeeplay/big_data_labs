CREATE TABLE IF NOT EXISTS shop.invoice_products_line
(

    `ID` UInt32,

    `ProductID` UInt32,

    `Quantity` UInt16,

    `InvoiceID` UInt32
)
ENGINE = MergeTree
ORDER BY ID
SETTINGS index_granularity = 8192;
