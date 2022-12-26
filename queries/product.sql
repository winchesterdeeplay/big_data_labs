CREATE TABLE IF NOT EXISTS shop.product
(

    `ID` UInt32,

    `Name` String,

    `Code` UInt64
)
ENGINE = MergeTree
ORDER BY ID
SETTINGS index_granularity = 8192;
