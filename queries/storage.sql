CREATE TABLE IF NOT EXISTS shop.storage
(

    `ID` UInt32,

    `Name` String,

    `Address` String
)
ENGINE = MergeTree
ORDER BY ID
SETTINGS index_granularity = 8192;