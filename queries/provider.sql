CREATE TABLE IF NOT EXISTS shop.provider
(

    `ID` UInt32,

    `Name` String
)
ENGINE = MergeTree
ORDER BY ID
SETTINGS index_granularity = 8192;
