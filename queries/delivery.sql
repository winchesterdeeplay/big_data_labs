CREATE TABLE IF NOT EXISTS shop.delivery
(

    `ID` UInt32,

    `ProviderID` UInt32,

    `StorageID` UInt32,

    `DeliveryDateTime` DateTime
)
ENGINE = MergeTree
ORDER BY ID
SETTINGS index_granularity = 8192;
