-- Silver fact table at the day / item / store grain.
-- Holds harmonized supply-chain metrics from multiple bronze sources
-- (sales, inventory, ...). Columns suffixed with `ly` represent the
-- corresponding "last year" value for the same business key.
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_day_item_store_tyly (
    day          DATE        NOT NULL,
    itemid       BIGINT      NOT NULL,
    storeid      BIGINT      NOT NULL,
    channelid    BIGINT      NOT NULL,
    customerid   BIGINT      NOT NULL,
    retailerid   INT         NOT NULL,
    countryid    INT         NOT NULL,

    -- Lineage / audit
    insjobid     STRING,
    modjobid     STRING,
    ins_ts       TIMESTAMP,
    mod_ts       TIMESTAMP,

    -- Metrics (this year / last year pairs)
    posqty       DOUBLE,
    posqtyly     DOUBLE,
    possales     DOUBLE,
    possalesly   DOUBLE,
    unitprice    DOUBLE,
    unitpricely  DOUBLE,
    onhandqty    DOUBLE,
    onhandqtyly  DOUBLE,
    instock      DOUBLE,
    instockly    DOUBLE
)
USING DELTA
PARTITIONED BY (countryid, retailerid, day)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
