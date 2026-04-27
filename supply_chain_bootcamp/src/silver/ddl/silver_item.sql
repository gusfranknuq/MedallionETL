-- Dimension table for items.
-- ITEM_KEY is built from one or more bronze fields per the
-- dimension_key_l configuration (concatenated with '_').
-- ITEMID is the generated surrogate key and is the true PK.
-- A unique item is identified by (ITEM_KEY, RETAILERID, COUNTRYID).
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_item (
    itemid      BIGINT      NOT NULL,
    retailerid  INT         NOT NULL,
    countryid   INT         NOT NULL,
    item_key    STRING      NOT NULL,
    itemdesc1   STRING,
    itemdesc2   STRING,
    itemdesc3   STRING,

    insjobid    STRING,
    modjobid    STRING,
    ins_ts      TIMESTAMP,
    mod_ts      TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
