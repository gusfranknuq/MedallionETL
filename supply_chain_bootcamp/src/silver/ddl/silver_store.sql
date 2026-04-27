-- Dimension table for stores.
-- STORE_KEY is built from one or more bronze fields per the
-- dimension_key_l configuration (e.g., store_number + '_' + division_number).
-- STOREID is the generated surrogate key and is the true PK.
-- A unique store is identified by (STORE_KEY, RETAILERID, COUNTRYID).
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_store (
    storeid     BIGINT      NOT NULL,
    retailerid  INT         NOT NULL,
    countryid   INT         NOT NULL,
    store_key   STRING      NOT NULL,
    storedesc1  STRING,
    storedesc2  STRING,
    storedesc3  STRING,

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
