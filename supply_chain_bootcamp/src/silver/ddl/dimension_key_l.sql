-- Lookup table that defines how a dimension's natural _KEY is constructed
-- from one or more bronze columns, scoped per retailer/country.
--
-- Composite primary key: (retailerid, countryid, keyname, keyorder)
--
-- Example: silver_store STORE_KEY = store_number + '_' + division_number
--   1|1|storeid|store_number|1
--   1|1|storeid|division_number|2
--
-- `keyname` is the name of the surrogate id column being built (e.g. 'storeid'),
-- `keypart` is the bronze source column to concatenate, and `keyorder` is the
-- 1-based position of that part within the final _KEY string.
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dimension_key_l (
    retailerid  INT     NOT NULL,
    countryid   INT     NOT NULL,
    keyname     STRING  NOT NULL,
    keypart     STRING  NOT NULL,
    keyorder    INT     NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
