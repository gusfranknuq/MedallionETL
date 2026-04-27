-- Dimension table for retailers.
-- Note: silver_retailer is itself keyed on RETAILERID, but we follow the
-- standard dimension shape so dim_handler() can manage it uniformly.
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_retailer (
    retailerid     BIGINT      NOT NULL,
    countryid      INT         NOT NULL,
    retailer_key   STRING      NOT NULL,
    retailerdesc1  STRING,
    retailerdesc2  STRING,
    retailerdesc3  STRING,

    insjobid       STRING,
    modjobid       STRING,
    ins_ts         TIMESTAMP,
    mod_ts         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
