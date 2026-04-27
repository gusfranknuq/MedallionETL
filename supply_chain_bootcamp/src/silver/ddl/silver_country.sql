-- Dimension table for countries.
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_country (
    countryid     BIGINT      NOT NULL,
    country_key   STRING      NOT NULL,
    countrydesc1  STRING,
    countrydesc2  STRING,
    countrydesc3  STRING,

    insjobid      STRING,
    modjobid      STRING,
    ins_ts        TIMESTAMP,
    mod_ts        TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
