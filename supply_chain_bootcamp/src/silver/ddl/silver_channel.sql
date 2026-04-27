-- Dimension table for channels (e.g., in-store, e-commerce, wholesale).
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_channel (
    channelid     BIGINT      NOT NULL,
    retailerid    INT         NOT NULL,
    countryid     INT         NOT NULL,
    channel_key   STRING      NOT NULL,
    channeldesc1  STRING,
    channeldesc2  STRING,
    channeldesc3  STRING,

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
