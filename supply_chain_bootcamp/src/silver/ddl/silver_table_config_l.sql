-- Lookup table that describes the column shape of every silver target table.
-- Used by transform_silver to discover MERGE keys and updatable fact columns
-- without having to parse DDL or hardcode column lists per target.
--
-- Composite primary key: (table_name, column_name)
--
-- column_type values:
--   KEY    -- part of the natural/business primary key. Used in MERGE ON.
--             Loader requires every KEY column to be present on the stage frame.
--   FACT   -- a metric/value column. Updated on match, inserted on miss.
--             Different sources (sales vs inventory) typically supply different
--             subsets of FACT columns; ones not provided are left untouched.
--   AUDIT  -- lineage/audit column (insjobid, modjobid, ins_ts, mod_ts).
--             Documented here for completeness but the loader hardcodes how
--             to populate them.
--
-- This table is manually maintained alongside the silver target DDL files.
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_table_config_l (
    table_name   STRING  NOT NULL,
    column_name  STRING  NOT NULL,
    column_type  STRING  NOT NULL,
    column_order INT
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- Seed entries for silver_day_item_store_tyly.
-- Keep this in sync with src/silver/ddl/silver_day_item_store_tyly.sql.
MERGE INTO {catalog}.{schema}.silver_table_config_l AS t
USING (
    SELECT * FROM VALUES
        -- KEYs (composite business key)
        ('silver_day_item_store_tyly', 'day',         'KEY',   1),
        ('silver_day_item_store_tyly', 'itemid',      'KEY',   2),
        ('silver_day_item_store_tyly', 'storeid',     'KEY',   3),
        ('silver_day_item_store_tyly', 'channelid',   'KEY',   4),
        ('silver_day_item_store_tyly', 'customerid',  'KEY',   5),
        ('silver_day_item_store_tyly', 'retailerid',  'KEY',   6),
        ('silver_day_item_store_tyly', 'countryid',   'KEY',   7),

        -- AUDIT
        ('silver_day_item_store_tyly', 'insjobid',    'AUDIT', 10),
        ('silver_day_item_store_tyly', 'modjobid',    'AUDIT', 11),
        ('silver_day_item_store_tyly', 'ins_ts',      'AUDIT', 12),
        ('silver_day_item_store_tyly', 'mod_ts',      'AUDIT', 13),

        -- FACTs (this year / last year metric pairs)
        ('silver_day_item_store_tyly', 'posqty',      'FACT',  20),
        ('silver_day_item_store_tyly', 'posqtyly',    'FACT',  21),
        ('silver_day_item_store_tyly', 'possales',    'FACT',  22),
        ('silver_day_item_store_tyly', 'possalesly',  'FACT',  23),
        ('silver_day_item_store_tyly', 'unitprice',   'FACT',  24),
        ('silver_day_item_store_tyly', 'unitpricely', 'FACT',  25),
        ('silver_day_item_store_tyly', 'onhandqty',   'FACT',  26),
        ('silver_day_item_store_tyly', 'onhandqtyly', 'FACT',  27),
        ('silver_day_item_store_tyly', 'instock',     'FACT',  28),
        ('silver_day_item_store_tyly', 'instockly',   'FACT',  29)
    AS s(table_name, column_name, column_type, column_order)
) AS s
ON  t.table_name  = s.table_name
AND t.column_name = s.column_name
WHEN MATCHED THEN UPDATE SET column_type = s.column_type, column_order = s.column_order
WHEN NOT MATCHED THEN INSERT (table_name, column_name, column_type, column_order)
                      VALUES (s.table_name, s.column_name, s.column_type, s.column_order);
