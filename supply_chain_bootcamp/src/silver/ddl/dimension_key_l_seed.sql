-- Seed rows for dimension_key_l.
--
-- Each row says: "When building the natural key for <keyname> in scope
-- (retailerid, countryid), concatenate <keypart> at position <keyorder>."
--
-- The resulting <dim>_key (e.g. item_key, store_key) is built by
-- dim_handler as: parts[1] || '_' || parts[2] || ...  with empty-string
-- coalesce on each part.
--
-- The `keypart` values must match column names that exist on the stage
-- frame AFTER column_definitions are applied. The current sample sales
-- config projects `itemnbr`, `storenbr`, and `channel_lvl`, which is what
-- this seed assumes.
--
-- Auto-resolution: src/silver/transform_silver.py reads dimension_key_l
-- scoped to (retailerid, countryid) and runs dim_handler for any keyname
-- whose keyparts are all present on the stage frame -- there is no
-- per-config dimensions[] list anymore.
--
-- Add more rows as you onboard new dimensions or new (retailer, country)
-- scopes.

MERGE INTO {catalog}.{schema}.dimension_key_l AS t
USING (
    SELECT * FROM VALUES
        -- itemid: built from itemnbr
        (1, 1, 'itemid',    'itemnbr',     1),
        -- storeid: built from storenbr
        (1, 1, 'storeid',   'storenbr',    1),
        -- channelid: built from channel_lvl
        (1, 1, 'channelid', 'channel_lvl', 1)
    AS v(retailerid, countryid, keyname, keypart, keyorder)
) AS s
ON  t.retailerid = s.retailerid
AND t.countryid  = s.countryid
AND t.keyname    = s.keyname
AND t.keyorder   = s.keyorder
WHEN MATCHED THEN UPDATE SET
    t.keypart = s.keypart
WHEN NOT MATCHED THEN INSERT (
    retailerid, countryid, keyname, keypart, keyorder
) VALUES (
    s.retailerid, s.countryid, s.keyname, s.keypart, s.keyorder
);
