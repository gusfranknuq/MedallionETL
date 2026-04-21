CREATE SCHEMA IF NOT EXISTS ${var.catalog_name}.${var.schema_name};

CREATE OR REPLACE TABLE ${var.catalog_name}.${var.schema_name}.silver_inventory AS
SELECT
  to_timestamp(snapshot_time) AS snapshot_ts,
  CAST(store_id AS STRING) AS store_id,
  CAST(sku AS STRING) AS sku,
  CAST(stock_level AS INT) AS stock_level,
  UPPER(CAST(status AS STRING)) AS status,
  _ingest_ts,
  _source_file,
  _run_date,
  current_timestamp() AS _silver_ingest_ts
FROM ${var.catalog_name}.${var.schema_name}.bronze_inventory
WHERE store_id IS NOT NULL
  AND sku IS NOT NULL
  AND CAST(stock_level AS INT) >= 0;
