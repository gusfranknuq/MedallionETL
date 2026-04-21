CREATE SCHEMA IF NOT EXISTS ${var.catalog_name}.${var.schema_name};

CREATE OR REPLACE TABLE ${var.catalog_name}.${var.schema_name}.silver_sales AS
SELECT
  CAST(transaction_id AS STRING) AS transaction_id,
  to_timestamp(`timestamp`) AS sale_ts,
  CAST(store_id AS STRING) AS store_id,
  CAST(customer_id AS STRING) AS customer_id,
  CAST(item.sku AS STRING) AS sku,
  CAST(item.qty AS INT) AS quantity,
  CAST(item.price AS DOUBLE) AS unit_price,
  CAST(payload.payment_method AS STRING) AS payment_method,
  _ingest_ts,
  _source_file,
  _run_date,
  current_timestamp() AS _silver_ingest_ts
FROM ${var.catalog_name}.${var.schema_name}.bronze_sales
LATERAL VIEW explode(payload.items) exploded_items AS item
WHERE transaction_id IS NOT NULL
  AND store_id IS NOT NULL
  AND item.sku IS NOT NULL
  AND CAST(item.qty AS INT) > 0
  AND CAST(item.price AS DOUBLE) >= 0;
