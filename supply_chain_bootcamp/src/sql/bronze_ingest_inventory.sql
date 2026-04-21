CREATE SCHEMA IF NOT EXISTS ${var.catalog_name}.${var.schema_name};

CREATE OR REPLACE TABLE ${var.catalog_name}.${var.schema_name}.bronze_inventory AS
SELECT
  inventory_data.*,
  current_timestamp() AS _ingest_ts,
  _metadata.file_path AS _source_file,
  NULLIF('{{job.parameters.run_date}}', '') AS _run_date
FROM read_files(
  '${var.source_path}/${var.inventory_file_pattern}',
  format => 'json',
  inferSchema => true
) AS inventory_data;
