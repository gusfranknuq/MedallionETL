# MedallionETL
End-to-end supply chain data pipeline using the Medallion architecture

## Supply chain Bronze ingestion (SQL Warehouse + read_files)

Databricks bundle project root: `supply_chain_bootcamp`

- Bundle entrypoint: `supply_chain_bootcamp/databricks.yml`
- Job definition: `supply_chain_bootcamp/resources/job_definition.yml`
- SQL Bronze scripts: `supply_chain_bootcamp/src/sql/bronze_ingest_sales.sql`, `supply_chain_bootcamp/src/sql/bronze_ingest_inventory.sql`
- SQL Silver scripts: `supply_chain_bootcamp/src/sql/silver_transform_sales.sql`, `supply_chain_bootcamp/src/sql/silver_transform_inventory.sql`
- Set `workspace_host`, `sql_warehouse_id`, `source_path`, and per-stream file pattern variables in `supply_chain_bootcamp/databricks.yml`
- The default variable values in `databricks.yml` are templates/examples and must be replaced with your real Azure Databricks and storage values

The Bronze SQL tasks run a Unity Catalog-aware ingestion flow:

- Creates Unity Catalog schema: `supply_chain` (inside configurable catalog)
- Reads supply chain files from cloud storage with SQL `read_files`
- Filters each ingestion by file pattern so each Bronze table tracks only its raw dataset
- Adds ingestion metadata columns (`_ingest_ts`, `_source_file`, `_run_date`)
- Writes to Bronze Delta tables (`bronze_sales`, `bronze_inventory`) using SQL Warehouse compute

## Silver transformations (SQL)

The bundle defines separate Bronze and Silver jobs:

- `supply_chain_bronze_ingestion` for SQL-based ingestion from raw files to Bronze
- `supply_chain_silver_transform` for SQL cleanup from Bronze to Silver
- `supply_chain_medallion_orchestration` to run Bronze and then Silver in sequence

The Silver job uses SQL only:

- Reads from Bronze tables (`bronze_sales`, `bronze_inventory`)
- Applies typed casts, null filtering, flattening for sales line items, and simple data quality filters
- Writes to Silver Delta tables (`silver_sales`, `silver_inventory`) on SQL Warehouse compute

You can run each layer independently, or run the orchestration job to execute layers in sequence.
