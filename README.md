# MedallionETL
End-to-end supply chain data pipeline using the Medallion architecture

## Supply chain Bronze ingestion (PySpark + Auto Loader)

Databricks bundle project root: `supply_chain_bootcamp`

- Bundle entrypoint: `supply_chain_bootcamp/databricks.yml`
- Job definition: `supply_chain_bootcamp/resources/job_definition.yml`
- PySpark ingest script: `supply_chain_bootcamp/src/bronze/ingest_raw.py`
- PySpark Silver transform script: `supply_chain_bootcamp/src/silver/build_silver.py`
- Set `workspace_host`, `source_path`, and per-stream Bronze variables (`sales_*`, `inventory_*`) in `supply_chain_bootcamp/databricks.yml`
- The default variable values in `databricks.yml` are templates/examples and must be replaced with your real Azure Databricks and storage values

The ingest script runs a Unity Catalog-aware Bronze ingestion flow:

- Creates Unity Catalog schema: `supply_chain` (inside configurable catalog)
- Reads supply chain files from cloud storage with Auto Loader (`cloudFiles`)
- Filters file discovery with `pathGlobFilter` so each stream only tracks its raw file pattern
- Enables `cloudFiles.inferColumnTypes`
- Enables schema evolution (`cloudFiles.schemaEvolutionMode=addNewColumns`)
- Uses `trigger(availableNow=True)` so each Bronze task finishes after available files are processed
- Streams data into Bronze Delta tables (`bronze_sales`, `bronze_inventory`) with separate schema/checkpoint tracking

## Silver transformations (PySpark DataFrame API)

The bundle defines separate Bronze and Silver jobs:

- `supply_chain_bronze_ingestion` for Auto Loader ingestion from raw files to Bronze
- `supply_chain_silver_transform` for DataFrame API cleanup from Bronze to Silver
- `supply_chain_medallion_orchestration` to run Bronze and then Silver in sequence

The Silver job uses DataFrame API only:

- Reads from Bronze tables (`bronze_sales`, `bronze_inventory`) as streaming sources
- Applies typed casts, null filtering, simple data quality filters, and deduplication
- Writes to Silver Delta tables (`silver_sales`, `silver_inventory`) with separate checkpoint locations
- Uses `trigger(availableNow=True)` to process available Bronze data and finish

You can run each layer independently, or run the orchestration job to execute layers in sequence.
