# MedallionETL
End-to-end supply chain data pipeline using the Medallion architecture

## Supply chain Bronze ingestion (PySpark + Auto Loader)

Databricks bundle project root: `supply_chain_bootcamp`

- Bundle entrypoint: `supply_chain_bootcamp/databricks.yml`
- Job definition: `supply_chain_bootcamp/resources/job_definition.yml`
- PySpark ingest script: `supply_chain_bootcamp/src/bronze/ingest_raw.py`
- Set `workspace_host`, `source_path`, `checkpoint_path`, and `schema_location` variables in `supply_chain_bootcamp/databricks.yml`

The ingest script runs a Unity Catalog-aware Bronze ingestion flow:

- Creates Unity Catalog schema: `supply_chain` (inside configurable catalog)
- Reads supply chain files from cloud storage with Auto Loader (`cloudFiles`)
- Enables `cloudFiles.inferColumnTypes`
- Enables schema evolution (`cloudFiles.schemaEvolutionMode=addNewColumns`)
- Streams data into a Bronze Delta table
