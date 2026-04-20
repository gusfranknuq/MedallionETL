# MedallionETL
End-to-end supply chain data pipeline using the Medallion architecture

## Supply chain Bronze ingestion (PySpark + Auto Loader)

Use `supply_chain_bronze_pipeline.py` to run a Unity Catalog-aware Bronze ingestion flow:

- Creates Unity Catalog schema: `supply_chain` (inside configurable catalog)
- Reads supply chain files from cloud storage with Auto Loader (`cloudFiles`)
- Enables `cloudFiles.inferColumnTypes`
- Enables schema evolution (`cloudFiles.schemaEvolutionMode=addNewColumns`)
- Streams data into a Bronze Delta table
