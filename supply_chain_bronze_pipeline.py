import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def quote_identifier(value: str, name: str) -> str:
    if not value or not value.strip():
        raise ValueError(f"{name} cannot be empty")
    return f"`{value.replace('`', '``')}`"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream supply chain data into a Bronze Delta table with Auto Loader."
    )
    parser.add_argument("--catalog", default="main", help="Unity Catalog name")
    parser.add_argument("--schema", default="supply_chain", help="Unity Catalog schema name")
    parser.add_argument(
        "--source-path",
        required=True,
        help="Cloud storage URI for incoming supply chain files (s3://, abfss://, gs://, etc.)",
    )
    parser.add_argument("--source-format", default="json", help="Input file format")
    parser.add_argument(
        "--bronze-table",
        default="bronze_supply_chain",
        help="Bronze Delta table name",
    )
    parser.add_argument(
        "--checkpoint-path",
        required=True,
        help="Cloud storage checkpoint path for the streaming query",
    )
    parser.add_argument(
        "--schema-location",
        required=True,
        help="Cloud storage path where Auto Loader persists inferred schema",
    )
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_bronze_pipeline").getOrCreate()

    catalog = quote_identifier(args.catalog, "catalog")
    schema = quote_identifier(args.schema, "schema")
    bronze_table = quote_identifier(args.bronze_table, "bronze_table")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    source_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", args.source_format)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", args.schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(args.source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    query = (
        source_df.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .toTable(f"{catalog}.{schema}.{bronze_table}")
    )
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Gracefully shutting down streaming query...")
        query.stop()


if __name__ == "__main__":
    run_pipeline(parse_args())
