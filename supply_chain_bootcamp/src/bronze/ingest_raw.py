import argparse
import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
logging.basicConfig(level=logging.INFO)


def validate_identifier(value: str, name: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} must match [A-Za-z_][A-Za-z0-9_]* for safe table/schema creation"
        )
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw supply chain data into a Bronze Delta table with Auto Loader."
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
        "--source-file-pattern",
        required=True,
        help="File pattern for Auto Loader discovery (for example, sales_raw.jsonl)",
    )
    parser.add_argument(
        "--bronze-table",
        required=True,
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
    parser.add_argument(
        "--run-date",
        default="",
        help="Optional run date parameter passed by workflow tasks",
    )
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_bronze_pipeline").getOrCreate()

    catalog = validate_identifier(args.catalog, "catalog")
    schema = validate_identifier(args.schema, "schema")
    bronze_table = validate_identifier(args.bronze_table, "bronze_table")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    source_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", args.source_format)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", args.schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("pathGlobFilter", args.source_file_pattern)
        .load(args.source_path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_run_date", F.lit(args.run_date or None))
    )

    query = (
        source_df.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(availableNow=True)
        .outputMode("append")
        .toTable(f"{catalog}.{schema}.{bronze_table}")
    )
    query.awaitTermination()


if __name__ == "__main__":
    run_pipeline(parse_args())
