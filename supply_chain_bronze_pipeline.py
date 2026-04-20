import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{args.catalog}`.`{args.schema}`")

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

    (
        source_df.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .toTable(f"`{args.catalog}`.`{args.schema}`.`{args.bronze_table}`")
    )


if __name__ == "__main__":
    run_pipeline(parse_args())
