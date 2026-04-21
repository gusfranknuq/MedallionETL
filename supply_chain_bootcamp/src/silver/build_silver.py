import argparse
import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RUN_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
logging.basicConfig(level=logging.INFO)


def validate_identifier(value: str, name: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} must match [A-Za-z_][A-Za-z0-9_]* for safe table/schema creation"
        )
    return value


def validate_run_date(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    if not RUN_DATE_PATTERN.match(value):
        raise ValueError("run_date must be in YYYY-MM-DD format")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform Bronze supply chain tables into Silver Delta tables with DataFrame API."
    )
    parser.add_argument("--catalog", default="supply_chain", help="Unity Catalog name")
    parser.add_argument("--schema", default="supply_chain", help="Unity Catalog schema name")
    parser.add_argument(
        "--entity",
        required=True,
        choices=["sales", "inventory"],
        help="Domain entity to clean into Silver layer",
    )
    parser.add_argument(
        "--checkpoint-path",
        required=True,
        help="Checkpoint location for this Silver streaming query",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Optional run date parameter passed by workflow tasks",
    )
    return parser.parse_args()


def clean_sales(df):
    return (
        df.filter(F.col("sale_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .withColumn("sale_id", F.col("sale_id").cast("string"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("sale_ts", F.to_timestamp("sale_ts"))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") >= 0)
        .dropDuplicates(["sale_id"])
    )


def clean_inventory(df):
    return (
        df.filter(F.col("inventory_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .withColumn("inventory_id", F.col("inventory_id").cast("string"))
        .withColumn("product_id", F.col("product_id").cast("string"))
        .withColumn("location_id", F.col("location_id").cast("string"))
        .withColumn("on_hand_qty", F.col("on_hand_qty").cast("int"))
        .withColumn("updated_ts", F.to_timestamp("updated_ts"))
        .filter(F.col("on_hand_qty") >= 0)
        .dropDuplicates(["inventory_id"])
    )


def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_silver_pipeline").getOrCreate()

    catalog = validate_identifier(args.catalog, "catalog")
    schema = validate_identifier(args.schema, "schema")
    entity = args.entity
    run_date_value = validate_run_date(args.run_date)

    bronze_table = f"bronze_{entity}"
    silver_table = f"silver_{entity}"

    source_df = spark.readStream.table(f"{catalog}.{schema}.{bronze_table}")

    if entity == "sales":
        transformed_df = clean_sales(source_df)
    else:
        transformed_df = clean_inventory(source_df)

    output_df = (
        transformed_df.withColumn("_silver_ingest_ts", F.current_timestamp())
        .withColumn("_run_date", F.lit(run_date_value))
    )

    query = (
        output_df.writeStream.format("delta")
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(availableNow=True)
        .outputMode("append")
        .toTable(f"{catalog}.{schema}.{silver_table}")
    )
    query.awaitTermination()


if __name__ == "__main__":
    run_pipeline(parse_args())
