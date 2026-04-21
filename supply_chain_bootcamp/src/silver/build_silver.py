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
        df.filter(F.col("transaction_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .withColumn("sale_id", F.col("transaction_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("store_id", F.col("store_id").cast("string"))
        .withColumn("sale_ts", F.to_timestamp("timestamp"))
        .withColumn("item", F.explode_outer(F.col("payload.items")))
        .withColumn("sku", F.col("item.sku").cast("string"))
        .withColumn("quantity", F.col("item.qty").cast("int"))
        .withColumn("unit_price", F.col("item.price").cast("double"))
        .withColumn("order_total", F.col("payload.total").cast("double"))
        .withColumn("payment_method", F.col("payload.payment_method").cast("string"))
        .withColumn("line_amount", F.col("quantity") * F.col("unit_price"))
        .drop("item")
        .filter(F.col("sku").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") >= 0)
        .dropDuplicates(["sale_id", "sku"])
    )


def clean_inventory(df):
    return (
        df.filter(F.col("sku").isNotNull())
        .filter(F.col("store_id").isNotNull())
        .withColumn("sku", F.col("sku").cast("string"))
        .withColumn("store_id", F.col("store_id").cast("string"))
        .withColumn("status", F.col("status").cast("string"))
        .withColumn("stock_level", F.col("stock_level").cast("int"))
        .withColumn("snapshot_ts", F.to_timestamp("snapshot_time"))
        .filter(F.col("stock_level") >= 0)
        .dropDuplicates(["sku", "store_id", "snapshot_ts"])
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
        .withColumn(
            "_run_date",
            F.coalesce(F.col("_run_date").cast("string"), F.lit(run_date_value).cast("string")),
        )
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
