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
        description="Build Gold supply chain serving tables from Silver tables."
    )
    parser.add_argument("--catalog", default="supply_chain", help="Unity Catalog name")
    parser.add_argument("--schema", default="supply_chain", help="Unity Catalog schema name")
    parser.add_argument(
        "--entity",
        required=True,
        choices=["sales", "inventory"],
        help="Gold entity to build",
    )
    parser.add_argument(
        "--job-run-id",
        default=None,
        help="Optional Databricks job run id for lineage",
    )
    return parser.parse_args()


def build_gold_daily_sales(spark: SparkSession, catalog: str, schema: str, job_run_id: str | None):
    sales_df = spark.table(f"{catalog}.{schema}.silver_sales")
    output_df = (
        sales_df.withColumn("sale_date", F.to_date(F.col("sale_timestamp")))
        .groupBy("store_id", "sale_date")
        .agg(
            F.sum(F.col("sales_retail")).cast("double").alias("total_revenue"),
            F.sum(F.col("sales_qty")).cast("long").alias("units_sold"),
        )
        .withColumn("_job_run_id", F.lit(job_run_id).cast("string"))
        .select("store_id", "sale_date", "total_revenue", "units_sold", "_job_run_id")
    )

    (
        output_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.gold_daily_sales")
    )


def build_gold_low_stock_alerts(
    spark: SparkSession, catalog: str, schema: str, job_run_id: str | None
):
    inventory_df = spark.table(f"{catalog}.{schema}.silver_inventory")
    output_df = (
        inventory_df.filter(F.col("stock_level") < 10)
        .withColumn("_job_run_id", F.lit(job_run_id).cast("string"))
        .select(
            "latest_snapshot_time",
            "store_id",
            "sku",
            "stock_level",
            "status",
            "_job_run_id",
        )
    )

    (
        output_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.gold_low_stock_alerts")
    )


def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_gold_pipeline").getOrCreate()

    catalog = validate_identifier(args.catalog, "catalog")
    schema = validate_identifier(args.schema, "schema")

    if args.entity == "sales":
        build_gold_daily_sales(spark, catalog, schema, args.job_run_id)
    else:
        build_gold_low_stock_alerts(spark, catalog, schema, args.job_run_id)


if __name__ == "__main__":
    run_pipeline(parse_args())
