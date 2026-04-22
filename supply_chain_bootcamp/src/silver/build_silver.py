import argparse
import logging
import re

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
        "--job-run-id",
        default=None,
        help="Optional Databricks job run id for lineage",
    )
    return parser.parse_args()


def clean_sales(df):
    return (
        unnest_sales_items(df)
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("sku").isNotNull())
        .filter(F.col("sales_qty") > 0)
        .filter(F.col("unit_price") >= 0)
        .dropDuplicates(["transaction_id", "sku"])
        .select(
            "transaction_id",
            "sale_timestamp",
            "store_id",
            "customer_id",
            "sku",
            "payment_method",
            "sales_qty",
            "sales_retail",
            "unit_price",
        )
    )


def unnest_sales_items(df):
    return (
        df.withColumn("transaction_id", F.col("transaction_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("store_id", F.col("store_id").cast("string"))
        .withColumn("sale_timestamp", F.to_timestamp("timestamp"))
        .withColumn("item", F.explode_outer(F.col("payload.items")))
        .withColumn("sku", F.col("item.sku").cast("string"))
        .withColumn("sales_qty", F.col("item.qty").cast("int"))
        .withColumn("unit_price", F.col("item.price").cast("double"))
        .withColumn("payment_method", F.col("payload.payment_method").cast("string"))
        .withColumn("sales_retail", F.col("sales_qty") * F.col("unit_price"))
        .drop("item")
    )


def clean_inventory(df):
    return (
        df.withColumn("latest_snapshot_time", F.to_timestamp("snapshot_time"))
        .withColumn("store_id", F.col("store_id").cast("string"))
        .withColumn("sku", F.col("sku").cast("string"))
        .withColumn("stock_level", F.col("stock_level").cast("int"))
        .withColumn("status", F.col("status").cast("string"))
        .filter(F.col("sku").isNotNull())
        .filter(F.col("store_id").isNotNull())
        .filter(F.col("latest_snapshot_time").isNotNull())
        .filter(F.col("stock_level") >= 0)
        .dropDuplicates(["sku", "store_id", "latest_snapshot_time"])
        .select("latest_snapshot_time", "store_id", "sku", "stock_level", "status")
    )


def merge_inventory_batch(batch_df, catalog, schema, silver_table, job_run_id):
    spark = batch_df.sparkSession
    table_name = f"{catalog}.{schema}.{silver_table}"

    prepared_df = clean_inventory(batch_df).withColumn(
        "_job_run_id", F.lit(job_run_id).cast("string")
    )

    latest_window = Window.partitionBy("sku", "store_id").orderBy(
        F.col("latest_snapshot_time").desc()
    )
    latest_df = (
        prepared_df.withColumn("_latest_rank", F.row_number().over(latest_window))
        .filter(F.col("_latest_rank") == 1)
        .drop("_latest_rank")
    )

    if not spark.catalog.tableExists(table_name):
        latest_df.limit(0).write.format("delta").saveAsTable(table_name)

    target = DeltaTable.forName(spark, table_name)
    (
        target.alias("t")
        .merge(latest_df.alias("s"), "t.sku = s.sku AND t.store_id = s.store_id")
        .whenMatchedUpdateAll(
            condition="s.latest_snapshot_time >= t.latest_snapshot_time"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_silver_pipeline").getOrCreate()

    catalog = validate_identifier(args.catalog, "catalog")
    schema = validate_identifier(args.schema, "schema")
    entity = args.entity

    bronze_table = f"bronze_{entity}"
    silver_table = f"silver_{entity}"

    source_df = spark.readStream.table(f"{catalog}.{schema}.{bronze_table}")

    if entity == "sales":
        query = (
            clean_sales(source_df)
            .withColumn("_job_run_id", F.lit(args.job_run_id).cast("string"))
            .writeStream.format("delta")
            .option("checkpointLocation", args.checkpoint_path)
            .trigger(availableNow=True)
            .outputMode("append")
            .toTable(f"{catalog}.{schema}.{silver_table}")
        )
    else:
        query = (
            source_df.writeStream.option("checkpointLocation", args.checkpoint_path)
            .trigger(availableNow=True)
            .foreachBatch(
                lambda batch_df, _batch_id: merge_inventory_batch(
                    batch_df,
                    catalog,
                    schema,
                    silver_table,
                    args.job_run_id,
                )
            )
            .start()
        )
    query.awaitTermination()


if __name__ == "__main__":
    run_pipeline(parse_args())
