"""Custom per-source cleaners for the silver layer.

A "cleaner" is a function that takes the raw bronze ``DataFrame`` and returns
a cleaned, row-level stage frame ready to be mapped to the silver target via
``column_definitions`` in the pipeline config.

Cleaners are looked up by string name from :data:`CLEANER_REGISTRY` so that
configs stay declarative and we never ``eval`` arbitrary Python.

To add a new cleaner:

1. Implement ``def my_cleaner(df: DataFrame) -> DataFrame`` here.
2. Register it under a stable string key in :data:`CLEANER_REGISTRY`.
3. Reference that key as ``"custom_cleaner"`` in the pipeline config.
"""

from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# --------------------------------------------------------------------------- #
# Sales
# --------------------------------------------------------------------------- #

def unnest_sales_items(df: DataFrame) -> DataFrame:
    """Explode ``payload.items`` so each output row is one (transaction, sku).

    Produces typed columns the silver mapping can consume:
      transaction_id, sale_timestamp, store_id, customer_id, sku,
      sales_qty, unit_price, payment_method, sales_retail.
    """
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


def clean_sales(df: DataFrame) -> DataFrame:
    """Unnest sales items and drop rows that fail basic quality checks."""
    return (
        unnest_sales_items(df)
        .filter(F.col("transaction_id").isNotNull())
        .filter(F.col("sku").isNotNull())
        .filter(F.col("sales_qty") > 0)
        .filter(F.col("unit_price") >= 0)
        .dropDuplicates(["transaction_id", "sku"])
    )


# --------------------------------------------------------------------------- #
# Inventory
# --------------------------------------------------------------------------- #

def clean_inventory_snapshots(df: DataFrame) -> DataFrame:
    """Cast inventory snapshot columns and filter to valid rows."""
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
    )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #

CleanerFn = Callable[[DataFrame], DataFrame]

CLEANER_REGISTRY: dict[str, CleanerFn] = {
    "unnest_sales_items": unnest_sales_items,
    "clean_sales": clean_sales,
    "clean_inventory_snapshots": clean_inventory_snapshots,
}


def get_cleaner(name: str | None) -> CleanerFn:
    """Return the cleaner registered under ``name``, or a no-op if ``None``."""
    if name is None:
        return lambda df: df
    if name not in CLEANER_REGISTRY:
        raise ValueError(
            f"Unknown custom_cleaner '{name}'. "
            f"Known cleaners: {sorted(CLEANER_REGISTRY)}"
        )
    return CLEANER_REGISTRY[name]
