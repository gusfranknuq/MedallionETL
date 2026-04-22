import inspect
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import types as T

SCRIPT_PATH = Path(
    globals().get("__file__", inspect.currentframe().f_code.co_filename)
).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.silver.build_silver import unnest_sales_items


def sales_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("transaction_id", T.StringType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("store_id", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
            T.StructField(
                "payload",
                T.StructType(
                    [
                        T.StructField(
                            "items",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField("price", T.DoubleType(), True),
                                        T.StructField("qty", T.LongType(), True),
                                        T.StructField("sku", T.StringType(), True),
                                    ]
                                )
                            ),
                            True,
                        ),
                        T.StructField("total", T.DoubleType(), True),
                        T.StructField("payment_method", T.StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )


def test_creates_line_rows(spark: SparkSession) -> None:
    data = [
        {
            "transaction_id": "tx-1",
            "customer_id": "c-1",
            "store_id": "s-1",
            "timestamp": "2026-01-01T10:00:00",
            "payload": {
                "items": [
                    {"price": 3.5, "qty": 2, "sku": "sku-a"},
                    {"price": 1.0, "qty": 5, "sku": "sku-b"},
                ],
                "total": 12.0,
                "payment_method": "card",
            },
        }
    ]
    df = spark.createDataFrame(data, schema=sales_schema())

    result = unnest_sales_items(df).select(
        "transaction_id",
        "customer_id",
        "store_id",
        "sale_timestamp",
        "sku",
        "sales_qty",
        "unit_price",
        "payment_method",
        "sales_retail",
    )

    rows = sorted(result.collect(), key=lambda row: row.sku)

    assert len(rows) == 2
    assert rows[0].transaction_id == "tx-1"
    assert rows[0].customer_id == "c-1"
    assert rows[0].store_id == "s-1"
    assert rows[0].sale_timestamp is not None
    assert rows[0].sku == "sku-a"
    assert rows[0].sales_qty == 2
    assert rows[0].unit_price == 3.5
    assert rows[0].sales_retail == 7.0
    assert rows[0].payment_method == "card"

    assert rows[1].sku == "sku-b"
    assert rows[1].sales_qty == 5
    assert rows[1].unit_price == 1.0
    assert rows[1].sales_retail == 5.0


def test_handles_missing_items_with_nulls(spark: SparkSession) -> None:
    data = [
        {
            "transaction_id": "tx-2",
            "customer_id": "c-2",
            "store_id": "s-2",
            "timestamp": "2026-01-01T11:00:00",
            "payload": {"items": None, "total": 0.0, "payment_method": None},
        }
    ]
    df = spark.createDataFrame(data, schema=sales_schema())

    result = unnest_sales_items(df).select(
        "transaction_id", "sku", "sales_qty", "unit_price"
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].transaction_id == "tx-2"
    assert rows[0].sku is None
    assert rows[0].sales_qty is None
    assert rows[0].unit_price is None


def main() -> None:
    spark = SparkSession.builder.appName("supply-chain-unit-tests").getOrCreate()
    try:
        test_creates_line_rows(spark)
        test_handles_missing_items_with_nulls(spark)
        print("Databricks tests passed: unnest_sales_items")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
