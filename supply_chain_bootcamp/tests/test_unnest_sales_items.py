import pytest

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.silver.transform_silver import unnest_sales_items


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("supply-chain-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sales_schema():
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


def test_unnest_sales_items_creates_line_rows(spark):
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
    df = spark.createDataFrame(data, schema=_sales_schema())

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

    rows = sorted(result.collect(), key=lambda r: r.sku)

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


def test_unnest_sales_items_handles_missing_items_with_nulls(spark):
    data = [
        {
            "transaction_id": "tx-2",
            "customer_id": "c-2",
            "store_id": "s-2",
            "timestamp": "2026-01-01T11:00:00",
            "payload": {"items": None, "total": 0.0, "payment_method": None},
        }
    ]
    df = spark.createDataFrame(data, schema=_sales_schema())

    result = unnest_sales_items(df).select(
        "transaction_id", "sku", "sales_qty", "unit_price"
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].transaction_id == "tx-2"
    assert rows[0].sku is None
    assert rows[0].sales_qty is None
    assert rows[0].unit_price is None
