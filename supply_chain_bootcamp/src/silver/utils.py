from __future__ import annotations

from pyspark.sql import DataFrame


def is_df_empty(frame: DataFrame) -> bool:
    return len(frame.take(1)) == 0
