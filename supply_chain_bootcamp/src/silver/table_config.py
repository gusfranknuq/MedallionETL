"""Helpers for reading silver target table metadata.

The ``silver_table_config_l`` table describes each silver target's column
shape -- which columns are KEYs (used in the MERGE ``ON`` clause), which are
FACTs (updatable metrics), and which are AUDIT (lineage). Reading this at
runtime means transform_silver does not need a per-target hardcoded list of
columns and the pipeline configs do not need to repeat ``merge_keys`` /
``metric_columns``.

Audit columns are documented in the table for completeness but are NOT
returned by :func:`get_load_table_spec`; the loader hardcodes how to populate
``insjobid`` / ``modjobid`` / ``ins_ts`` / ``mod_ts``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


COLUMN_TYPE_KEY = "KEY"
COLUMN_TYPE_FACT = "FACT"
COLUMN_TYPE_AUDIT = "AUDIT"

VALID_COLUMN_TYPES = {COLUMN_TYPE_KEY, COLUMN_TYPE_FACT, COLUMN_TYPE_AUDIT}


@dataclass(frozen=True)
class SilverTableSpec:
    """Resolved metadata for one silver target table."""

    table_name: str
    key_columns: tuple[str, ...]    # ordered, used in MERGE ON
    fact_columns: tuple[str, ...]   # ordered, candidates for update/insert
    audit_columns: tuple[str, ...]  # documented audit cols (loader hardcodes writes)

    @property
    def all_data_columns(self) -> tuple[str, ...]:
        return self.key_columns + self.fact_columns


def get_load_table_spec(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table_name: str,
) -> SilverTableSpec:
    """Read silver_table_config_l and return the spec for ``table_name``.

    Rows are ordered by ``column_order`` so the returned tuples preserve the
    intended column order. Raises if no rows are found or if any row has an
    unknown ``column_type``.
    """
    config_fqn = f"{catalog}.{schema}.silver_table_config_l"
    if not spark.catalog.tableExists(config_fqn):
        raise ValueError(
            f"silver_table_config_l does not exist at {config_fqn}. "
            "Apply src/silver/ddl/silver_table_config_l.sql before running silver."
        )

    rows = (
        spark.table(config_fqn)
        .filter(F.col("table_name") == F.lit(table_name))
        .orderBy(F.col("column_order").asc_nulls_last(), F.col("column_name").asc())
        .select("column_name", "column_type")
        .collect()
    )

    if not rows:
        raise ValueError(
            f"No silver_table_config_l entries found for table_name='{table_name}'. "
            "Add rows for every KEY/FACT/AUDIT column of the target."
        )

    keys: list[str] = []
    facts: list[str] = []
    audits: list[str] = []
    for row in rows:
        ctype = (row["column_type"] or "").strip().upper()
        cname = row["column_name"]
        if ctype not in VALID_COLUMN_TYPES:
            raise ValueError(
                f"silver_table_config_l row for {table_name}.{cname} has unknown "
                f"column_type='{row['column_type']}'. Expected one of {sorted(VALID_COLUMN_TYPES)}."
            )
        if ctype == COLUMN_TYPE_KEY:
            keys.append(cname)
        elif ctype == COLUMN_TYPE_FACT:
            facts.append(cname)
        else:  # AUDIT
            audits.append(cname)

    if not keys:
        raise ValueError(
            f"silver_table_config_l for {table_name} defines no KEY columns; "
            "MERGE requires at least one key."
        )

    spec = SilverTableSpec(
        table_name=table_name,
        key_columns=tuple(keys),
        fact_columns=tuple(facts),
        audit_columns=tuple(audits),
    )
    logger.info(
        "Loaded silver table spec for %s: keys=%s facts=%s audits=%s",
        table_name, spec.key_columns, spec.fact_columns, spec.audit_columns,
    )
    return spec
