"""Dimension handler for the silver layer.

Given a stage DataFrame coming out of bronze, ``dim_handler`` will:

1. Read ``dimension_key_l`` to discover which bronze columns make up the
   natural key for the requested dimension (scoped by retailerid /
   countryid).
2. Build the ``<dim>_key`` column on the stage DataFrame by concatenating
   the configured bronze key parts -- ordered by ``keyorder`` -- with
   ``'_'`` as separator.
3. Insert any brand-new (``<dim>_key``, retailerid, countryid) tuples into
   the silver dimension table, assigning a fresh surrogate id by offsetting
   ``monotonically_increasing_id()`` from the current ``MAX(<dim>id)``.
4. Join the dimension table back onto the stage DataFrame so the caller
   gets a DataFrame that has the surrogate id column (e.g. ``storeid``)
   populated and ready for downstream silver fact merges.

This function is intended to be the *first* stage of every silver
transformation that needs to resolve dimension surrogate ids.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Dimension metadata
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class DimensionSpec:
    """Static metadata describing one silver dimension table."""

    name: str           # logical dimension name, e.g. "store"
    table: str          # silver table name, e.g. "silver_store"
    id_col: str         # surrogate id column, e.g. "storeid"
    key_col: str        # natural key column, e.g. "store_key"
    keyname: str        # value of dimension_key_l.keyname, e.g. "storeid"
    display: str        # human-readable plural label, e.g. "Stores"
    desc_cols: tuple[str, ...] = ()   # optional descriptor columns to upsert


# Registry of supported dimensions. Add new dimensions here.
DIMENSION_REGISTRY: dict[str, DimensionSpec] = {
    "item": DimensionSpec(
        name="item",
        table="silver_item",
        id_col="itemid",
        key_col="item_key",
        keyname="itemid",
        display="Items",
        desc_cols=("itemdesc1", "itemdesc2", "itemdesc3"),
    ),
    "store": DimensionSpec(
        name="store",
        table="silver_store",
        id_col="storeid",
        key_col="store_key",
        keyname="storeid",
        display="Stores",
        desc_cols=("storedesc1", "storedesc2", "storedesc3"),
    ),
    "channel": DimensionSpec(
        name="channel",
        table="silver_channel",
        id_col="channelid",
        key_col="channel_key",
        keyname="channelid",
        display="Channels",
        desc_cols=("channeldesc1", "channeldesc2", "channeldesc3"),
    ),
    "retailer": DimensionSpec(
        name="retailer",
        table="silver_retailer",
        id_col="retailerid",
        key_col="retailer_key",
        keyname="retailerid",
        display="Retailers",
        desc_cols=("retailerdesc1", "retailerdesc2", "retailerdesc3"),
    ),
    "country": DimensionSpec(
        name="country",
        table="silver_country",
        id_col="countryid",
        key_col="country_key",
        keyname="countryid",
        display="Countries",
        desc_cols=("countrydesc1", "countrydesc2", "countrydesc3"),
    ),
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _qualify(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _load_key_parts(
    spark: SparkSession,
    catalog: str,
    schema: str,
    keyname: str,
    retailerid: int,
    countryid: int,
) -> list[str]:
    """Return the ordered list of bronze columns that compose ``<dim>_key``."""
    rows = (
        spark.table(_qualify(catalog, schema, "dimension_key_l"))
        .filter(F.col("keyname") == F.lit(keyname))
        .filter(F.col("retailerid") == F.lit(retailerid))
        .filter(F.col("countryid") == F.lit(countryid))
        .orderBy(F.col("keyorder").asc())
        .select("keypart")
        .collect()
    )
    if not rows:
        raise ValueError(
            f"No dimension_key_l entries found for keyname='{keyname}', "
            f"retailerid={retailerid}, countryid={countryid}"
        )
    return [r["keypart"] for r in rows]


def _build_key_column(df: DataFrame, parts: Iterable[str], key_col: str) -> DataFrame:
    """Add ``key_col`` = parts[0] || '_' || parts[1] || ... cast to string."""
    parts_list = list(parts)
    missing = [p for p in parts_list if p not in df.columns]
    if missing:
        raise ValueError(
            f"Stage DataFrame is missing key part column(s) {missing} "
            f"required to build {key_col}"
        )
    cast_parts = [F.coalesce(F.col(p).cast("string"), F.lit("")) for p in parts_list]
    return df.withColumn(key_col, F.concat_ws("_", *cast_parts))


def _next_id_offset(spark: SparkSession, table_fqn: str, id_col: str) -> int:
    """Return ``MAX(id_col) + 1`` from the dimension table, or 1 if empty/new."""
    if not spark.catalog.tableExists(table_fqn):
        return 1
    row = spark.table(table_fqn).agg(F.max(F.col(id_col)).alias("m")).collect()[0]
    current_max = row["m"]
    return int(current_max) + 1 if current_max is not None else 1


def _ensure_dim_table_exists(
    spark: SparkSession,
    table_fqn: str,
    template_df: DataFrame,
) -> None:
    """Create the dimension table from an empty DF if it does not yet exist.

    DDL files in ``src/silver/ddl/`` are the source of truth, but this guard
    keeps unit tests and ad-hoc runs from blowing up when the table has not
    been pre-created.
    """
    if not spark.catalog.tableExists(table_fqn):
        logger.info("Dimension table %s missing; creating empty Delta table", table_fqn)
        template_df.limit(0).write.format("delta").saveAsTable(table_fqn)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def dim_handler(
    stage_df: DataFrame,
    dimension: str,
    catalog: str,
    schema: str,
    retailerid: int,
    countryid: int,
    job_run_id: str | None = None,
) -> DataFrame:
    """Resolve a dimension surrogate id onto ``stage_df``.

    Parameters
    ----------
    stage_df : DataFrame
        The post-bronze stage frame. Must already contain ``retailerid`` and
        ``countryid`` columns plus the bronze columns referenced in
        ``dimension_key_l`` for this dimension.
    dimension : str
        Logical dimension name -- one of :data:`DIMENSION_REGISTRY`.
    catalog, schema : str
        Unity Catalog location for the silver dimension and dimension_key_l.
    retailerid, countryid : int
        Scope used both to look up the key recipe and to merge into the
        dimension table.
    job_run_id : str | None
        Databricks job run id for lineage; written to ``insjobid`` for new
        rows and ``modjobid`` for descriptor updates.

    Returns
    -------
    DataFrame
        ``stage_df`` augmented with the dimension's natural ``<dim>_key`` and
        surrogate ``<dim>id`` columns.
    """
    if dimension not in DIMENSION_REGISTRY:
        raise ValueError(
            f"Unknown dimension '{dimension}'. "
            f"Known: {sorted(DIMENSION_REGISTRY)}"
        )
    spec = DIMENSION_REGISTRY[dimension]
    spark = stage_df.sparkSession
    table_fqn = _qualify(catalog, schema, spec.table)

    # 1. Load key recipe and build the natural key on the stage frame.
    key_parts = _load_key_parts(
        spark, catalog, schema, spec.keyname, retailerid, countryid
    )
    logger.info(
        "dim_handler[%s]: building %s from parts %s (retailerid=%s, countryid=%s)",
        dimension, spec.key_col, key_parts, retailerid, countryid,
    )
    staged = _build_key_column(stage_df, key_parts, spec.key_col)

    # Make sure scope columns are present for the dimension merge.
    if "retailerid" not in staged.columns:
        staged = staged.withColumn("retailerid", F.lit(retailerid).cast("int"))
    if "countryid" not in staged.columns:
        staged = staged.withColumn("countryid", F.lit(countryid).cast("int"))

    # 2. Identify candidate dimension rows (distinct on key + scope).
    desc_present = [c for c in spec.desc_cols if c in staged.columns]
    candidate_cols = [spec.key_col, "retailerid", "countryid", *desc_present]
    candidates = (
        staged.select(*[F.col(c) for c in candidate_cols])
        .filter(F.col(spec.key_col).isNotNull() & (F.col(spec.key_col) != F.lit("")))
        .dropDuplicates([spec.key_col, "retailerid", "countryid"])
    )

    # 3. Determine which candidates are new and assign surrogate ids.
    template_for_create = candidates.select(
        F.lit(None).cast("bigint").alias(spec.id_col),
        F.col("retailerid").cast("int").alias("retailerid"),
        F.col("countryid").cast("int").alias("countryid"),
        F.col(spec.key_col).cast("string").alias(spec.key_col),
        *[F.lit(None).cast("string").alias(c) for c in spec.desc_cols],
        F.lit(None).cast("string").alias("insjobid"),
        F.lit(None).cast("string").alias("modjobid"),
        F.lit(None).cast("timestamp").alias("ins_ts"),
        F.lit(None).cast("timestamp").alias("mod_ts"),
    )
    _ensure_dim_table_exists(spark, table_fqn, template_for_create)

    existing = spark.table(table_fqn).select(
        spec.key_col, "retailerid", "countryid", spec.id_col,
    )
    new_rows = candidates.join(
        existing, [spec.key_col, "retailerid", "countryid"], "left_anti"
    )

    if not new_rows.isEmpty():
        offset = _next_id_offset(spark, table_fqn, spec.id_col)
        # monotonically_increasing_id() is non-contiguous across partitions,
        # so collapse to a single partition to get a dense 0..N-1 sequence
        # that we then offset by the current max id.
        numbered = (
            new_rows.coalesce(1)
            .withColumn("_seq", F.monotonically_increasing_id())
            .withColumn(spec.id_col, (F.col("_seq") + F.lit(offset)).cast("bigint"))
            .drop("_seq")
        )

        insert_df = numbered.select(
            F.col(spec.id_col),
            F.col("retailerid").cast("int").alias("retailerid"),
            F.col("countryid").cast("int").alias("countryid"),
            F.col(spec.key_col),
            *[
                (F.col(c) if c in numbered.columns
                 else F.lit(None).cast("string")).alias(c)
                for c in spec.desc_cols
            ],
            F.lit(job_run_id).cast("string").alias("insjobid"),
            F.lit(job_run_id).cast("string").alias("modjobid"),
            F.current_timestamp().alias("ins_ts"),
            F.current_timestamp().alias("mod_ts"),
        )

        logger.info(
            "dim_handler[%s]: inserting %d new rows into %s starting at id=%d",
            dimension, insert_df.count(), table_fqn, offset,
        )
        # Use MERGE (not append) so concurrent runs cannot duplicate keys.
        target = DeltaTable.forName(spark, table_fqn)
        merge_cond = (
            f"t.{spec.key_col} = s.{spec.key_col} "
            f"AND t.retailerid = s.retailerid "
            f"AND t.countryid = s.countryid"
        )
        (
            target.alias("t")
            .merge(insert_df.alias("s"), merge_cond)
            .whenNotMatchedInsertAll()
            .execute()
        )

    # 4. Join the (now complete) dimension back onto the stage frame.
    dim_lookup = spark.table(table_fqn).select(
        spec.id_col, spec.key_col, "retailerid", "countryid",
    )
    enriched = staged.join(
        dim_lookup, [spec.key_col, "retailerid", "countryid"], "left"
    )
    return enriched
