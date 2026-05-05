"""Config-driven silver transform pipeline.

Mirrors the bronze ``run_pipeline`` shape: each source file/entity is
described by a JSON pipeline config (``resources/pipeline_config.*.json``),
and this module reads the ``silver`` block to:

1. Stream from the configured bronze table.
2. Apply the configured ``custom_cleaner`` (e.g. unnest sales items).
3. Evaluate ``column_definitions`` to project the cleaned frame onto the
   silver target schema.
4. Resolve dimension surrogate ids via :func:`dim_handler.dim_handler`.
5. MERGE into the configured silver target. The MERGE keys and the set of
   updatable fact columns are read from ``silver_table_config_l`` -- the
   pipeline config does not duplicate that knowledge. Only the FACT columns
   that are actually present on the stage frame are updated, so e.g. an
   inventory file does not clobber sales metrics with NULLs (and vice versa).
"""

from __future__ import annotations

import argparse
import inspect
import logging
import re
import sys
from pathlib import Path
from typing import Any

# Bundle-root bootstrap: when Databricks runs this file via exec(compile(...))
# as a spark_python_task, __file__ is *not* defined and only the script's
# directory is on sys.path. Resolve this module's path through inspect (which
# works under exec) and prepend the bundle root (two parents up:
# src/silver/ -> src/ -> bundle root) so 'from src.bronze.ingest_bronze import
# ...' style imports resolve.
def _resolve_module_path() -> Path:
    file_from_globals = globals().get("__file__")
    if file_from_globals:
        return Path(file_from_globals).resolve()
    inspected = inspect.getsourcefile(_resolve_module_path) or inspect.getfile(
        _resolve_module_path
    )
    if inspected:
        return Path(inspected).resolve()
    raise RuntimeError("Cannot determine transform_silver.py path for sys.path bootstrap")


_BUNDLE_ROOT = _resolve_module_path().parents[2]
if str(_BUNDLE_ROOT) not in sys.path:
    sys.path.insert(0, str(_BUNDLE_ROOT))

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Reuse bronze's config loader so the two layers stay in lockstep.
from src.bronze.ingest_bronze import (
    _load_json_with_resolved_path,
    _parse_task_config_map,
)
from src.silver.cleaners import get_cleaner
from src.silver.dim_handler import DIMENSION_REGISTRY, dim_handler
from src.silver.table_config import SilverTableSpec, get_load_table_spec, get_load_table_spec
from src.silver.utils import is_df_empty


IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
from src.common.logging_utils import configure_project_logging

configure_project_logging()
logger = logging.getLogger(__name__)


# Re-exported for backwards compatibility with existing tests.
from src.silver.cleaners import unnest_sales_items  # noqa: E402, F401


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def validate_identifier(value: str, name: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"{name} must match [A-Za-z_][A-Za-z0-9_]* for safe table/schema creation"
        )
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Config-driven Silver transform from a Bronze table to a Silver fact table."
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Path to a pipeline JSON config that contains a 'silver' block.",
    )
    parser.add_argument(
        "--task-name",
        default=None,
        help="Task name used to look up a pipeline config from the task-to-config map.",
    )
    parser.add_argument(
        "--task-config-map-path",
        default=None,
        help="Optional override path for the task-to-config JSON map.",
    )
    parser.add_argument("--catalog", default="supply_chain", help="Unity Catalog name")
    parser.add_argument("--schema", default="supply_chain", help="Unity Catalog schema name")
    parser.add_argument(
        "--job-run-id",
        default=None,
        help="Optional Databricks job run id for lineage",
    )
    return parser.parse_args()


# --------------------------------------------------------------------------- #
# Config resolution
# --------------------------------------------------------------------------- #

def _resolve_silver_config(args: argparse.Namespace) -> dict[str, Any]:
    selected_config_path = args.config_path
    if not selected_config_path and args.task_name:
        task_config_map = _parse_task_config_map(args.task_config_map_path)
        selected_config_path = task_config_map.get(args.task_name)
        if not selected_config_path:
            known = ", ".join(sorted(task_config_map.keys()))
            raise ValueError(
                f"Task name '{args.task_name}' is not configured in the task config map. "
                f"Known tasks: {known}"
            )

    if not selected_config_path:
        raise ValueError(
            "transform_silver requires either --config-path or --task-name to locate "
            "a pipeline config with a 'silver' block."
        )

    config = _load_json_with_resolved_path(selected_config_path)
    if not isinstance(config, dict):
        raise ValueError("Pipeline config must be a JSON object")

    silver_cfg = config.get("silver")
    if not isinstance(silver_cfg, dict):
        raise ValueError(
            f"Pipeline config '{selected_config_path}' has no 'silver' block; "
            "transform_silver cannot run."
        )

    required = [
        "source_bronze_table",
        "silver_table",
        "checkpoint_path",
        "scope",
        "column_definitions",
    ]
    for key in required:
        if key not in silver_cfg:
            raise ValueError(f"silver config is missing required field: {key}")

    scope = silver_cfg["scope"]
    if not isinstance(scope, dict) or not all(
        k in scope for k in ("retailerid", "countryid", "customerid")
    ):
        raise ValueError(
            "silver.scope must be an object with retailerid, countryid, and customerid"
        )

    return {
        "catalog": config.get("catalog", args.catalog),
        "schema": config.get("schema", args.schema),
        "source_bronze_table": silver_cfg["source_bronze_table"],
        "silver_table": silver_cfg["silver_table"],
        "custom_cleaner": silver_cfg.get("custom_cleaner"),
        "checkpoint_path": silver_cfg["checkpoint_path"],
        "scope": {
            "retailerid": int(scope["retailerid"]),
            "countryid": int(scope["countryid"]),
            "customerid": int(scope["customerid"]),
        },
        "dimensions": silver_cfg.get("dimensions", []),  # deprecated, ignored
        "column_definitions": silver_cfg["column_definitions"],
        "job_run_id": args.job_run_id,
    }


# --------------------------------------------------------------------------- #
# Stage-frame helpers
# --------------------------------------------------------------------------- #

# Spark cast targets allowed in column_definitions[].default_value.type.
# 'sql' is the escape hatch -- value is treated as a Spark SQL fragment
# (e.g. 'current_timestamp()'). All others wrap the value in F.lit() and
# cast to the named Spark type.
_DEFAULT_VALUE_CAST_TYPES = {
    "string", "int", "bigint", "double", "boolean", "date", "timestamp",
}
_DEFAULT_VALUE_TYPES = _DEFAULT_VALUE_CAST_TYPES | {"sql"}


def _resolve_default_value(target: str, default_value: Any) -> Any:
    """Convert a column_definitions[].default_value spec into a Spark Column.

    Spec shape:
        {"type": "<type>", "value": <json scalar or sql string>}
    where <type> is one of _DEFAULT_VALUE_TYPES. For 'sql', value is wrapped
    in F.expr; for everything else it is wrapped in F.lit and cast.
    """
    if not isinstance(default_value, dict):
        raise ValueError(
            f"column_definitions[{target}].default_value must be an object "
            f"with 'type' and 'value' fields; got {default_value!r}"
        )
    if "type" not in default_value or "value" not in default_value:
        raise ValueError(
            f"column_definitions[{target}].default_value must have both "
            f"'type' and 'value'; got keys {sorted(default_value)}"
        )

    dv_type = default_value["type"]
    dv_value = default_value["value"]
    if dv_type not in _DEFAULT_VALUE_TYPES:
        raise ValueError(
            f"column_definitions[{target}].default_value.type='{dv_type}' "
            f"is not one of {sorted(_DEFAULT_VALUE_TYPES)}"
        )

    if dv_type == "sql":
        if not isinstance(dv_value, str):
            raise ValueError(
                f"column_definitions[{target}].default_value.value must be "
                f"a SQL string when type='sql'; got {dv_value!r}"
            )
        return F.expr(dv_value)
    return F.lit(dv_value).cast(dv_type)


def _apply_column_definitions(
    df: DataFrame,
    column_definitions: list[dict[str, Any]],
) -> DataFrame:
    """Add each ``target`` column to ``df``.

    Two modes per entry:
      * ``expr`` is the literal string ``"DEFAULT"`` -> use ``default_value``.
        ``default_value`` is required in this mode.
      * Otherwise ``expr`` is a Spark SQL expression evaluated against ``df``.
        ``default_value`` is ignored in this mode.
    """
    out = df
    for entry in column_definitions:
        target = entry["target"]
        expr = entry["expr"]
        if expr == "DEFAULT":
            if "default_value" not in entry:
                raise ValueError(
                    f"column_definitions[{target}] has expr='DEFAULT' but no "
                    "default_value; cannot resolve column."
                )
            column = _resolve_default_value(target, entry["default_value"])
        else:
            column = F.expr(expr)
        out = out.withColumn(target, column)
    return out


def _attach_scope_columns(
    df: DataFrame, retailerid: int, countryid: int, customerid: int
) -> DataFrame:
    if "retailerid" not in df.columns:
        df = df.withColumn("retailerid", F.lit(retailerid).cast("int"))
    if "countryid" not in df.columns:
        df = df.withColumn("countryid", F.lit(countryid).cast("int"))
    if "customerid" not in df.columns:
        df = df.withColumn("customerid", F.lit(customerid).cast("bigint"))
    return df


def _ensure_silver_target_exists(
    spark: SparkSession,
    table_fqn: str,
    stage_with_audit: DataFrame,
) -> None:
    if not spark.catalog.tableExists(table_fqn):
        logger.info("Silver target %s missing; creating empty Delta table", table_fqn)
        stage_with_audit.limit(0).write.format("delta").saveAsTable(table_fqn)


def _last_operation_metrics(spark: SparkSession, table_fqn: str) -> dict[str, str]:
    """Return Delta operationMetrics from the most recent commit on a table.

    Used to log how many rows the latest MERGE / WRITE actually touched
    without re-scanning the data ourselves.
    """
    try:
        history_row = (
            DeltaTable.forName(spark, table_fqn)
            .history(1)
            .select("operationMetrics")
            .collect()[0]
        )
        return dict(history_row["operationMetrics"] or {})
    except Exception as exc:  # pragma: no cover - logging best-effort
        logger.warning("Could not read Delta history for %s: %s", table_fqn, exc)
        return {}


def _dimensions_to_resolve(
    spark: SparkSession,
    catalog: str,
    schema: str,
    retailerid: int,
    countryid: int,
    stage_columns: list[str],
) -> list[tuple[str, list[str]]]:
    """Discover which dimensions can be resolved against the current stage frame.

    Reads ``dimension_key_l`` filtered to ``(retailerid, countryid)``, groups
    rows by ``keyname``, and returns ``(dimension_name, keyparts)`` pairs for
    every keyname whose ``keypart`` columns are *all* present on
    ``stage_columns``. ``keyparts`` is the ordered list of source columns
    that compose the natural key (used for human-readable logging).
    Keynames not registered in DIMENSION_REGISTRY are skipped with a
    warning. Keynames whose keyparts are not yet projected onto the stage
    frame are silently skipped (the source simply does not contribute to
    that dimension).
    """
    rows = (
        spark.table(f"{catalog}.{schema}.dimension_key_l")
        .filter(F.col("retailerid") == F.lit(retailerid))
        .filter(F.col("countryid") == F.lit(countryid))
        .select("keyname", "keypart", "keyorder")
        .orderBy("keyname", "keyorder")
        .collect()
    )
    by_keyname: dict[str, list[str]] = {}
    for r in rows:
        by_keyname.setdefault(r["keyname"], []).append(r["keypart"])

    keyname_to_dim = {spec.keyname: name for name, spec in DIMENSION_REGISTRY.items()}
    stage_set = set(stage_columns)
    resolved: list[tuple[str, list[str]]] = []
    for keyname, parts in by_keyname.items():
        dim_name = keyname_to_dim.get(keyname)
        if dim_name is None:
            logger.warning(
                "dimension_key_l has keyname '%s' but no DIMENSION_REGISTRY "
                "entry maps to it; skipping.",
                keyname,
            )
            continue
        missing = [p for p in parts if p not in stage_set]
        if missing:
            logger.info(
                "Skipping dimension '%s': stage frame missing keyparts %s",
                dim_name, missing,
            )
            continue
        resolved.append((dim_name, parts))
    return resolved


# --------------------------------------------------------------------------- #
# Per-batch transform + merge
# --------------------------------------------------------------------------- #

# Audit columns are written by the loader on every row; not driven by config.
AUDIT_INSERT_COLS = ("insjobid", "modjobid", "ins_ts", "mod_ts")
AUDIT_UPDATE_COLS = ("modjobid", "mod_ts")


def transform_and_merge_batch(
    batch_df: DataFrame,
    runtime: dict[str, Any],
    table_spec: SilverTableSpec,
) -> None:
    """Process one streaming microbatch end-to-end.

    1) Custom cleaner -> 2) column_definitions -> 3) dim_handler per dimension
    -> 4) MERGE into silver target. KEY columns and FACT candidates come from
    ``table_spec`` (read from silver_table_config_l), not from the pipeline
    config.
    """
    spark = batch_df.sparkSession
    catalog = runtime["catalog"]
    schema = runtime["schema"]
    silver_table = runtime["silver_table"]
    table_fqn = f"{catalog}.{schema}.{silver_table}"
    retailerid = runtime["scope"]["retailerid"]
    countryid = runtime["scope"]["countryid"]
    customerid = runtime["scope"]["customerid"]
    job_run_id = runtime["job_run_id"]
    source_job_run_id = runtime["source_job_run_id"]

    # Silver only ever processes one bronze run at a time. We auto-discovered
    # the latest _job_run_id present in the source bronze table at startup
    # (see run_pipeline) and filter to it here. _job_run_id is written by
    # ingest_bronze on every row.
    if "_job_run_id" not in batch_df.columns:
        raise ValueError(
            "Bronze source is missing _job_run_id column; "
            "transform_silver requires it to scope to a single run."
        )
    batch_df = batch_df.filter(F.col("_job_run_id") == F.lit(str(source_job_run_id)))

    if is_df_empty(batch_df):
        print(
            f"  No bronze rows for {silver_table} with "
            f"_job_run_id={source_job_run_id}; skipping"
        )
        return

    cleaner_name = runtime["custom_cleaner"]
    cleaner = get_cleaner(cleaner_name)
    stage = cleaner(batch_df)

    # Carry scope onto the stage frame so dim_handler and merge can use them.
    stage = _attach_scope_columns(stage, retailerid, countryid, customerid)

    # 2) Project to silver columns.
    stage = _apply_column_definitions(stage, runtime["column_definitions"])

    # 3) Auto-resolve dimensions: any keyname in dimension_key_l (scoped to
    # retailerid/countryid) whose keyparts are all present as columns on the
    # stage frame triggers a dim_handler call.
    dimensions_to_run = _dimensions_to_resolve(
        spark, catalog, schema, retailerid, countryid, stage.columns
    )
    for dimension_name, keyparts in dimensions_to_run:
        stage = dim_handler(
            stage_df=stage,
            dimension=dimension_name,
            catalog=catalog,
            schema=schema,
            retailerid=retailerid,
            countryid=countryid,
            job_run_id=job_run_id,
        )
        spec = DIMENSION_REGISTRY[dimension_name]
        dim_fqn = f"{catalog}.{schema}.{spec.table}"
        metrics = _last_operation_metrics(spark, dim_fqn)
        new_rows = metrics.get("numTargetRowsInserted", "0")
        keyparts_display = " + ".join(keyparts)
        print(
            f"  Found {new_rows} new {spec.display} based on {keyparts_display}"
        )
        print(f"  Adding {spec.id_col.upper()} to stage data for merge")

    # 4) Resolve key + fact columns from the table spec.
    key_columns = list(table_spec.key_columns)

    missing_keys = [k for k in key_columns if k not in stage.columns]
    if missing_keys:
        raise ValueError(
            f"Stage frame is missing KEY columns {missing_keys} required by "
            f"silver_table_config_l for {silver_table}. "
            "Check column_definitions and configured dimensions."
        )

    # Only update FACT columns the source actually supplies. A sales file
    # contributes posqty/possales/unitprice; an inventory file contributes
    # onhandqty/instock; neither should overwrite the other's metrics.
    fact_columns = [c for c in table_spec.fact_columns if c in stage.columns]
    if not fact_columns:
        logger.warning(
            "No FACT columns for %s present on stage frame; "
            "merge will only insert empty key rows.",
            silver_table,
        )

    select_cols = [F.col(c) for c in [*key_columns, *fact_columns]]
    final_df = stage.select(*select_cols).dropna(subset=key_columns)
    if fact_columns:
        agg_exprs = [F.sum(F.col(c)).alias(c) for c in fact_columns]
        final_df = final_df.groupBy(*key_columns).agg(*agg_exprs)

    final_df = (
        final_df
        .withColumn("insjobid", F.lit(job_run_id).cast("string"))
        .withColumn("modjobid", F.lit(job_run_id).cast("string"))
        .withColumn("ins_ts", F.current_timestamp())
        .withColumn("mod_ts", F.current_timestamp())
    )

    _ensure_silver_target_exists(spark, table_fqn, final_df)

    # 5) MERGE: update only the fact columns we actually have + audit.
    target = DeltaTable.forName(spark, table_fqn)
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in key_columns)
    update_set = {m: F.col(f"s.{m}") for m in fact_columns}
    for audit_col in AUDIT_UPDATE_COLS:
        update_set[audit_col] = F.col(f"s.{audit_col}")

    key_cols_display = ", ".join(k.upper() for k in key_columns)
    print(f"  Merging into {silver_table} on {key_cols_display}")

    (
        target.alias("t")
        .merge(final_df.alias("s"), on_clause)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    metrics = _last_operation_metrics(spark, table_fqn)
    inserted = metrics.get("numTargetRowsInserted", "0")
    updated = metrics.get("numTargetRowsUpdated", "0")
    print(f"  Inserted {inserted} new rows into {silver_table}")
    print(f"  Updated {updated} existing rows in {silver_table}")


# --------------------------------------------------------------------------- #
# Streaming entry point
# --------------------------------------------------------------------------- #

def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_silver_pipeline").getOrCreate()
    runtime = _resolve_silver_config(args)

    if not runtime["job_run_id"]:
        raise ValueError(
            "transform_silver requires --job-run-id (or job_run_id in the config); "
            "silver scopes the bronze read to a single run."
        )

    catalog = validate_identifier(runtime["catalog"], "catalog")
    schema = validate_identifier(runtime["schema"], "schema")
    bronze_table = validate_identifier(
        runtime["source_bronze_table"], "source_bronze_table"
    )
    silver_table = validate_identifier(runtime["silver_table"], "silver_table")
    runtime["catalog"] = catalog
    runtime["schema"] = schema

    task_name = args.task_name or "transform_silver"
    print(f"Task {task_name}:")
    print(f"  Running {task_name} with Job Run ID: {runtime['job_run_id']}")

    # Resolve KEY/FACT/AUDIT columns once at startup from silver_table_config_l;
    # the per-batch loader doesn't need to re-read this metadata.
    table_spec = get_load_table_spec(spark, catalog, schema, silver_table)

    # Auto-discover the latest bronze run to process. Silver is self-contained:
    # it does NOT receive the bronze run id from the orchestrator. It picks the
    # most recent _job_run_id present in its source bronze table and processes
    # only those rows. Silver's own --job-run-id remains the audit-stamp value.
    #
    # _job_run_id is stored as a string but Databricks job_run_ids are always
    # numeric, so cast to bigint before MAX to get correct ordering (string
    # MAX would put "9" > "10"). Rows with non-numeric ids (e.g. ad-hoc
    # 'manual-001' test runs) cast to NULL and are silently ignored.
    bronze_fqn = f"{catalog}.{schema}.{bronze_table}"
    max_row = (
        spark.table(bronze_fqn)
        .withColumn("_job_run_id_int", F.col("_job_run_id").cast("bigint"))
        .filter(F.col("_job_run_id_int").isNotNull())
        .agg(F.max(F.col("_job_run_id_int")).alias("m"))
        .collect()[0]
    )
    source_job_run_id_int = max_row["m"]
    if source_job_run_id_int is None:
        raise ValueError(
            f"Source bronze table {bronze_fqn} has no rows with a numeric "
            "_job_run_id; nothing for silver to process. Run bronze first."
        )
    # Convert back to string for the filter (matches the on-disk type).
    source_job_run_id = str(source_job_run_id_int)
    runtime["source_job_run_id"] = source_job_run_id

    # Pre-count rows in bronze for this run so we can announce the workload.
    rows_to_transform = (
        spark.table(bronze_fqn)
        .filter(F.col("_job_run_id") == F.lit(source_job_run_id))
        .count()
    )
    print(
        f"  Transforming {rows_to_transform} rows from {bronze_fqn} "
        f"based on Bronze Job Run ID {source_job_run_id}"
    )

    source_df = spark.readStream.table(bronze_fqn)

    query = (
        source_df.writeStream.option("checkpointLocation", runtime["checkpoint_path"])
        .trigger(availableNow=True)
        .foreachBatch(
            lambda batch_df, _batch_id: transform_and_merge_batch(
                batch_df, runtime, table_spec
            )
        )
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run_pipeline(parse_args())
