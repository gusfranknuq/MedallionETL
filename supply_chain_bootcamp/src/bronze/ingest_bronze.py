import argparse
import inspect
import json
import logging
from pathlib import Path
import re
from typing import Any

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
        description=(
            "Ingest raw supply chain data into a Bronze Delta table with Auto Loader. "
            "Use --config-path for the new parameterized contract, or pass legacy flags."
        )
    )
    parser.add_argument(
        "--config-path",
        default=None,
        help="Path to Bronze ingest JSON config that defines source, destination table, and options.",
    )
    parser.add_argument(
        "--task-name",
        default=None,
        help="Task name used to look up an ingest config from the task-to-config map.",
    )
    parser.add_argument(
        "--task-config-map-path",
        default=None,
        help="Optional override path for the task-to-config JSON map.",
    )
    parser.add_argument("--catalog", default="supply_chain", help="Unity Catalog name")
    parser.add_argument("--schema", default="supply_chain", help="Unity Catalog schema name")
    parser.add_argument(
        "--source-path",
        required=False,
        help="Cloud storage URI for incoming files (legacy mode when --config-path is not used).",
    )
    parser.add_argument("--source-format", default="json", help="Input file format (legacy mode)")
    parser.add_argument(
        "--source-file-pattern",
        required=False,
        help="File pattern for Auto Loader discovery (legacy mode)",
    )
    parser.add_argument(
        "--bronze-table",
        required=False,
        help="Bronze Delta table name (legacy mode)",
    )
    parser.add_argument(
        "--checkpoint-path",
        required=False,
        help="Cloud storage checkpoint path for streaming query",
    )
    parser.add_argument(
        "--schema-location",
        required=False,
        help="Cloud storage path where Auto Loader persists inferred schema",
    )
    parser.add_argument(
        "--job-run-id",
        default=None,
        help="Optional Databricks job run id for lineage",
    )
    return parser.parse_args()


def _value_to_option(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _module_dir() -> Path:
    file_from_globals = globals().get("__file__")
    if file_from_globals:
        return Path(file_from_globals).resolve().parent

    inspected_path = inspect.getsourcefile(_module_dir) or inspect.getfile(_module_dir)
    if inspected_path:
        return Path(inspected_path).resolve().parent

    return Path.cwd().resolve()


def _default_task_config_map_path() -> Path:
    module_dir = _module_dir()
    candidates = [
        module_dir.parents[2] / "resources" / "bronze_ingest_task_config_map.json"
        if len(module_dir.parents) >= 3
        else None,
        module_dir.parents[1] / "resources" / "bronze_ingest_task_config_map.json"
        if len(module_dir.parents) >= 2
        else None,
        module_dir / "bronze_ingest_task_config_map.json",
        Path.cwd().resolve() / "resources" / "bronze_ingest_task_config_map.json",
    ]
    for candidate in candidates:
        if candidate and candidate.exists():
            return candidate
    return (
        module_dir.parents[2] / "resources" / "bronze_ingest_task_config_map.json"
        if len(module_dir.parents) >= 3
        else module_dir / "bronze_ingest_task_config_map.json"
    )


def _parse_bronze_config(config_path: str) -> dict[str, Any]:
    config = _load_json_with_resolved_path(config_path)
    if not isinstance(config, dict):
        raise ValueError("Bronze config must be a JSON object")

    required_top_level = [
        "source_file_path",
        "file_type",
        "source_file_pattern",
        "bronze_destination_table",
        "schema_location",
        "checkpoint_path",
    ]
    for key in required_top_level:
        if key not in config or config[key] in (None, ""):
            raise ValueError(f"Config is missing required field: {key}")

    if "reader_options" in config and not isinstance(config["reader_options"], dict):
        raise ValueError("reader_options must be a JSON object")

    if "autoloader_options" in config and not isinstance(
        config["autoloader_options"], dict
    ):
        raise ValueError("autoloader_options must be a JSON object")

    if "write_options" in config and not isinstance(config["write_options"], dict):
        raise ValueError("write_options must be a JSON object")

    return config


def _load_json_with_resolved_path(config_path: str) -> Any:
    path = Path(config_path)
    if not path.is_absolute():
        script_relative = (_module_dir() / path).resolve()
        if script_relative.exists():
            path = script_relative
        else:
            path = path.resolve()

    with path.open(encoding="utf-8") as config_file:
        return json.load(config_file)


def _parse_task_config_map(map_path: str | None) -> dict[str, str]:
    default_map_path = _default_task_config_map_path()
    selected_map_path = map_path if map_path else str(default_map_path)
    parsed_map = _load_json_with_resolved_path(selected_map_path)
    if not isinstance(parsed_map, dict):
        raise ValueError("Task config map must be a JSON object")

    for task_name, config_path in parsed_map.items():
        if not isinstance(task_name, str) or not isinstance(config_path, str):
            raise ValueError("Task config map must contain string task names and string config paths")

    return parsed_map


def _resolve_runtime_config(args: argparse.Namespace) -> dict[str, Any]:
    selected_config_path = args.config_path
    if not selected_config_path and args.task_name:
        task_config_map = _parse_task_config_map(args.task_config_map_path)
        selected_config_path = task_config_map.get(args.task_name)
        if not selected_config_path:
            known_tasks = ", ".join(sorted(task_config_map.keys()))
            raise ValueError(
                f"Task name '{args.task_name}' is not configured in task config map. "
                f"Known tasks: {known_tasks}"
            )

    if selected_config_path:
        config = _parse_bronze_config(selected_config_path)
        config_job_run_id = config.get("job_run_id")
        if config_job_run_id is None:
            config_job_run_id = args.job_run_id
        return {
            "catalog": config.get("catalog", args.catalog),
            "schema": config.get("schema", args.schema),
            "file_type": config["file_type"],
            "source_file_path": config["source_file_path"],
            "source_file_pattern": config["source_file_pattern"],
            "bronze_destination_table": config["bronze_destination_table"],
            "schema_location": config["schema_location"],
            "checkpoint_path": config["checkpoint_path"],
            "job_run_id": config_job_run_id,
            "reader_options": config.get("reader_options", {}),
            "autoloader_options": config.get(
                "autoloader_options",
                {
                    "cloudFiles.inferColumnTypes": True,
                    "cloudFiles.schemaEvolutionMode": "addNewColumns",
                },
            ),
            "write_options": config.get(
                "write_options",
                {
                    "output_mode": "append",
                    "trigger_available_now": True,
                },
            ),
        }

    required_legacy_fields = [
        ("source_path", args.source_path),
        ("source_file_pattern", args.source_file_pattern),
        ("bronze_table", args.bronze_table),
        ("checkpoint_path", args.checkpoint_path),
        ("schema_location", args.schema_location),
    ]
    missing_legacy_fields = [name for name, value in required_legacy_fields if not value]
    if missing_legacy_fields:
        missing = ", ".join(missing_legacy_fields)
        raise ValueError(
            "Missing required legacy flags: "
            f"{missing}. Either provide them or use --config-path with the Bronze config contract."
        )

    return {
        "catalog": args.catalog,
        "schema": args.schema,
        "file_type": args.source_format,
        "source_file_path": args.source_path,
        "source_file_pattern": args.source_file_pattern,
        "bronze_destination_table": args.bronze_table,
        "schema_location": args.schema_location,
        "checkpoint_path": args.checkpoint_path,
        "job_run_id": args.job_run_id,
        "reader_options": {},
        "autoloader_options": {
            "cloudFiles.inferColumnTypes": True,
            "cloudFiles.schemaEvolutionMode": "addNewColumns",
        },
        "write_options": {
            "output_mode": "append",
            "trigger_available_now": True,
        },
    }


def run_pipeline(args: argparse.Namespace) -> None:
    spark = SparkSession.builder.appName("supply_chain_bronze_pipeline").getOrCreate()
    runtime_config = _resolve_runtime_config(args)

    catalog = validate_identifier(runtime_config["catalog"], "catalog")
    schema = validate_identifier(runtime_config["schema"], "schema")
    bronze_table = validate_identifier(
        runtime_config["bronze_destination_table"], "bronze_destination_table"
    )

    table_name = f"{catalog}.{schema}.{bronze_table}"

    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", runtime_config["file_type"])
        .option("cloudFiles.schemaLocation", runtime_config["schema_location"])
        .option("pathGlobFilter", runtime_config["source_file_pattern"])
    )

    for option_key, option_value in runtime_config["autoloader_options"].items():
        reader = reader.option(option_key, _value_to_option(option_value))

    for option_key, option_value in runtime_config["reader_options"].items():
        reader = reader.option(option_key, _value_to_option(option_value))

    source_df = (
        reader.load(runtime_config["source_file_path"])
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_job_run_id", F.lit(runtime_config["job_run_id"]).cast("string"))
    )

    write_options = runtime_config["write_options"]
    output_mode = write_options.get("output_mode", "append")
    trigger_available_now = write_options.get("trigger_available_now", True)

    writer = source_df.writeStream.format("delta").option(
        "checkpointLocation", runtime_config["checkpoint_path"]
    )
    if trigger_available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.outputMode(output_mode).toTable(table_name)
    query.awaitTermination()


if __name__ == "__main__":
    run_pipeline(parse_args())
