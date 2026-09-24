from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

from src.common import create_spark, load_config
from src.data_aggregation.aggregate_tables import build_aggregate_tables
from src.data_cleaning.normal_tables import build_normal_tables
from src.data_consumption.raw_tables import build_raw_tables
from src.data_integration.integrated_tables import build_integrated_tables
from src.data_quality import build_validation_report
from src.monitoring.logger import (
    safe_record_operation,
    utc_now,
)
from src.monitoring.queries import (
    run_monitoring_queries,
)

def run_incremental_pipeline(spark, config: dict) -> dict:
    """Consume sources and rebuild derived tables only when new valid rows arrive."""
    ingestion = build_raw_tables(spark, config)
    if not ingestion["has_new_data"]:
        print("No new accepted raw records; skipping normal, integrated, and aggregate steps")
        if "incremental" in config:
            run_updates(spark, config)
        return ingestion

    print(
        f"New data detected: {ingestion['accepted_records']} accepted row(s) "
        f"from {ingestion['consumed_files']} file(s)"
    )
    build_normal_tables(spark, config)
    build_integrated_tables(spark, config)
    build_aggregate_tables(spark, config)
    if "incremental" in config:
        run_updates(spark, config)
    return ingestion


def run_updates(spark, config, manifest=None):
    from src.incremental.pipeline import apply_release
    manifests = [Path(manifest)] if manifest else sorted(
        Path(config["incremental"]["releases_root"]).glob("*/manifest.json")
    )
    if manifest is None:
        pending = {
            Path(state["manifest"]).resolve()
            for path in Path(config["incremental"]["state_root"]).glob("*/state.json")
            if (state := json.loads(path.read_text()))["status"] != "complete"
        }
        manifests.sort(key=lambda path: path.resolve() not in pending)
    return [apply_release(spark, config, path) for path in manifests]


def run_pipeline_step(spark, config: dict, step: str):
    """Run one manual step or the conditional incremental pipeline."""
    if step == "all":
        return run_incremental_pipeline(spark, config)

    steps = {
        "raw": build_raw_tables,
        "normal": build_normal_tables,
        "integrated": build_integrated_tables,
        "aggregate": build_aggregate_tables,
        "monitoring": run_monitoring_queries,
        "validation": build_validation_report,
    }
    return steps[step](spark, config)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a lakehouse step or the conditional incremental pipeline."
    )
    parser.add_argument(
        "step",
        choices=[
            "raw",
            "normal",
            "integrated",
            "aggregate",
            "all",
            "updates",
            "monitoring",
            "validation",
        ],
    )
    parser.add_argument("--manifest", type=Path, help="Immutable release manifest (updates only)")
    args = parser.parse_args()
    if args.manifest and args.step != "updates":
        parser.error("--manifest requires the updates step")

    config = load_config()
    spark = create_spark()

    started_at = utc_now()
    started = time.perf_counter()

    result = None

    try:
        if args.step == "updates":
            result = run_updates(
                spark,
                config,
                args.manifest,
            )
        else:
            result = run_pipeline_step(
                spark,
                config,
                args.step,
            )

    except Exception as error:
        finished_at = utc_now()

        safe_record_operation(
            spark,
            config,
            operation_type="pipeline_command",
            pipeline_step=args.step,
            operation_name=f"src.pipeline {args.step}",
            status="FAILED",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(
                    time.perf_counter() - started
            ),
            error=error,
            details={
                "manifest":
                    str(args.manifest)
                    if args.manifest
                    else None
            },
        )

        raise

    else:
        finished_at = utc_now()

        processed_records = None
        inserted_records = None
        rejected_records = None

        details = {
            "manifest":
                str(args.manifest)
                if args.manifest
                else None
        }

        if (
                isinstance(result, dict)
                and "accepted_records" in result
        ):
            inserted_records = int(
                result.get(
                    "accepted_records",
                    0,
                )
            )

            rejected_records = int(
                result.get(
                    "rejected_records",
                    0,
                )
            )

            processed_records = (
                    inserted_records
                    + rejected_records
            )

            details["consumed_files"] = (
                result.get("consumed_files")
            )

        elif isinstance(result, list):
            details["release_attempts"] = len(
                result
            )

        safe_record_operation(
            spark,
            config,
            operation_type="pipeline_command",
            pipeline_step=args.step,
            operation_name=f"src.pipeline {args.step}",
            status="SUCCESS",
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=(
                    time.perf_counter() - started
            ),
            processed_records=processed_records,
            inserted_records=inserted_records,
            rejected_records=rejected_records,
            details=details,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
