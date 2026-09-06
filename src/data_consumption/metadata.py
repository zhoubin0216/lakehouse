from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def start_ingestion_run(dataset_name: str) -> dict:
    """Create an ingestion run metadata object."""
    return {
        "run_id": str(uuid.uuid4()),
        "dataset_name": dataset_name,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "started_monotonic": time.perf_counter(),
    }


def mark_ingestion_success(run: dict, processed_records: int, rejected_records: int = 0) -> dict:
    """Attach success metrics to an ingestion run."""
    finished = datetime.now(timezone.utc).isoformat()
    execution_time = time.perf_counter() - run["started_monotonic"]
    return {
        **without_internal_fields(run),
        "status": "success",
        "finished_at": finished,
        "processed_records": processed_records,
        "rejected_records": rejected_records,
        "execution_time_seconds": round(execution_time, 3),
    }


def mark_ingestion_failure(run: dict, error: Exception) -> dict:
    """Attach failure details to an ingestion run."""
    finished = datetime.now(timezone.utc).isoformat()
    execution_time = time.perf_counter() - run["started_monotonic"]
    return {
        **without_internal_fields(run),
        "status": "failed",
        "finished_at": finished,
        "processed_records": 0,
        "rejected_records": 0,
        "execution_time_seconds": round(execution_time, 3),
        "error_type": type(error).__name__,
        "error_message": str(error),
    }


def save_ingestion_metadata(run: dict, config: dict) -> None:
    """Persist ingestion run metadata for auditing and restartability."""
    output_dir = Path(config["paths"]["ingestion_runs"]) / run["dataset_name"]
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{run['run_id']}.json"
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(run, handle, indent=2)


def without_internal_fields(run: dict) -> dict:
    return {key: value for key, value in run.items() if key != "started_monotonic"}
