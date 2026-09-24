import json

from src.data_analysis.data_products import PRODUCT_BUILDERS
from src.evaluation.production_readiness import (
    latest_release_metrics,
    overhead_metrics,
    path_size,
    report_markdown,
)


def test_overhead_metrics_uses_medians() -> None:
    result = overhead_metrics([1.0, 2.0, 3.0], [2.0, 4.0, 6.0])

    assert result["baseline_median_seconds"] == 2.0
    assert result["enabled_median_seconds"] == 4.0
    assert result["overhead_seconds"] == 2.0
    assert result["overhead_percent"] == 100.0


def test_path_size_and_latest_release_metrics(tmp_path) -> None:
    storage = tmp_path / "storage"
    storage.mkdir()
    (storage / "one").write_bytes(b"123")
    (storage / "two").write_bytes(b"4567")
    assert path_size(storage) == {"bytes": 7, "files": 2}

    state_root = tmp_path / "state"
    release = state_root / "release-1"
    release.mkdir(parents=True)
    (release / "state.json").write_text(
        json.dumps(
            {
                "release_id": "release-1",
                "status": "complete",
                "last_attempt_seconds": 12.5,
                "analytical_refresh_seconds": 3.5,
            }
        )
    )
    result = latest_release_metrics({"incremental": {"state_root": str(state_root)}})
    assert result["release_id"] == "release-1"
    assert result["incremental_update_seconds"] == 12.5
    assert result["analytical_refresh_seconds"] == 3.5


def test_report_contains_required_task5_measurements() -> None:
    product_times = {name: 0.1 for name in PRODUCT_BUILDERS}
    product_rows = {name: 2 for name in PRODUCT_BUILDERS}
    storage_record = {"bytes": 10, "files": 1, "paths": []}
    results = {
        "environment": {"spark_version": "test", "spark_master": "local[1]"},
        "settings": {"rows": 1000, "repeats": 3},
        "incremental_update": {
            "median_seconds": 1.0,
            "incoming_records": 110,
            "inserted_records": 100,
            "duplicate_records": 10,
        },
        "analytical_refresh": {
            "median_seconds": 2.0,
            "affected_scope_records": 500,
            "product_median_seconds": product_times,
            "product_rows": product_rows,
        },
        "validation_overhead": {
            "overhead_seconds": 0.2,
            "overhead_percent": 20.0,
            "records": 1000,
        },
        "monitoring_overhead": {
            "overhead_seconds": 0.3,
            "overhead_percent": 30.0,
        },
        "platform_storage": {
            "core_tables": storage_record,
            "analytical_products": storage_record,
            "validation": storage_record,
            "monitoring": storage_record,
            "metadata_and_update_state": storage_record,
            "supporting_overhead_bytes": 40,
            "overhead_percent_of_core": 400.0,
        },
        "latest_release": None,
    }

    report = report_markdown(results)

    assert "Incremental update median" in report
    assert "Analytical refresh median" in report
    assert "Validation overhead" in report
    assert "Monitoring overhead" in report
    assert "Production-readiness assessment" in report
