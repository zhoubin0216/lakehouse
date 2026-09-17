import pytest

from src.common import load_config, resolve_schema_definition, validate_config
from src.view_table import parse_columns, resolve_table_path


def test_config_loads() -> None:
    config = load_config()
    assert "datasets" in config
    assert "yellow_taxi_trips" in config["datasets"]
    assert all(dataset["current_schema_version"] == 1 for dataset in config["datasets"].values())
    assert all("1" in dataset["schema_versions"] for dataset in config["datasets"].values())
    assert len(config["data_analysis"]["products"]["definitions"]) == 4


def test_config_rejects_missing_required_analytical_product() -> None:
    config = load_config()
    del config["data_analysis"]["products"]["definitions"]["daily_mobility_summary"]

    with pytest.raises(ValueError, match="Missing required analytical products"):
        validate_config(config)


def test_config_defines_standalone_report_output() -> None:
    config = load_config()
    assert config["data_analysis"]["report"]["output_file"].endswith(".html")


@pytest.mark.parametrize("schema_version", [None, 0, -1, True, "1"])
def test_config_rejects_invalid_current_schema_version(schema_version) -> None:
    config = {
        "data_quality": {"rejected_table_root": "rejected"},
        "datasets": {
            "example": {
                "current_schema_version": schema_version,
                "schema_versions": {"1": schema_definition()},
            }
        }
    }

    with pytest.raises(ValueError, match="current_schema_version as a positive integer"):
        validate_config(config)


def test_current_and_historical_schema_versions_can_be_resolved() -> None:
    dataset = {
        "current_schema_version": 2,
        "schema_versions": {
            "1": schema_definition(["old_name"]),
            "2": schema_definition(["new_name"]),
        },
    }
    validate_config(
        {
            "data_quality": {"rejected_table_root": "rejected"},
            "datasets": {"example": dataset},
        }
    )

    current_version, current = resolve_schema_definition("example", dataset)
    historical_version, historical = resolve_schema_definition("example", dataset, 1)

    assert current_version == 2
    assert current["expected_columns"] == ["new_name"]
    assert historical_version == 1
    assert historical["expected_columns"] == ["old_name"]


def test_config_rejects_pointer_to_missing_schema_definition() -> None:
    config = {
        "data_quality": {"rejected_table_root": "rejected"},
        "datasets": {
            "example": {
                "current_schema_version": 2,
                "schema_versions": {"1": schema_definition()},
            }
        }
    }

    with pytest.raises(ValueError, match="no schema definition for version 2"):
        validate_config(config)


def test_config_rejects_incomplete_column_types() -> None:
    definition = schema_definition(["id", "amount"])
    del definition["column_types"]["amount"]
    config = {
        "data_quality": {"rejected_table_root": "rejected"},
        "datasets": {
            "example": {
                "current_schema_version": 1,
                "schema_versions": {"1": definition},
            }
        },
    }

    with pytest.raises(ValueError, match="column_types keys must match"):
        validate_config(config)


def test_resolve_table_path_from_dataset_name() -> None:
    config = load_config()

    path = resolve_table_path(config, "yellow_taxi_trips", "raw")

    assert path == "data/lakehouse/raw/yellow_taxi_trips"


def test_parse_columns() -> None:
    assert parse_columns("vendor_id, pickup_timestamp , total_amount") == [
        "vendor_id",
        "pickup_timestamp",
        "total_amount",
    ]


def schema_definition(expected_columns: list[str] | None = None) -> dict:
    columns = expected_columns or []
    return {
        "format": "csv",
        "expected_columns": columns,
        "column_types": {column: "string" for column in columns},
        "columns": {},
    }
