from src.common import load_config
from src.view_table import parse_columns, resolve_table_path


def test_config_loads() -> None:
    config = load_config()
    assert "datasets" in config
    assert "yellow_taxi_trips" in config["datasets"]


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
