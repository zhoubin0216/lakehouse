from src.common import load_config


def test_config_loads() -> None:
    config = load_config()
    assert "datasets" in config
    assert "yellow_taxi_trips" in config["datasets"]
