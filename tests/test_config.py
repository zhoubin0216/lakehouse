from pathlib import Path

from src.utils.config import load_yaml


def test_platform_config_loads() -> None:
    config = load_yaml(Path("configs/platform.yaml"))
    assert config["project_name"] == "urban_data_lakehouse"
