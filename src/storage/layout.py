from pathlib import Path


def table_path(warehouse_root: str | Path, layer: str, table_name: str) -> Path:
    return Path(warehouse_root) / layer / table_name
