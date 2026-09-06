from __future__ import annotations

import hashlib
import json
from pathlib import Path


def discover_source_files(dataset_name: str, dataset_config: dict, config: dict) -> list[Path]:
    """Return source files for one dataset."""
    source = Path(config["paths"]["raw"]) / dataset_config["source"]

    if any(char in str(source) for char in "*?[]"):
        files = sorted(source.parent.glob(source.name))
    elif source.is_dir():
        extension = dataset_config["format"].lower()
        files = sorted(path for path in source.iterdir() if path.is_file() and path.suffix.lower() == f".{extension}")
    elif source.is_file():
        files = [source]
    else:
        raise FileNotFoundError(f"No source files found for {dataset_name}: {source}")

    if not files:
        raise FileNotFoundError(f"No source files matched for {dataset_name}: {source}")
    return files


def get_file_state(path: Path) -> dict:
    """Return path, size, modification time, and optional checksum metadata."""
    stat = path.stat()
    state = {
        "path": str(path),
        "source_file_uri": path.resolve().as_uri(),
        "size_bytes": stat.st_size,
        "modified_time_ns": stat.st_mtime_ns,
        "checksum_sha256": None,
    }
    return state


def add_checksum(state: dict, checksum_max_bytes: int) -> dict:
    """Add a checksum for small files where the cost is acceptable."""
    path = Path(state["path"])
    if state["size_bytes"] > checksum_max_bytes:
        return state

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    state["checksum_sha256"] = digest.hexdigest()
    return state


def load_source_file_registry(dataset_name: str, config: dict) -> dict:
    """Load previously consumed source file states from metadata storage."""
    path = registry_path(dataset_name, config)
    if not path.exists():
        return {"dataset": dataset_name, "files": []}

    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def find_files_to_consume(source_files: list[Path], registry: dict) -> list[Path]:
    """Select new or changed source files that should be consumed."""
    consumed = {item["path"]: item for item in registry.get("files", [])}
    files_to_consume = []

    for path in source_files:
        state = get_file_state(path)
        previous = consumed.get(str(path))
        if previous is None:
            files_to_consume.append(path)
            continue
        if previous.get("size_bytes") != state["size_bytes"]:
            files_to_consume.append(path)
            continue
        if previous.get("modified_time_ns") != state["modified_time_ns"]:
            files_to_consume.append(path)

    return files_to_consume


def save_source_file_registry(dataset_name: str, consumed_file_states: list[dict], config: dict) -> None:
    """Persist source file states only after the raw Delta write succeeds."""
    path = registry_path(dataset_name, config)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing = load_source_file_registry(dataset_name, config)
    files_by_path = {item["path"]: item for item in existing.get("files", [])}
    for state in consumed_file_states:
        files_by_path[state["path"]] = state

    registry = {"dataset": dataset_name, "files": sorted(files_by_path.values(), key=lambda item: item["path"])}
    with path.open("w", encoding="utf-8") as handle:
        json.dump(registry, handle, indent=2)


def registry_path(dataset_name: str, config: dict) -> Path:
    return Path(config["paths"]["source_file_registry"]) / f"{dataset_name}.json"
