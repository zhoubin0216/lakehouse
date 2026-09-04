from dataclasses import dataclass
from time import perf_counter


@dataclass
class IngestionMetrics:
    dataset_name: str
    processed_records: int
    rejected_records: int
    execution_time_seconds: float
    schema_version: str = "v1"


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.execution_time_seconds = perf_counter() - self.start
