from src.data_analysis.data_products import PRODUCT_BUILDERS
from src.data_analysis.product_benchmark import report_markdown, summarize


def test_product_benchmark_summary_and_report():
    timings = []
    catalog = {}
    for index, name in enumerate(PRODUCT_BUILDERS, start=1):
        catalog[name] = {
            "row_count": index,
            "storage_bytes": index * 100,
            "refresh_duration_seconds": index / 10,
            "source_delta_version": 7,
        }
        for block in (1, 2, 3):
            for run in (1, 2):
                timings.extend([
                    {"product_name": name, "variant": "on_demand", "block": block,
                     "run": run, "seconds": 2.0},
                    {"product_name": name, "variant": "materialized", "block": block,
                     "run": run, "seconds": 0.2},
                ])
    summary = summarize(timings, catalog, 3)
    assert len(summary) == 4
    assert all(row["speedup"] == 10 for row in summary)
    assert all(row["materialized_block_wins"] == 3 for row in summary)

    metadata = {
        "suite_id": "fixture", "spark_version": "test", "spark_master": "local[1]",
        "blocks": 3, "measured_runs": 2,
    }
    evidence = [
        {"product_name": name, "variant": variant,
         "scans": [{"files": 1, "file_bytes": 100}]}
        for name in PRODUCT_BUILDERS for variant in ("on_demand", "materialized")
    ]
    report = report_markdown(summary, metadata, evidence)
    assert "Task 5 Product Materialization Benchmark" in report
    assert "1,000 bytes" in report
    assert "3/3" in report
