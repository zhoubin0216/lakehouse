import src.pipeline as pipeline


def ingestion_summary(accepted_records: int) -> dict:
    return {
        "datasets": [],
        "consumed_files": 1 if accepted_records else 0,
        "accepted_records": accepted_records,
        "rejected_records": 0,
        "has_new_data": accepted_records > 0,
    }


def test_all_skips_downstream_steps_without_new_accepted_data(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        pipeline,
        "build_raw_tables",
        lambda spark, config: calls.append("raw") or ingestion_summary(0),
    )
    monkeypatch.setattr(
        pipeline,
        "build_normal_tables",
        lambda spark, config: calls.append("normal"),
    )
    monkeypatch.setattr(
        pipeline,
        "build_integrated_tables",
        lambda spark, config: calls.append("integrated"),
    )
    monkeypatch.setattr(
        pipeline,
        "build_aggregate_tables",
        lambda spark, config: calls.append("aggregate"),
    )

    result = pipeline.run_incremental_pipeline(object(), {})

    assert calls == ["raw"]
    assert result["has_new_data"] is False


def test_all_runs_three_downstream_steps_when_new_data_arrives(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        pipeline,
        "build_raw_tables",
        lambda spark, config: calls.append("raw") or ingestion_summary(12),
    )
    monkeypatch.setattr(
        pipeline,
        "build_normal_tables",
        lambda spark, config: calls.append("normal"),
    )
    monkeypatch.setattr(
        pipeline,
        "build_integrated_tables",
        lambda spark, config: calls.append("integrated"),
    )
    monkeypatch.setattr(
        pipeline,
        "build_aggregate_tables",
        lambda spark, config: calls.append("aggregate"),
    )

    result = pipeline.run_incremental_pipeline(object(), {})

    assert calls == ["raw", "normal", "integrated", "aggregate"]
    assert result["accepted_records"] == 12
