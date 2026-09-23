SELECT
    run_id,
    parent_run_id,
    release_id,
    started_at,
    dataset_name,
    processed_records,
    rejected_records,
    validation_failures,
    status
FROM monitoring_pipeline_runs
WHERE operation_type = 'dataset_update' AND status = 'SUCCESS'
ORDER BY started_at, dataset_name;
