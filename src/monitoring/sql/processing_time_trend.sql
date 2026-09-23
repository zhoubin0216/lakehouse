SELECT
    started_at,
    release_id,
    dataset_name,
    duration_seconds,
    processed_records,
    status
FROM monitoring_pipeline_runs
WHERE operation_type = 'dataset_update' AND status = 'SUCCESS'
ORDER BY dataset_name, started_at;
