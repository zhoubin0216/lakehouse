SELECT
    dataset_name,
    COUNT(*) AS executions,
    AVG(duration_seconds) AS avg_processing_seconds,
    MAX(duration_seconds) AS max_processing_seconds,
    MIN(duration_seconds) AS min_processing_seconds
FROM monitoring_pipeline_runs
WHERE operation_type = 'dataset_update' AND status = 'SUCCESS'
GROUP BY dataset_name
ORDER BY avg_processing_seconds DESC, dataset_name;
