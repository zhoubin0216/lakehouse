SELECT
    dataset_name,
    COALESCE(SUM(validation_failures), 0) AS total_validation_failures,
    COALESCE(SUM(rejected_records), 0) AS total_rejected_records,
    COUNT(*) AS executions
FROM monitoring_pipeline_runs
WHERE operation_type = 'dataset_update'
GROUP BY dataset_name
ORDER BY total_validation_failures DESC, dataset_name;
