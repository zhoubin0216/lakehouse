SELECT
    detected_at,
    release_id,
    dataset_name,
    old_schema_version,
    new_schema_version,
    change_type,
    added_columns_json,
    removed_columns_json,
    changed_types_json,
    compatible
FROM monitoring_schema_events
ORDER BY detected_at, dataset_name;
