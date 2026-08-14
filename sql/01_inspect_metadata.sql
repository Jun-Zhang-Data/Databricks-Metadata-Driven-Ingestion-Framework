SELECT
  source_id,
  source_type,
  enabled,
  source_format,
  source_path,
  target_catalog,
  target_schema,
  target_table,
  load_type,
  merge_keys,
  read_options,
  watermark_column,
  required_columns,
  ingestion_order,
  description,
  updated_at
FROM workspace.metadata.ingestion_sources
ORDER BY ingestion_order, source_id;
