SELECT
  source_id,
  last_watermark,
  last_run_id,
  status,
  updated_at
FROM workspace.metadata.ingestion_state
ORDER BY source_id;
