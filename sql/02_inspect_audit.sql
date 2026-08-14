SELECT
  run_id,
  source_id,
  target_table,
  load_type,
  started_at,
  ended_at,
  rows_input,
  rows_valid,
  rows_quarantined,
  status,
  error_message
FROM workspace.ops.ingestion_audit
ORDER BY ended_at DESC;
