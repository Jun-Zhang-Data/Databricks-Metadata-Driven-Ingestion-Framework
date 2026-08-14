-- Inspect the most recent multi-source integration run.
SELECT
  source_id,
  load_type,
  rows_input,
  rows_valid,
  rows_quarantined,
  status,
  ended_at
FROM workspace.ops.ingestion_audit
ORDER BY ended_at DESC
LIMIT 10;

SELECT
  source_id,
  last_watermark,
  last_run_id,
  status,
  updated_at
FROM workspace.metadata.ingestion_state
ORDER BY source_id;
