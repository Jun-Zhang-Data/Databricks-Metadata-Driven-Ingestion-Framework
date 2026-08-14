-- Verify API -> Databricks ingestion.
SELECT *
FROM workspace.bronze.api_products_md
ORDER BY id;

SELECT *
FROM workspace.metadata.ingestion_state
WHERE source_id = 'api_products';

SELECT
  source_id,
  load_type,
  rows_input,
  rows_valid,
  rows_quarantined,
  status,
  error_message,
  started_at,
  ended_at
FROM workspace.ops.ingestion_audit
WHERE source_id = 'api_products'
ORDER BY ended_at DESC;
