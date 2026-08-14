-- Enable all five demonstrated sources for the final integration run.
UPDATE workspace.metadata.ingestion_sources
SET
  enabled = true,
  updated_at = current_timestamp()
WHERE source_id IN (
  'customers',
  'orders',
  'events',
  'jdbc_accounts',
  'api_products'
);

SELECT
  source_id,
  source_type,
  enabled,
  load_type,
  watermark_column,
  ingestion_order
FROM workspace.metadata.ingestion_sources
ORDER BY ingestion_order;
