-- Isolate jdbc_accounts for a controlled ingestion run.
UPDATE workspace.metadata.ingestion_sources
SET enabled = CASE WHEN source_id = 'jdbc_accounts' THEN true ELSE false END,
    updated_at = current_timestamp()
WHERE source_id IN ('customers', 'orders', 'events', 'jdbc_accounts', 'api_products');

SELECT source_id, source_type, enabled, ingestion_order
FROM workspace.metadata.ingestion_sources
ORDER BY ingestion_order;
