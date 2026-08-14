-- Configure api_products to the public DummyJSON endpoint used in the demo.
-- Keep enabled=false until you intentionally isolate/enable it.

UPDATE workspace.metadata.ingestion_sources
SET
  source_type = 'API',
  source_format = 'json',
  source_path = 'https://dummyjson.com/products',
  read_options = map(
    'records_path', 'products',
    'timeout_seconds', '30',
    'param.limit', '5',
    'param.select', 'id,title,price,category',
    'header.User-Agent', 'Mozilla/5.0',
    'header.Accept', 'application/json'
  ),
  load_type = 'MERGE',
  merge_keys = array('id'),
  watermark_column = CAST(NULL AS STRING),
  required_columns = array('id', 'title'),
  enabled = false,
  updated_at = current_timestamp()
WHERE source_id = 'api_products';

SELECT
  source_id,
  source_type,
  enabled,
  source_format,
  source_path,
  read_options,
  target_table,
  load_type,
  merge_keys,
  watermark_column,
  required_columns
FROM workspace.metadata.ingestion_sources
WHERE source_id = 'api_products';
