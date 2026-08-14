-- Configure jdbc_accounts for a Lakebase PostgreSQL demo.
--
-- BEFORE RUNNING:
-- Replace <LAKEBASE_HOST> with your own Lakebase endpoint hostname.
--
-- Keep enabled=false until the framework is deployed and this metadata row
-- has been verified.

UPDATE workspace.metadata.ingestion_sources
SET
  source_type = 'JDBC',
  source_format = 'postgresql',
  source_path = '<LAKEBASE_HOST>',
  read_options = map(
    'port', '5432',
    'database', 'databricks_postgres',
    'dbtable', 'public.accounts',
    'user', 'secret://metadata-ingestion/db-user',
    'password', 'secret://metadata-ingestion/db-password'
  ),
  load_type = 'MERGE',
  merge_keys = array('account_id'),
  watermark_column = 'updated_at',
  required_columns = array('account_id'),
  enabled = false,
  updated_at = current_timestamp()
WHERE source_id = 'jdbc_accounts';

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
WHERE source_id = 'jdbc_accounts';
