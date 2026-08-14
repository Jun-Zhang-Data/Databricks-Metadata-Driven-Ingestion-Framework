-- Metadata templates for adding sources without writing a new ingestion function.
-- Start with enabled = false, inspect the row, then enable it deliberately.

-- -----------------------------------------------------------------------------
-- FILE example
-- -----------------------------------------------------------------------------
INSERT INTO workspace.metadata.ingestion_sources
SELECT
  'new_file_source'                         AS source_id,
  'FILE'                                    AS source_type,
  false                                     AS enabled,
  'json'                                    AS source_format,
  '/Volumes/workspace/bronze/md_ingestion_raw/new_file_source' AS source_path,
  'workspace'                               AS target_catalog,
  'bronze'                                  AS target_schema,
  'new_file_source_md'                      AS target_table,
  'MERGE'                                   AS load_type,
  array('id')                               AS merge_keys,
  map('inferSchema', 'true')                AS read_options,
  CAST(NULL AS STRING)                      AS watermark_column,
  array('id')                               AS required_columns,
  60                                        AS ingestion_order,
  'Example FILE metadata row.'              AS description,
  current_timestamp()                       AS updated_at;

-- -----------------------------------------------------------------------------
-- JDBC/database example using the bundled PostgreSQL connector
-- Credentials may use secret://<scope>/<key> or env://<NAME>.
-- For the generic source_format='jdbc' path, see docs/SOURCE_TYPES.md; it
-- requires read_options['databricks.connection'] in this framework.
-- -----------------------------------------------------------------------------
INSERT INTO workspace.metadata.ingestion_sources
SELECT
  'new_jdbc_source'                         AS source_id,
  'JDBC'                                    AS source_type,
  false                                     AS enabled,
  'postgresql'                              AS source_format,
  'db.example.com'                          AS source_path,
  'workspace'                               AS target_catalog,
  'bronze'                                  AS target_schema,
  'new_jdbc_source_md'                      AS target_table,
  'MERGE'                                   AS load_type,
  array('id')                               AS merge_keys,
  map(
    'port', '5432',
    'database', 'app',
    'dbtable', 'public.example_table',
    'user', 'secret://metadata-ingestion/db-user',
    'password', 'secret://metadata-ingestion/db-password'
  )                                         AS read_options,
  'updated_at'                              AS watermark_column,
  array('id')                               AS required_columns,
  70                                        AS ingestion_order,
  'Example PostgreSQL database metadata row.' AS description,
  current_timestamp()                       AS updated_at;

-- -----------------------------------------------------------------------------
-- HTTP API example (GET + JSON)
-- records_path is dot-separated, e.g. data.items. Pagination is optional.
-- Store the full Authorization header value in the secret if needed, e.g.
-- "Bearer <token>".
-- -----------------------------------------------------------------------------
INSERT INTO workspace.metadata.ingestion_sources
SELECT
  'new_api_source'                          AS source_id,
  'API'                                     AS source_type,
  false                                     AS enabled,
  'json'                                    AS source_format,
  'https://api.example.com/items'           AS source_path,
  'workspace'                               AS target_catalog,
  'bronze'                                  AS target_schema,
  'new_api_source_md'                       AS target_table,
  'APPEND'                                  AS load_type,
  array()                                   AS merge_keys,
  map(
    'records_path', 'data',
    'timeout_seconds', '30',
    'header.Authorization', 'secret://metadata-ingestion/api-authorization',
    'param.limit', '100',
    'page_param', 'page',
    'start_page', '1',
    'max_pages', '10'
  )                                         AS read_options,
  'updated_at'                              AS watermark_column,
  array('id')                               AS required_columns,
  80                                        AS ingestion_order,
  'Example API metadata row.'               AS description,
  current_timestamp()                       AS updated_at;
