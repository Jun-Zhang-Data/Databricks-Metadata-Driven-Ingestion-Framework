-- Verify Lakebase -> Databricks JDBC ingestion.
SELECT *
FROM workspace.bronze.jdbc_accounts_md
ORDER BY account_id;

SELECT *
FROM workspace.metadata.ingestion_state
WHERE source_id = 'jdbc_accounts';

SELECT source_id, load_type, rows_input, rows_valid, rows_quarantined,
       status, started_at, ended_at
FROM workspace.ops.ingestion_audit
WHERE source_id = 'jdbc_accounts'
ORDER BY ended_at DESC;
