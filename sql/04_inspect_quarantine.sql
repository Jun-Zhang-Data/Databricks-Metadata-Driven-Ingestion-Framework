SELECT
  source_id,
  quarantined_at,
  reason,
  source_file,
  record_json
FROM workspace.ops.quarantine
ORDER BY quarantined_at DESC;
