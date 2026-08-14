# Copy this file to scripts/set_env.local.ps1, fill in your own values,
# then load it from the repository root:
#
#   . .\scripts\set_env.local.ps1
#
# The local file is ignored by Git.

# If "databricks" is already on PATH, leave this as "databricks".
$env:DATABRICKS_CLI = "databricks"

# Optional. Leave empty to use the CLI's default authentication/profile.
$env:DATABRICKS_PROFILE = "<your-databricks-cli-profile>"

# Lakebase project resources.
$env:P2_LAKEBASE_PROJECT = "project2-jdbc"
$env:P2_LAKEBASE_BRANCH = "production"
$env:P2_LAKEBASE_ENDPOINT = "primary"

# Obtain these from your own Lakebase project/endpoint.
$env:P2_LAKEBASE_HOST = "<your-lakebase-host>"
$env:P2_LAKEBASE_PORT = "5432"
$env:P2_LAKEBASE_DATABASE = "databricks_postgres"
$env:P2_LAKEBASE_ADMIN_USER = "<your-postgres-admin-role>"

# Demo ingestion role created by create_p2_ingest_role.py.
$env:P2_LAKEBASE_INGEST_ROLE = "p2_ingest"
