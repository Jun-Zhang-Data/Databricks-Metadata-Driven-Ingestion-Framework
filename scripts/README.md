# Local Lakebase demo scripts

These scripts are **local setup/demo utilities**. They are not part of the
Databricks wheel and are not executed by bundle jobs.

## Install optional local dependencies

From the repo root:

```powershell
python -m pip install -e ".[lakebase]"
```

## Configure your local environment

Copy the committed example:

```powershell
Copy-Item scripts\set_env.example.ps1 scripts\set_env.local.ps1
```

Edit `scripts\set_env.local.ps1` with your own Databricks/Lakebase values.

Then load it into the current PowerShell session:

```powershell
. .\scripts\set_env.local.ps1
```

The local file is ignored by Git.

If the Databricks CLI is not on `PATH`, set `DATABRICKS_CLI` to its full
executable path in the local environment file.

## Recommended demo order

1. Create/reset the demo source table:

```powershell
python scripts\create_lakebase_accounts.py
```

2. Create the read-only native PostgreSQL login:

```powershell
python scripts\create_p2_ingest_role.py
```

If the demo role already exists and you intentionally want to replace it:

```powershell
python scripts\create_p2_ingest_role.py --recreate
```

3. Store the role credentials in Databricks Secrets:

```powershell
databricks secrets create-scope metadata-ingestion --profile <profile>
databricks secrets put-secret metadata-ingestion db-user --string-value "p2_ingest" --profile <profile>
databricks secrets put-secret metadata-ingestion db-password --profile <profile>
```

If you use the CLI default profile, omit `--profile <profile>`.

4. Copy/edit `sql/06_configure_lakebase_jdbc_accounts.sql` and replace
`<LAKEBASE_HOST>` with your own endpoint hostname.

5. Isolate the JDBC source with `sql/07_enable_jdbc_only.sql` and run:

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

6. After the first successful JDBC load, create the incremental source batch:

```powershell
python scripts\update_lakebase_accounts.py
```

7. Run `metadata_ingest` again and inspect with
`sql/08_inspect_jdbc_accounts.sql`.

8. When finished with the JDBC-only lab, restore the FILE demo sources with
`sql/09_restore_file_demo_sources.sql`.

## Security

No database password, OAuth token, workspace hostname, personal email, or CLI
profile is committed in the default configuration.

The admin scripts obtain a temporary OAuth database credential through the
Databricks CLI. The `p2_ingest` password should be stored only in Databricks
Secrets (or another secret manager).
