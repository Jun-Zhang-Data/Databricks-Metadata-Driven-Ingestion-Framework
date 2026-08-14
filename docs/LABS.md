# Labs

This document is a hands-on walkthrough for the metadata-driven ingestion
framework.

The labs are intentionally ordered so that each stage proves one architectural
idea before moving to the next.

```text
Lab 1 -> deploy and inspect metadata
Lab 2 -> FILE baseline
Lab 3 -> FILE incremental behavior
Lab 4 -> Lakebase PostgreSQL / JDBC
Lab 5 -> REST API
Lab 6 -> final all-source run
```

The repository uses safe defaults:

```text
customers      -> enabled
orders         -> enabled
events         -> enabled
jdbc_accounts  -> disabled template
api_products   -> disabled template
```

Database and API sources are disabled until real environment-specific settings
are configured.

---

# Prerequisites

You need:

```text
Databricks workspace
Databricks CLI
Python
Databricks Asset Bundles support
Unity Catalog access
```

For the JDBC lab you also need a Lakebase PostgreSQL project/endpoint.

For the API lab you need outbound HTTP access from the Databricks runtime.

---

# Lab 1 — Validate and deploy

## Why

Before testing ingestion, prove that:

```text
bundle configuration is valid
wheel builds
job resources deploy
```

## Validate

From the repository root:

```powershell
databricks bundle validate -t dev --profile <profile>
```

If you use the default CLI profile, omit:

```text
--profile <profile>
```

If the CLI is not on `PATH`, use the full executable path.

## Deploy

```powershell
databricks bundle deploy -t dev --profile <profile>
```

Expected outcome:

```text
bundle validates
wheel builds
bundle uploads
job resources deploy
```

The bundle defines:

```text
metadata_bootstrap
metadata_ingest
metadata_demo_update
metadata_upgrade
```

---

# Lab 2 — Bootstrap and inspect the FILE baseline

## Why

This creates the original FILE learning environment:

```text
customers -> FULL
orders    -> MERGE
events    -> APPEND + watermark
```

## Important warning

`metadata_bootstrap` is destructive for Project 2 demo objects.

It intentionally resets:

```text
ingestion_sources
ingestion_state
ingestion_audit
quarantine
customers_md
orders_md
events_md
jdbc_accounts_md
api_products_md
```

Use it only when you want a fresh demo environment.

## Run bootstrap

```powershell
databricks bundle run metadata_bootstrap -t dev --profile <profile>
```

## Inspect metadata

Use:

```text
sql/01_inspect_metadata.sql
```

Expected FILE source configuration:

```text
customers
source_type      = FILE
source_format    = csv
load_type        = FULL
enabled          = true

orders
source_type      = FILE
source_format    = json
load_type        = MERGE
merge_keys       = [order_id]
enabled          = true

events
source_type      = FILE
source_format    = json
load_type        = APPEND
watermark_column = event_ts
enabled          = true
```

The extended metadata rows should exist as disabled templates:

```text
jdbc_accounts -> disabled
api_products  -> disabled
```

---

# Lab 3 — Run the initial FILE ingestion

## Why

This proves the shared framework can apply different write strategies from
metadata without one hardcoded pipeline per source.

## Run

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

## Inspect audit

Use:

```text
sql/02_inspect_audit.sql
```

Expected first-run behavior:

```text
customers -> FULL  -> valid snapshot written
orders    -> MERGE -> target created/merged
events    -> APPEND -> initial events appended
```

## Inspect state

Use:

```text
sql/03_inspect_state.sql
```

Expected important result:

```text
events last_watermark = latest initial event_ts
```

`customers` and `orders` do not use a watermark in the original FILE demo, so
their saved watermark remains null.

---

# Lab 4 — FILE incremental demo

## Why

This lab proves three separate behaviors:

```text
FULL snapshot replacement
MERGE update + insert
APPEND + watermark
```

It also demonstrates quarantine.

## Create the incremental demo batch

```powershell
databricks bundle run metadata_demo_update -t dev --profile <profile>
```

The demo update produces source changes equivalent to:

```text
customers
+ one new customer in the snapshot

orders
5001 -> existing order updated
5005 -> new order
5999 -> deliberately invalid required-column row

events
old event repeated
+ two newer events
```

## Expected `orders` behavior

```text
5001 -> UPDATE
5005 -> INSERT
5999 -> quarantine
```

The MERGE key is:

```text
order_id
```

## Expected `events` behavior

Suppose the previous saved watermark is:

```text
10:00
```

and the new landing data contains:

```text
08:00
11:00
12:00
```

The framework processes only:

```text
11:00
12:00
```

and persists:

```text
new watermark = 12:00
```

## Inspect quarantine

Use:

```text
sql/04_inspect_quarantine.sql
```

You should see the deliberately invalid order.

This proves:

```text
bad record != entire source failure
```

The invalid row is isolated while valid rows continue through the target write.

---

# Lab 5 — Upgrade an existing Project 2 environment

## Why

If you already have the FILE project and want to add JDBC/API support, use the
non-destructive upgrade rather than rerunning bootstrap.

## Run

```powershell
databricks bundle run metadata_upgrade -t dev --profile <profile>
```

Expected result:

```text
existing source configurations remain unchanged
existing state/audit/quarantine and targets remain
source_type support is added if missing
jdbc_accounts template is inserted only if missing
api_products template is inserted only if missing
```

This is the preferred path for extending an already-tested workspace.

---

# Lab 6 — Configure Lakebase PostgreSQL

## Why

This lab proves:

```text
database source
→ PostgreSQL connector
→ watermark
→ quality
→ MERGE
→ Bronze
→ state + audit
```

The demonstrated source is:

```text
public.accounts
```

and the target is:

```text
workspace.bronze.jdbc_accounts_md
```

## 6.1 Install local helper dependency

```powershell
python -m pip install -e ".[lakebase]"
```

## 6.2 Configure local environment

Copy the committed example:

```powershell
Copy-Item scripts\set_env.example.ps1 scripts\set_env.local.ps1
```

Edit the local file with your own values, then load it:

```powershell
. .\scripts\set_env.local.ps1
```

The local file is ignored by Git.

Required environment-specific values include:

```text
Lakebase host
Lakebase admin role/user
Databricks CLI/profile if needed
```

Do not place passwords in the file.

## 6.3 Create the demo source table

Run:

```powershell
python scripts\create_lakebase_accounts.py
```

The demonstrated initial source data contains three accounts.

Conceptually:

```text
account 1 -> ACTIVE
account 2 -> ACTIVE
account 3 -> INACTIVE
```

with increasing `updated_at` timestamps.

## 6.4 Create the read-only ingestion role

Run:

```powershell
python scripts\create_p2_ingest_role.py
```

The role is intended to have only the database/table permissions required for
ingestion.

If you intentionally need to recreate the demo role:

```powershell
python scripts\create_p2_ingest_role.py --recreate
```

Do not paste the generated/chosen password into chat, source files, or Git.

## 6.5 Store credentials in Databricks Secrets

Create the scope if needed:

```powershell
databricks secrets create-scope metadata-ingestion --profile <profile>
```

Store the ingestion username:

```powershell
databricks secrets put-secret metadata-ingestion db-user --string-value "p2_ingest" --profile <profile>
```

Store the password interactively:

```powershell
databricks secrets put-secret metadata-ingestion db-password --profile <profile>
```

The ingestion metadata references:

```text
secret://metadata-ingestion/db-user
secret://metadata-ingestion/db-password
```

## 6.6 Configure the JDBC metadata row

Open:

```text
sql/06_configure_lakebase_jdbc_accounts.sql
```

Replace:

```text
<LAKEBASE_HOST>
```

with your own Lakebase endpoint host.

Run the SQL.

Expected metadata shape:

```text
source_id          = jdbc_accounts
source_type        = JDBC
source_format      = postgresql
load_type          = MERGE
merge_keys         = [account_id]
watermark_column   = updated_at
required_columns   = [account_id]
```

## 6.7 Isolate JDBC for the controlled test

Run:

```text
sql/07_enable_jdbc_only.sql
```

Then verify that:

```text
jdbc_accounts -> enabled
other demo sources -> disabled
```

## 6.8 First JDBC ingestion

Run:

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

Inspect:

```text
sql/08_inspect_jdbc_accounts.sql
```

Expected:

```text
3 account rows in Bronze
SUCCESS audit
state watermark = maximum initial updated_at
```

In the demonstrated run, the first saved watermark became the timestamp on the
third account.

---

# Lab 7 — JDBC incremental MERGE

## Why

This proves that watermark and MERGE are separate but complementary:

```text
watermark -> which source rows are new enough
MERGE     -> update existing keys / insert new keys
```

## Update the Lakebase source

Run:

```powershell
python scripts\update_lakebase_accounts.py
```

The demonstrated update does this:

```text
account 2 -> existing row changed with newer updated_at
account 4 -> new row with newer updated_at
```

Older accounts keep their previous timestamps.

## Run ingestion again

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

Expected result:

```text
rows_input = 2
rows_valid = 2
rows_quarantined = 0
status = SUCCESS
```

Bronze behavior:

```text
account 2 -> UPDATE
account 4 -> INSERT
```

State behavior:

```text
old watermark -> previous max updated_at
new watermark -> MAX(new valid rows)
```

In the demonstrated run:

```text
old watermark = 21:00
new rows      = 22:00, 23:00
new watermark = 23:00
```

## Run once more without changing the database

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

Expected:

```text
rows_input = 0
rows_valid = 0
status = SUCCESS
watermark unchanged
```

This proves that “nothing new” is a normal successful incremental result.

---

# Lab 8 — Configure the REST API source

## Why

This proves the source router can normalize HTTP/JSON into the same downstream
Spark DataFrame path.

The demonstrated metadata source is:

```text
api_products
```

with:

```text
source_type = API
load_type   = MERGE
merge_keys  = [id]
```

No watermark is configured for this demo.

## Configure metadata

Run:

```text
sql/10_configure_api_products.sql
```

This applies the exact public DummyJSON configuration used in the demonstrated
run, while leaving the source disabled.

The configuration contains:

```text
records_path
timeout_seconds
param.*
header.*
```

The demonstrated API returned a JSON object with a `products` array.

The request also required:

```text
User-Agent
Accept: application/json
```

Without those headers, the observed result was HTTP 403.

## Isolate API for the controlled test

Run:

```text
sql/11_enable_api_only.sql
```

Expected:

```text
api_products -> enabled
customers -> disabled
orders -> disabled
events -> disabled
jdbc_accounts -> disabled
```

The purpose is not architectural necessity; it simply makes the first API
debugging run easier to inspect.

## Run

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

If the endpoint rejects the request, inspect the audit error, update metadata
headers/options, and rerun.

The demonstrated recovery was:

```text
HTTP 403
   ↓
add User-Agent + Accept headers
   ↓
rerun
   ↓
SUCCESS
```

Expected successful behavior:

```text
5 API records
5 valid rows
0 quarantined
MERGE by id
```

## Inspect target/state/audit

Run:

```text
sql/12_inspect_api_products.sql
```

Expected target shape includes product columns plus:

```text
_source_file = api:api_products
_ingested_at
_source_id   = api_products
```

Expected state:

```text
status = SUCCESS
last_watermark = null
```

The null watermark is intentional.

The source is incremental/idempotent at the target through:

```text
MERGE by id
```

rather than source-side watermark filtering.

---

# Lab 9 — Clean the deliberate FILE quarantine row

## Why

The original incremental orders demo intentionally leaves one invalid record in
the landing data.

Because `orders` does not use a watermark, repeated all-source runs would read
that invalid row again and quarantine it again.

For the final steady-state demo, replace the landing snapshot with a clean
five-order set.

Run:

```text
sql/13_clean_orders_landing.sql
```

Expected order IDs:

```text
5001
5002
5003
5004
5005
```

No `5999`.

If your catalog/schema/volume names differ, adjust the path before running.

---

# Lab 10 — Final all-source run

## Why

This is the final integration proof:

```text
FILE + JDBC + API
through one generic framework run
```

Enable all five with:

```text
sql/14_enable_all_sources.sql
```

Then run:

```powershell
databricks bundle run metadata_ingest -t dev --profile <profile>
```

Inspect the final audit/state with:

```text
sql/15_inspect_final_run.sql
```

A demonstrated final run produced the following behavior:

```text
customers       FULL    5 input  5 valid  0 quarantine  SUCCESS
orders          MERGE   5 input  5 valid  0 quarantine  SUCCESS
events          APPEND  0 input  0 valid  0 quarantine  SUCCESS
jdbc_accounts   MERGE   0 input  0 valid  0 quarantine  SUCCESS
api_products    MERGE   5 input  5 valid  0 quarantine  SUCCESS
```

Why the counts differ:

```text
customers
FULL snapshot -> rereads all 5

orders
no watermark -> rereads all 5
MERGE keeps target keyed by order_id

events
watermark already at newest event -> 0 new rows

jdbc_accounts
watermark already at newest updated_at -> 0 new rows

api_products
no watermark -> rereads 5
MERGE keeps target keyed by id
```

This is the intended final demonstration of the framework.

---

# What each lab proves

```text
Lab 1
bundle/package/deployment path

Lab 2-4
metadata-driven FILE ingestion
FULL vs MERGE vs APPEND
watermark
quarantine

Lab 5
non-destructive framework extension

Lab 6-7
database connector
runtime secrets
MERGE + watermark
zero-row incremental success

Lab 8
HTTP/JSON connector
metadata-driven headers/params
MERGE without watermark
failure/retry history

Lab 9
controlled cleanup of deliberate bad demo data

Lab 10
all connector types in one generic run
```

---

# Useful inspection queries

## Metadata

```sql
SELECT *
FROM workspace.metadata.ingestion_sources
ORDER BY ingestion_order, source_id;
```

## State

```sql
SELECT *
FROM workspace.metadata.ingestion_state
ORDER BY source_id;
```

## Audit

```sql
SELECT *
FROM workspace.ops.ingestion_audit
ORDER BY ended_at DESC;
```

## Quarantine

```sql
SELECT *
FROM workspace.ops.quarantine
ORDER BY quarantined_at DESC;
```

## FILE targets

```sql
SELECT * FROM workspace.bronze.customers_md;
SELECT * FROM workspace.bronze.orders_md;
SELECT * FROM workspace.bronze.events_md;
```

## JDBC target

```sql
SELECT *
FROM workspace.bronze.jdbc_accounts_md
ORDER BY account_id;
```

## API target

```sql
SELECT *
FROM workspace.bronze.api_products_md
ORDER BY id;
```

---

# Troubleshooting

## Bundle validation/deploy problems

Run:

```powershell
databricks bundle validate -t dev --profile <profile>
```

before deploy/run.

For serverless jobs in this project, the wheel dependency belongs in the job
environment specification rather than task-level `libraries`.

## JDBC cannot authenticate

Check:

```text
Lakebase host
database
ingestion role
Databricks secret scope/key names
network/runtime access
```

Do not hardcode the database password into metadata.

## API returns HTTP 403

Check metadata-configured:

```text
User-Agent
Accept
query parameters
endpoint URL
```

The demonstrated API needed both `User-Agent` and
`Accept: application/json`.

## Incremental source returns zero rows

This can be correct.

Compare:

```text
saved watermark
vs
MAX(source watermark column)
```

If no source value is greater than the saved watermark:

```text
0 input rows + SUCCESS
```

is expected.

## MERGE source rereads old data

That is also possible.

MERGE and watermark are independent:

```text
MERGE without watermark
-> source may be reread
-> target remains keyed/upserted

MERGE with watermark
-> source is filtered first
-> target is then upserted
```

---

# Cleanup / reset

To return to the original FILE-only demo enablement, use:

```text
sql/09_restore_file_demo_sources.sql
```

To fully recreate the Project 2 learning environment, rerun:

```powershell
databricks bundle run metadata_bootstrap -t dev --profile <profile>
```

Remember that bootstrap intentionally resets the Project 2 demo state and
targets.
