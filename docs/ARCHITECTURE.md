# Architecture

## Goal

The framework keeps **ingestion policy in metadata** and keeps source-specific
code limited to reusable connector types.

Instead of building one pipeline per dataset:

```text
customers pipeline
orders pipeline
events pipeline
accounts pipeline
products pipeline
```

the project uses:

```text
source metadata
      +
generic connector routing
      +
shared quality/load/operations logic
```

For a connector type the framework already understands, onboarding another
compatible source is primarily a metadata operation.

---

## High-level flow

```text
                 workspace.metadata.ingestion_sources
                              │
                    enabled configs in order
                              │
                              ▼
                       generic ingest_all()
                              │
                     one config at a time
                              │
                              ▼
                         ingest_one()
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
            FILE             JDBC             API
       Spark file read   DB connector      HTTP GET
              └───────────────┼───────────────┘
                              ▼
                       Spark DataFrame
                              │
                   optional watermark filter
                              │
                     required-column QC
                     ┌────────┴────────┐
                     ▼                 ▼
                   valid            invalid
                     │                 │
                     │                 ▼
                     │       workspace.ops.quarantine
                     ▼
              FULL / APPEND / MERGE
                     │
                     ▼
                Bronze target
                     │
              compute watermark
                     │
            ┌────────┴────────┐
            ▼                 ▼
 ingestion_state       ingestion_audit
```

The framework deliberately separates two decisions:

```text
source_type = HOW is the source read?
              FILE | JDBC | API

load_type   = HOW are valid rows written?
              FULL | APPEND | MERGE
```

Those decisions are independent.

Examples:

```text
FILE + FULL
FILE + MERGE
FILE + APPEND + watermark
JDBC + MERGE + watermark
API + MERGE
```

---

## Main execution boundary

`ingest_all()` loads enabled metadata rows in configured order:

```sql
SELECT *
FROM workspace.metadata.ingestion_sources
WHERE enabled = true
ORDER BY ingestion_order, source_id;
```

It then calls `ingest_one()` for each configuration.

Current execution is sequential:

```text
source 1
  ↓
source 2
  ↓
source 3
  ↓
...
```

Parallel source execution is intentionally left as a future improvement.

---

## Connector routing

`read_source()` is the connector boundary:

```text
FILE -> read_file_source()
JDBC -> read_jdbc_source()
API  -> read_api_source()
```

Every connector returns a Spark DataFrame with the same common lineage
columns:

```text
_source_file
_ingested_at
_source_id
```

After that point, the quality and write layers do not need to know whether the
data originally came from a file, database, or HTTP API.

That is one of the main architectural ideas in the project:

```text
connector-specific reading
        ↓
common DataFrame contract
        ↓
shared ingestion behavior
```

---

## FILE connector

FILE metadata contains values such as:

```text
source_type   = FILE
source_format = csv | json
source_path   = /Volumes/<catalog>/<schema>/<volume>/<dataset>
read_options  = format-specific Spark options
```

The reader uses:

```text
spark.read
     .format(source_format)
     .options(...)
     .load(source_path)
```

For FILE sources, `_source_file` comes from Spark's file metadata and contains
the physical input file path.

Demonstrated FILE sources:

```text
customers -> CSV  -> FULL
orders    -> JSON -> MERGE
events    -> JSON -> APPEND + watermark
```

---

## JDBC / database connector

The project uses `source_type = JDBC` as the logical metadata category for
database ingestion.

For the demonstrated Lakebase PostgreSQL source:

```text
source_type   = JDBC
source_format = postgresql
source_path   = <Lakebase endpoint host>
```

Reader options contain values such as:

```text
port
database
dbtable or query
user
password
```

On Databricks serverless, the demonstrated path uses the bundled PostgreSQL
Spark connector:

```text
spark.read
     .format("postgresql")
     .options(...)
     .load()
```

The database reader also supports a generic `jdbc` path when a Unity Catalog
JDBC connection is supplied through the appropriate reader option.

For the Lakebase demo, lineage is intentionally non-sensitive:

```text
_source_file = postgresql:<source_id>
```

The full hostname, password, and other connection secrets are not written into
the lineage column.

Demonstrated database source:

```text
jdbc_accounts
source table      = public.accounts
load strategy     = MERGE
merge key         = account_id
watermark column  = updated_at
```

---

## API connector

The API reader performs HTTP GET and converts JSON records into a Spark
DataFrame.

Metadata controls:

```text
source_path
records_path
timeout_seconds
header.<name>
param.<name>
page_param
start_page
max_pages
```

Example conceptual configuration:

```text
source_path        = https://example/api/products
records_path       = products
header.Accept      = application/json
param.limit        = 5
load_type          = MERGE
merge_keys         = [id]
```

The reader:

```text
build URL/query parameters
        ↓
HTTP GET
        ↓
parse JSON
        ↓
extract records_path
        ↓
Spark DataFrame
```

The full request URL is not stored in lineage because URLs may contain
sensitive query parameters.

API lineage therefore uses:

```text
_source_file = api:<source_id>
```

The demonstrated API also showed a useful failure/recovery path:

```text
request without expected headers
        ↓
HTTP 403
        ↓
FAILED audit/state recorded
        ↓
add User-Agent + Accept in metadata
        ↓
rerun
        ↓
SUCCESS
```

---

## Metadata control table

### `workspace.metadata.ingestion_sources`

Defines what **should happen**.

Important configuration fields:

```text
source_id
source_type
enabled
source_format
source_path
read_options
target_catalog
target_schema
target_table
load_type
merge_keys
watermark_column
required_columns
ingestion_order
```

The framework does not hardcode the five demo datasets into the generic
ingestion path.

Their differences are expressed through metadata.

---

## State table

### `workspace.metadata.ingestion_state`

Stores operational checkpoint state per source.

Conceptually:

```text
source_id
last_watermark
last_run_id
status
updated_at
```

Its main purpose is incremental restartability.

For a watermark-enabled source:

```text
last successful watermark
        ↓
next run reads it
        ↓
source rows are filtered
```

A source without a watermark still has a state row; its
`last_watermark` remains null.

---

## Audit table

### `workspace.ops.ingestion_audit`

Stores one operational record per source run.

Important fields include:

```text
run_id
source_id
target_table
load_type
started_at
ended_at
rows_input
rows_valid
rows_quarantined
status
error_message
```

This table answers:

> What actually happened?

The framework retains failed audit rows rather than deleting them after a
successful retry. That preserves operational history.

---

## Quarantine table

### `workspace.ops.quarantine`

Rows failing required-column checks are separated from valid rows.

Conceptually:

```text
incoming DataFrame
       │
required-column check
       │
  ┌────┴────┐
  ▼         ▼
valid     invalid
  │         │
target   quarantine
```

This prevents one bad record from forcing the entire source batch to fail when
the configured quality rule is simply a required-column violation.

---

## Watermark lifecycle

Watermarks answer:

> Which source rows should be processed this run?

The lifecycle is:

```text
get_last_watermark()
        ↓
read source
        ↓
filter watermark_column > saved watermark
        ↓
quality check
        ↓
write valid rows
        ↓
MAX(valid watermark_column)
        ↓
update ingestion_state
```

Example:

```text
saved watermark = 21:00

source rows:
19:00 -> filtered out
21:00 -> filtered out
22:00 -> processed
23:00 -> processed

new watermark = MAX(22:00, 23:00) = 23:00
```

If no new rows are available:

```text
rows_input = 0
rows_valid = 0
status     = SUCCESS
watermark  = unchanged
```

That behavior was explicitly demonstrated for both `events` and
`jdbc_accounts`.

---

## Watermark vs MERGE

These solve different problems.

### Watermark

Controls **which source records are processed**.

```text
source rows
   ↓
timestamp/id checkpoint filter
   ↓
new candidate rows
```

### MERGE

Controls **how valid rows are applied to the target**.

```text
matching business key -> UPDATE
new business key      -> INSERT
```

Therefore these combinations are all valid:

```text
APPEND + watermark
MERGE + watermark
MERGE without watermark
```

Examples from this project:

```text
events        -> APPEND + event_ts watermark
jdbc_accounts -> MERGE + updated_at watermark
api_products  -> MERGE without watermark
```

The API source can reread the same five product IDs and still avoid duplicate
target rows because MERGE upserts by `id`.

---

## Quality boundary

Quality rules are metadata-driven through `required_columns`.

Example:

```text
required_columns = [order_id, customer_id, product_id]
```

The framework splits the DataFrame into valid and invalid records.

This project currently implements required non-null checks only. Richer rule
types are intentionally left as a future extension.

---

## Write strategies

### FULL

```text
valid DataFrame
     ↓
overwrite target
```

Used for small authoritative snapshots.

Example:

```text
customers
```

### APPEND

```text
valid DataFrame
     ↓
append target
```

Used when each processed valid row should be added.

Example:

```text
events
```

A watermark is important here because rereading old events would otherwise
append duplicates.

### MERGE

The framework validates the metadata merge keys, drops duplicate incoming keys,
and dynamically builds the match condition.

Conceptually:

```sql
MERGE INTO target
USING incoming
ON target.key = incoming.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Examples:

```text
orders        -> order_id
jdbc_accounts -> account_id
api_products  -> id
```

If the target does not yet exist, the incoming DataFrame creates it.

---

## Success path

For a normal source run:

```text
create run_id
   ↓
read source
   ↓
read previous watermark
   ↓
apply watermark
   ↓
count rows_input
   ↓
split valid/invalid
   ↓
write quarantine
   ↓
FULL / APPEND / MERGE valid rows
   ↓
compute new watermark
   ↓
update state = SUCCESS
   ↓
write SUCCESS audit row
```

---

## Failure path

If an exception occurs:

```text
connector / quality / write error
        ↓
write FAILED audit row
        ↓
preserve previous watermark
        ↓
update state = FAILED
        ↓
re-raise exception
        ↓
Databricks task fails
```

The previous successful watermark is deliberately preserved. A failed run must
not advance the checkpoint past data that may not have been committed
successfully.

---

## Security boundary

Credentials are not intended to be literal values committed in Git.

Reader options can reference:

```text
secret://scope/key
env://VARIABLE_NAME
```

Example:

```text
user     = secret://metadata-ingestion/db-user
password = secret://metadata-ingestion/db-password
```

The GitHub-safe repository also keeps workspace-specific Lakebase host,
Databricks CLI profile, and administrator identity out of committed defaults.

Non-file lineage uses logical source references rather than full sensitive
connection strings or URLs.

---

## Bundle/runtime architecture

The project is packaged as a Python wheel and deployed through a Databricks
Asset Bundle.

```text
local source code
      ↓
build wheel
      ↓
bundle deploy
      ↓
serverless job environment
      ↓
wheel task command
      ↓
job.py
      ↓
framework.py
```

Bundle job resource keys:

```text
metadata_bootstrap
metadata_ingest
metadata_demo_update
metadata_upgrade
```

`metadata_ingest` is a Databricks Asset Bundle job resource name, not a Python
function name.

The runtime chain is conceptually:

```text
bundle run metadata_ingest
        ↓
wheel task
        ↓
job.py ingest command
        ↓
ingest_all()
        ↓
ingest_one() per enabled source
```

---

## Bootstrap vs configured workspace

The repository's safe bootstrap defaults are:

```text
customers      -> enabled
orders         -> enabled
events         -> enabled
jdbc_accounts  -> disabled template
api_products   -> disabled template
```

Database and API templates are disabled because those connectors require real
environment-specific configuration.

`metadata_bootstrap` is the destructive reset path: it clears metadata/state/
audit/quarantine and drops the five Project 2 Bronze demo targets.

`metadata_upgrade` is different: it adds missing metadata support and inserts
missing JDBC/API templates without rewriting existing source configurations or
clearing state/audit/quarantine/targets.

The completed test workspace was then configured with all five sources enabled
and successfully executed in one ingestion run.

This distinction keeps the repository portable while still demonstrating the
extended architecture.

---

## Final demonstrated topology

```text
FILE
├── customers       -> FULL
├── orders          -> MERGE
└── events          -> APPEND + watermark

JDBC
└── jdbc_accounts   -> PostgreSQL + MERGE + watermark

API
└── api_products    -> HTTP GET + MERGE
```

All five ultimately share:

```text
Spark DataFrame
      ↓
watermark when configured
      ↓
required-column quality
      ↓
FULL / APPEND / MERGE
      ↓
Bronze
      ↓
state + audit + quarantine
```

That shared path is what makes the project metadata-driven rather than a
collection of unrelated source-specific ingestion scripts.

---

## Current architectural tradeoffs

The current design is intentionally small enough to understand end to end.

Known tradeoffs:

```text
sequential source execution
driver-side API HTTP
simple API pagination
no generalized retry/backoff
required-column quality only
simple > / MAX watermark policy
lightweight schema-evolution handling
audit-table monitoring without alert integration
no full automated connector test suite yet
```

These are explicit extension points rather than hidden production claims.
