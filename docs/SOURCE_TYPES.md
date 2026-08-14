# Source Types

The framework currently supports three logical source types:

```text
FILE
JDBC
API
```

The source type controls only **how the source is read**.

It does not determine how the target is written.

```text
source_type = FILE | JDBC | API
load_type   = FULL | APPEND | MERGE
```

Examples from the project:

```text
FILE + FULL
FILE + MERGE
FILE + APPEND + watermark
JDBC + MERGE + watermark
API + MERGE
```

After a source is read, all three source types return a Spark DataFrame and use
the same downstream framework:

```text
optional watermark
→ required-column quality checks
→ quarantine invalid rows
→ FULL / APPEND / MERGE
→ state
→ audit
```

---

## Source routing

The framework router is conceptually:

```python
if source_type == "FILE":
    return read_file_source(...)

if source_type == "JDBC":
    return read_jdbc_source(...)

if source_type == "API":
    return read_api_source(...)
```

For backward compatibility, a metadata row with no `source_type` is treated as:

```text
FILE
```

Unsupported source types fail explicitly rather than silently choosing a reader.

---

# FILE

## When to use it

Use `FILE` when the source can be read directly by Spark from a supported path,
such as a Unity Catalog Volume.

Typical examples:

```text
CSV
JSON
Parquet
```

The project demo uses:

```text
customers -> CSV
orders    -> JSON
events    -> JSON
```

## Metadata contract

Typical metadata:

```text
source_type   = FILE
source_format = csv | json | parquet | ...
source_path   = /Volumes/... or another Spark-readable path
read_options  = Spark reader options
```

Conceptually, the framework executes:

```python
spark.read \
    .format(source_format) \
    .options(**read_options) \
    .load(source_path)
```

## FILE lineage

The FILE reader also reads Spark file metadata so it can populate:

```text
_source_file = physical input file path
_ingested_at = current timestamp
_source_id   = metadata source_id
```

This makes it possible to trace a Bronze row back to the actual file that
produced it.

## Demonstrated FILE combinations

### Customers

```text
source_type = FILE
format      = CSV
load_type   = FULL
```

### Orders

```text
source_type = FILE
format      = JSON
load_type   = MERGE
merge_keys  = [order_id]
```

### Events

```text
source_type      = FILE
format           = JSON
load_type        = APPEND
watermark_column = event_ts
```

---

# JDBC / database sources

## Logical meaning

`source_type = JDBC` means:

```text
this source is a relational/database source
```

The actual Spark connector is selected through:

```text
source_format
```

The project supports two database-reader paths:

```text
bundled database connector
generic JDBC
```

---

## PostgreSQL / Lakebase path

The demonstrated Lakebase source uses Databricks' bundled PostgreSQL Spark data
source.

Metadata:

```text
source_type   = JDBC
source_format = postgresql
source_path   = <PostgreSQL hostname>
```

`read_options` supplies the rest of the connection configuration:

```text
port
database
dbtable or query
user
password
```

Example shape:

```text
port      = 5432
database  = databricks_postgres
dbtable   = public.accounts
user      = secret://metadata-ingestion/db-user
password  = secret://metadata-ingestion/db-password
```

The framework executes the equivalent of:

```python
spark.read \
    .format("postgresql") \
    .options(
        host=source_path,
        ...
    ) \
    .load()
```

A database source must provide either:

```text
dbtable
```

or:

```text
query
```

Otherwise the framework fails with a configuration error.

## Demonstrated Lakebase source

```text
source_id         = jdbc_accounts
source_type       = JDBC
source_format     = postgresql
source table      = public.accounts
load_type         = MERGE
merge_keys        = [account_id]
watermark_column  = updated_at
required_columns  = [account_id]
```

This source proved:

```text
database read
→ watermark filter
→ MERGE update/insert
→ state + audit
```

---

## Generic JDBC path

The framework also keeps a generic:

```text
source_format = jdbc
```

reader path.

For the generic path, this implementation expects a Unity Catalog JDBC
connection name in:

```text
read_options["databricks.connection"]
```

`source_path` is not used by the generic `jdbc` branch; it remains required by
the shared metadata schema.

and also requires:

```text
dbtable
```

or:

```text
query
```

Conceptually:

```python
spark.read \
    .format("jdbc") \
    .options(**read_options) \
    .load()
```

If `source_format = jdbc` is used without `databricks.connection`, the framework
fails explicitly.

For PostgreSQL in this project, the demonstrated and preferred path is:

```text
source_format = postgresql
```

---

## JDBC lineage

The framework does not put the full connection string, host, credentials, or
query into `_source_file`.

For the PostgreSQL connector:

```text
_source_file = postgresql:<source_id>
```

For generic JDBC:

```text
_source_file = jdbc:<source_id>
```

Example:

```text
postgresql:jdbc_accounts
```

This gives a useful logical lineage reference without exposing connection
details.

---

# API

## When to use it

Use `API` for small/moderate HTTP JSON sources that can be fetched through an
HTTP GET request.

The current implementation intentionally stays simple and understandable.

## Metadata contract

```text
source_type = API
source_path = <HTTP URL>
```

Recognized `read_options` are:

```text
records_path
timeout_seconds
page_param
start_page
max_pages
header.<HTTP header name>
param.<query parameter name>
```

Any unknown API option causes an explicit configuration error.

---

## Query parameters

Use:

```text
param.<name>
```

Example:

```text
param.limit  = 5
param.select = id,title,price,category
```

These are added to the URL as query-string parameters.

If the base URL already contains query parameters, the configured parameters
are merged into them.

---

## HTTP headers

Use:

```text
header.<name>
```

Example:

```text
header.User-Agent = Mozilla/5.0
header.Accept     = application/json
```

The demonstrated API initially returned HTTP 403 until the required request
headers were added through metadata.

That behavior was fixed through configuration rather than a new source-specific
pipeline.

---

## `records_path`

The HTTP response is parsed as JSON.

If the JSON response itself is already an array of objects:

```json
[
  {"id": 1},
  {"id": 2}
]
```

then `records_path` can be omitted.

If the response is:

```json
{
  "products": [
    {"id": 1},
    {"id": 2}
  ]
}
```

use:

```text
records_path = products
```

Nested object paths use dot notation.

Example response:

```json
{
  "data": {
    "items": [
      {"id": 1},
      {"id": 2}
    ]
  }
}
```

Configuration:

```text
records_path = data.items
```

The resolved value must be:

```text
one JSON object
```

or:

```text
an array of JSON objects
```

Other shapes fail explicitly.

---

## Timeout

The default timeout is:

```text
30 seconds
```

Override with:

```text
timeout_seconds
```

Example:

```text
timeout_seconds = 60
```

---

## Simple page-number pagination

Pagination is optional.

Configure:

```text
page_param
start_page
max_pages
```

Example:

```text
page_param = page
start_page = 1
max_pages  = 10
```

The reader requests:

```text
page=1
page=2
...
```

up to `max_pages`.

If a paginated response resolves to zero records, the loop stops early.

If `page_param` is not set:

```text
only one HTTP request is made
```

regardless of `max_pages`.

---

## Empty API response

If the API returns no records, the framework creates an empty Spark DataFrame.

This allows downstream APPEND/MERGE handling to behave as a no-op rather than
treating an empty successful response as a connector error.

---

## API lineage

The framework deliberately does not write the full URL into `_source_file`.

URLs can contain sensitive query parameters.

Instead:

```text
_source_file = api:<source_id>
```

Example:

```text
api:api_products
```

---

## Demonstrated API source

The project demo uses:

```text
source_id        = api_products
source_type      = API
source_format    = json
load_type        = MERGE
merge_keys       = [id]
watermark_column = null
required_columns = [id, title]
```

The successful run demonstrated:

```text
HTTP GET
→ JSON response
→ products array
→ 5 Spark rows
→ quality check
→ MERGE by id
→ SUCCESS audit
```

Because the demo API has no reliable incremental timestamp:

```text
last_watermark = null
```

The source can be reread and MERGE keeps the target keyed by product ID.

---

# Credential references

Any string value in `read_options` is resolved at runtime.

Supported forms:

```text
env://NAME
secret://scope/key
```

## Environment variable

Example:

```text
password = env://DATABASE_PASSWORD
```

The framework resolves:

```text
DATABASE_PASSWORD
```

from the runtime environment.

If the variable is missing, the run fails.

## Databricks secret

Example:

```text
password = secret://metadata-ingestion/db-password
```

The framework resolves the value with Databricks Secrets at runtime.

The required syntax is exactly:

```text
secret://<scope>/<key>
```

Secret resolution requires the Databricks runtime.

For local/outside-Databricks execution, use:

```text
env://NAME
```

instead.

## Header secrets

Credential references also work inside API headers.

For example:

```text
header.Authorization = secret://metadata-ingestion/api-authorization
```

The stored secret can contain the complete header value:

```text
Bearer <token>
```

---

# Shared downstream behavior

After any reader returns a Spark DataFrame, the same generic framework controls:

```text
watermark
quality
FULL / APPEND / MERGE
state
audit
quarantine
```

This is the main reason `source_type` is a connector abstraction rather than a
complete pipeline definition.

---

# Choosing a source type

Use this quick guide:

```text
Can Spark read it directly from a path?
→ FILE

Is it a relational/database table or query?
→ JDBC

Is it HTTP GET returning JSON?
→ API
```

Then choose the write policy separately:

```text
authoritative snapshot
→ FULL

immutable/new facts
→ APPEND

existing keys can change
→ MERGE
```

Add a watermark when the source offers a trustworthy incremental checkpoint and
you want to avoid rereading already-processed source rows.

---

# Current limitations

## FILE

The framework delegates supported formats/options to Spark. It does not
implement a separate schema-contract system per file source.

## JDBC

The demonstrated serverless database integration is PostgreSQL/Lakebase.

Other database engines may require a supported Databricks connector or a Unity
Catalog JDBC connection.

## API

Current API connector limitations:

```text
GET only
JSON only
driver-side HTTP
simple page-number pagination only
no cursor/next-link pagination
no generalized retry/backoff
no OAuth refresh flow
no per-source explicit Spark schema registry
```

For high-volume or reliability-critical APIs, a durable raw landing/staging
pattern would be a stronger production design.

---

# Extension boundary

If a new source fits an existing reader:

```text
add metadata
```

If a brand-new connector type is needed:

```text
1. add one reusable reader
2. extend read_source()
3. define its metadata contract
4. return the common Spark DataFrame shape
5. reuse the existing downstream pipeline
```

The intended architecture is:

```text
new connector code only at the read boundary
        ↓
shared framework remains unchanged
```
