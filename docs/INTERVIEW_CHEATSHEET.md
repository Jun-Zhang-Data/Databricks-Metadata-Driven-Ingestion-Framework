# Interview Cheat Sheet

Use this document to explain the project clearly without walking through every
line of code.

---

## 30-second version

> I built a metadata-driven ingestion framework on Databricks. A Delta control
> table defines the source type, target, load strategy, merge keys, watermark,
> required columns, and execution order. FILE, PostgreSQL, and REST API sources
> are all normalized into Spark DataFrames and then use the same downstream
> quality, FULL/APPEND/MERGE, audit, state, and quarantine logic. I demonstrated
> FILE ingestion, incremental APPEND with a watermark, incremental PostgreSQL
> MERGE with a watermark, and API MERGE by business key.

---

## 2-minute version

The main problem I wanted to solve was avoiding one hardcoded pipeline per
source.

Instead of:

```text
ingest_customers()
ingest_orders()
ingest_events()
ingest_accounts()
ingest_products()
```

I created a metadata table:

```text
workspace.metadata.ingestion_sources
```

Each row describes:

```text
how to read the source
where to write it
FULL / APPEND / MERGE
merge keys
watermark column
required columns
whether it is enabled
execution order
```

The generic ingestion engine reads enabled rows and routes them by:

```text
source_type
```

to:

```text
FILE
JDBC
API
```

All readers return a Spark DataFrame with common lineage columns.

After that, every source uses the same pipeline:

```text
optional watermark
→ quality checks
→ quarantine invalid rows
→ FULL / APPEND / MERGE
→ update state
→ write audit
```

I tested five sources:

```text
customers       FILE / FULL
orders          FILE / MERGE
events          FILE / APPEND + watermark
jdbc_accounts   PostgreSQL / MERGE + watermark
api_products    REST API / MERGE
```

The final run processed all five sources successfully through the same
framework.

---

## The most important design idea

Remember this sentence:

> `source_type` controls how data is read; `load_type` controls how valid data
> is written.

```text
source_type
FILE | JDBC | API

load_type
FULL | APPEND | MERGE
```

They are independent.

Examples:

```text
FILE + FULL
FILE + MERGE
FILE + APPEND + watermark
JDBC + MERGE + watermark
API + MERGE
```

---

## Why is it metadata-driven?

A good answer:

> The generic ingestion code does not contain one custom execution path for
> each dataset. The dataset-specific behavior is stored in metadata: connector
> type, source location, target, load strategy, merge keys, watermark, required
> columns, reader options, enablement, and ordering. If the framework already
> supports the connector and write behavior, a new source can usually be added
> by inserting metadata rather than writing a new pipeline.

Do not claim:

```text
"zero code is ever needed for a new source"
```

A new connector type may still require reusable connector code.

---

## Control tables

### `ingestion_sources`

Answers:

> What should happen?

Contains the configuration.

### `ingestion_state`

Answers:

> Where did incremental processing stop?

Contains the saved watermark and latest run state.

### `ingestion_audit`

Answers:

> What actually happened?

Contains counts, status, timing, and errors.

### `quarantine`

Answers:

> Which records failed configured quality rules?

Contains rejected rows.

Easy memory aid:

```text
sources = plan
state   = checkpoint
audit   = history
quarantine = rejected data
```

---

## The five demonstrated sources

### 1. Customers

```text
source_type = FILE
format      = CSV
load_type   = FULL
```

Why FULL?

The file represents the current authoritative customer snapshot.

Behavior:

```text
read snapshot
→ validate
→ overwrite target
```

---

### 2. Orders

```text
source_type = FILE
format      = JSON
load_type   = MERGE
merge key   = order_id
```

Demonstrated:

```text
5001 -> existing row updated
5005 -> new row inserted
5999 -> invalid row quarantined
```

Why MERGE?

An existing order can change state, while new orders also arrive.

---

### 3. Events

```text
source_type      = FILE
load_type        = APPEND
watermark_column = event_ts
```

Demonstrated:

```text
saved watermark = 10:00

11:00 -> processed
12:00 -> processed

new watermark = 12:00
```

Why APPEND?

Events are treated as immutable facts.

Why watermark?

Without it, rereading the landing data would append old events again.

---

### 4. JDBC accounts

```text
source_type      = JDBC
source_format    = postgresql
load_type        = MERGE
merge key        = account_id
watermark_column = updated_at
```

Demonstrated:

```text
old watermark = 21:00

account 2 at 22:00 -> UPDATE
account 4 at 23:00 -> INSERT

new watermark = 23:00
```

This source proves that:

```text
watermark + MERGE
```

can work together.

---

### 5. API products

```text
source_type = API
load_type   = MERGE
merge key   = id
watermark   = none
```

Demonstrated:

```text
HTTP GET
→ JSON
→ extract products
→ Spark DataFrame
→ 5 valid rows
→ MERGE by id
```

Why no watermark?

The demo API did not provide a reliable incremental timestamp.

The API can therefore reread the same records, while MERGE keeps the target
keyed by `id`.

---

## Watermark vs MERGE

This is a likely interview question.

### Watermark

Answers:

> Which source rows should I process?

Example:

```text
updated_at > last_watermark
```

### MERGE

Answers:

> How should I write those rows?

Example:

```text
same account_id -> UPDATE
new account_id  -> INSERT
```

So:

```text
watermark = source filtering
MERGE     = target write behavior
```

They solve different problems.

---

## Why save MAX watermark?

Example:

```text
processed rows:
22:00
23:00
```

Save:

```text
MAX = 23:00
```

Why?

Because the next run should process only:

```text
> 23:00
```

If you saved `22:00`, the `23:00` row would be processed again.

---

## What happens when there are zero new rows?

The framework treats it as success:

```text
rows_input = 0
rows_valid = 0
status     = SUCCESS
```

The watermark stays unchanged.

This was demonstrated for:

```text
events
jdbc_accounts
```

Why is that important?

Because an incremental pipeline with no new data is a normal operational state,
not an error.

---

## Quality and quarantine

Current rule type:

```text
required_columns
```

Example:

```text
[order_id, customer_id, product_id]
```

If a required column is null:

```text
row -> quarantine
```

Valid rows continue.

This prevents one bad row from necessarily failing the entire batch.

Current limitation:

The framework does not yet implement a generalized rule engine for:

```text
ranges
regex
referential integrity
business rules
```

---

## Failure handling

If a source fails:

```text
write FAILED audit
update state status = FAILED
preserve previous successful watermark
re-raise exception
```

Why preserve the watermark?

Because advancing it after a failed write could skip data on the retry.

The API demo proved the failure history path:

```text
HTTP 403 -> FAILED
fix metadata headers
rerun -> SUCCESS
```

The failed audit row remains as operational history.

---

## API 403 story

Useful interview example:

> The API initially returned HTTP 403. I did not hardcode a workaround in the
> reader. I added `User-Agent` and `Accept` headers through metadata
> `read_options`, reran the same generic pipeline, and the source succeeded.

Why is this a good example?

Because it shows:

```text
HTTP behavior was configuration
not source-specific pipeline code
```

---

## Secrets

Database credentials are not committed.

Metadata can reference:

```text
secret://scope/key
env://VARIABLE_NAME
```

Example:

```text
user     = secret://metadata-ingestion/db-user
password = secret://metadata-ingestion/db-password
```

The repository also removes personal:

```text
Databricks profile
Lakebase hostname
admin email
```

from committed defaults.

---

## Lineage columns

All readers add:

```text
_source_file
_ingested_at
_source_id
```

For FILE:

```text
_source_file = physical source file
```

For JDBC/API:

```text
_source_file = logical safe source label
```

Examples:

```text
postgresql:jdbc_accounts
api:api_products
```

Why not write the full connection string or URL?

Because it can leak sensitive connection details or query parameters.

---

## Runtime flow

A useful explanation:

```text
databricks bundle run metadata_ingest
        ↓
Databricks Asset Bundle job
        ↓
Python wheel task
        ↓
job.py ingest command
        ↓
ingest_all()
        ↓
ingest_one() for each enabled source
        ↓
framework.py
```

Important:

```text
metadata_ingest
```

is the Databricks Asset Bundle job resource key, not the Python function name.

---

## Why Databricks Asset Bundles?

Good answer:

> I wanted the jobs, Python package, and environment configuration to be
> deployable from source control instead of manually creating notebook jobs.
> The bundle validates, builds the wheel, deploys the job resources, and gives
> me repeatable commands for bootstrap, ingest, demo update, and upgrade.

---

## Why a Python wheel instead of only notebooks?

Good answer:

> The ingestion engine is application-style reusable Python code, so packaging
> it as a wheel gives clearer module boundaries, deployable versioned code, and
> keeps job orchestration separate from implementation logic.

---

## Why Delta metadata tables?

Good answer:

> The framework already runs on Databricks, so Delta gives transactional,
> queryable configuration and operational state in the same platform. I can
> inspect metadata, state, audit, and quarantine directly with SQL.

---

## Why not put everything in one metadata table?

Because configuration and runtime state have different lifecycles.

```text
ingestion_sources
changes when ingestion policy changes

ingestion_state
changes on successful/failed runs

ingestion_audit
adds history every run
```

Separating them makes the model clearer and safer.

---

## Why sequential execution?

Current implementation:

```text
one source at a time
```

Reasonable answer:

> I kept the first version sequential because it makes state, failure handling,
> and debugging easy to understand. For larger scale, independent metadata rows
> could be parallelized with task-level fan-out or another orchestration layer.

Do not claim sequential execution is inherently better at scale.

---

## Current limitations

Be comfortable naming them.

```text
sequential sources
driver-side API HTTP
simple API pagination
no generalized retry/backoff
required-column quality only
simple > / MAX watermark policy
light schema-evolution handling
no full automated connector test suite
no integrated alerting
```

Naming limitations makes the design more credible.

---

## Watermark limitation: late-arriving data

Current logic:

```text
source_timestamp > saved_watermark
```

Potential problem:

A record can arrive late with:

```text
timestamp <= saved_watermark
```

and be missed.

Production options include:

```text
lookback window
composite checkpoint
source sequence/CDC
deduplication after reread
```

This is a good improvement to mention if asked what you would do next.

---

## Equal timestamp limitation

Suppose two records have timestamp:

```text
23:00
```

but one appears only after the watermark has already advanced to `23:00`.

With:

```text
> 23:00
```

that late equal-timestamp row would not be processed.

Possible production solutions:

```text
timestamp + unique key checkpoint
lookback window
CDC sequence
```

---

## Why API MERGE is not the same as API watermarking

The API demo:

```text
reads 5 rows again
MERGE by id
```

That is target idempotency.

A watermark would instead reduce source processing:

```text
request/filter only rows newer than checkpoint
```

So:

```text
MERGE prevents duplicate business keys in target
watermark avoids reprocessing old source rows
```

---

## Final all-source run

The final demonstrated behavior was:

```text
customers       FULL    5  5  0  SUCCESS
orders          MERGE   5  5  0  SUCCESS
events          APPEND  0  0  0  SUCCESS
jdbc_accounts   MERGE   0  0  0  SUCCESS
api_products    MERGE   5  5  0  SUCCESS
```

How to explain it:

```text
customers
FULL rereads the current snapshot

orders
no watermark, so all current source rows are reread and MERGEd

events
no event newer than saved watermark

jdbc_accounts
no database row newer than saved watermark

api_products
no watermark, so 5 products are reread and MERGEd by id
```

This run proves all three source types can execute through one metadata-driven
engine.

---

## If asked: "What was the hardest part?"

Good answer:

> The interesting part was not the FILE reader itself. It was designing the
> boundary between source-specific reading and common downstream behavior, and
> then keeping watermark state, MERGE semantics, failures, audit history, and
> secrets consistent across FILE, PostgreSQL, and API sources.

---

## If asked: "What would you build next?"

Choose two or three, not ten.

Strong options:

```text
1. automated tests for connector and failure behavior
2. configurable retry/backoff for API/database reads
3. richer data-quality rules
4. safer watermark strategy for late-arriving records
5. parallel independent sources
6. CI/CD for validate/test/deploy
7. operational alerting
```

A strong answer:

> I would first add automated tests and a safer incremental checkpoint strategy,
> because those improve correctness before adding more connector types.

---

## If asked: "Is this production-ready?"

Best answer:

> No. It is a production-inspired portfolio framework. The architecture is
> realistic, but I intentionally left areas such as retry policy, automated
> tests, late-arriving watermark handling, richer quality rules, parallelism,
> and alerting as explicit next steps.

---

## If asked: "How would you add another FILE source?"

Answer:

```text
1. land the file
2. insert metadata
3. choose FULL / APPEND / MERGE
4. configure merge key/watermark/required columns as needed
5. enable the row
6. run metadata_ingest
```

No new dataset-specific ingestion function is needed.

---

## If asked: "How would you add a brand-new connector type?"

Answer:

> I would add one reusable reader implementation, extend the `read_source`
> router for the new `source_type`, define its metadata contract, and keep the
> downstream quality/write/state/audit path unchanged.

That is the intended extension boundary.

---

## One-sentence architecture summary

> Different connectors normalize data into a common Spark DataFrame contract;
> metadata then drives incremental filtering, quality, target write strategy,
> and operational state.

---

## One-sentence watermark summary

> The watermark controls which source rows are candidates for the current run,
> and the framework advances it only after a successful write.

---

## One-sentence MERGE summary

> MERGE uses metadata-defined business keys to update existing target rows and
> insert new ones.

---

## One-sentence quarantine summary

> Required-column failures are isolated into quarantine so valid rows can still
> be processed.

---

## One-sentence audit summary

> Audit records what each source run actually did, including row counts,
> duration, success/failure, and error details.

---

## One-sentence state summary

> State stores the latest source checkpoint and run status needed for
> incremental restartability.

---

## Final memory map

```text
metadata
  ↓
router
  ↓
FILE / JDBC / API
  ↓
Spark DataFrame
  ↓
watermark
  ↓
quality
  ↓
FULL / APPEND / MERGE
  ↓
Bronze
  ↓
state + audit + quarantine
```

If you can explain that flow and the five concrete demo sources, you can explain
the project clearly without memorizing the code line by line.
