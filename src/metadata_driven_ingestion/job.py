from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from metadata_driven_ingestion.framework import ingest_all

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def volume_path(args, dataset: str) -> str:
    return (
        f"/Volumes/{args.catalog}/{args.bronze_schema}/"
        f"{args.raw_volume}/{dataset}"
    )


def ensure_source_type_column(args) -> None:
    """Upgrade an existing metadata table without deleting project state."""
    table = f"{args.catalog}.{args.metadata_schema}.ingestion_sources"
    columns = set(spark.table(table).columns)
    if "source_type" not in columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS (source_type STRING)")
    spark.sql(f"UPDATE {table} SET source_type = 'FILE' WHERE source_type IS NULL")


def setup(args) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.metadata_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.ops_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.bronze_schema}")
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS "
        f"{args.catalog}.{args.bronze_schema}.{args.raw_volume}"
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        {args.catalog}.{args.metadata_schema}.ingestion_sources (
            source_id STRING NOT NULL,
            source_type STRING,
            enabled BOOLEAN NOT NULL,
            source_format STRING NOT NULL,
            source_path STRING NOT NULL,
            target_catalog STRING NOT NULL,
            target_schema STRING NOT NULL,
            target_table STRING NOT NULL,
            load_type STRING NOT NULL,
            merge_keys ARRAY<STRING>,
            read_options MAP<STRING, STRING>,
            watermark_column STRING,
            required_columns ARRAY<STRING>,
            ingestion_order INT,
            description STRING,
            updated_at TIMESTAMP
        ) USING DELTA
    """)
    ensure_source_type_column(args)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        {args.catalog}.{args.metadata_schema}.ingestion_state (
            source_id STRING NOT NULL,
            last_watermark STRING,
            last_run_id STRING,
            status STRING,
            updated_at TIMESTAMP
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        {args.catalog}.{args.ops_schema}.ingestion_audit (
            run_id STRING,
            source_id STRING,
            target_table STRING,
            load_type STRING,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            rows_input BIGINT,
            rows_valid BIGINT,
            rows_quarantined BIGINT,
            status STRING,
            error_message STRING
        ) USING DELTA
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS
        {args.catalog}.{args.ops_schema}.quarantine (
            source_id STRING,
            quarantined_at TIMESTAMP,
            reason STRING,
            source_file STRING,
            record_json STRING
        ) USING DELTA
    """)
    print("Metadata schemas, tables, raw Volume, and source_type support are ready.")


def seed_initial_data(args) -> None:
    # Bootstrap is intentionally a clean reset for the learning/demo environment.
    spark.sql(f"DELETE FROM {args.catalog}.{args.metadata_schema}.ingestion_sources")
    spark.sql(f"DELETE FROM {args.catalog}.{args.metadata_schema}.ingestion_state")
    spark.sql(f"DELETE FROM {args.catalog}.{args.ops_schema}.ingestion_audit")
    spark.sql(f"DELETE FROM {args.catalog}.{args.ops_schema}.quarantine")
    for table in (
        "customers_md",
        "orders_md",
        "events_md",
        "jdbc_accounts_md",
        "api_products_md",
    ):
        spark.sql(f"DROP TABLE IF EXISTS {args.catalog}.{args.bronze_schema}.{table}")

    customers = spark.sql("""
        SELECT * FROM VALUES
          (101, 'Alice Andersson', 'alice@example.com', 'SE'),
          (102, 'Noah Hansen', 'noah@example.com', 'NO'),
          (103, 'Emma Jensen', 'emma@example.com', 'DK'),
          (104, 'Liam Niemi', 'liam@example.com', 'FI')
        AS t(customer_id, customer_name, email, country_code)
    """)
    (
        customers.coalesce(1).write.mode("overwrite")
        .option("header", "true").csv(volume_path(args, "customers"))
    )

    orders = spark.sql("""
        SELECT * FROM VALUES
          (5001, 101, 1001, 2, 79.0, '2026-08-11T08:00:00', 'CREATED'),
          (5002, 102, 1002, 1, 299.0, '2026-08-11T09:30:00', 'SHIPPED'),
          (5003, 101, 1003, 1, 149.0, '2026-08-11T10:15:00', 'CREATED'),
          (5004, 103, 1001, 1, 79.0, '2026-08-12T11:00:00', 'CREATED')
        AS t(order_id, customer_id, product_id, quantity, price, order_timestamp, status)
    """)
    orders.coalesce(1).write.mode("overwrite").json(volume_path(args, "orders"))

    events = spark.sql("""
        SELECT * FROM VALUES
          ('e001', 101, 'page_view', '2026-08-11T08:00:00'),
          ('e002', 102, 'add_to_cart', '2026-08-11T09:00:00'),
          ('e003', 101, 'purchase', '2026-08-11T10:00:00')
        AS t(event_id, customer_id, event_type, event_ts)
    """)
    events.coalesce(1).write.mode("overwrite").json(volume_path(args, "events"))
    print("Initial demo files written to the Unity Catalog Volume.")


def seed_incremental_data(args) -> None:
    customers = spark.sql("""
        SELECT * FROM VALUES
          (101, 'Alice Andersson', 'alice@example.com', 'SE'),
          (102, 'Noah Hansen', 'noah@example.com', 'NO'),
          (103, 'Emma Jensen', 'emma@example.com', 'DK'),
          (104, 'Liam Niemi', 'liam@example.com', 'FI'),
          (105, 'Sofia Berg', 'sofia@example.com', 'SE')
        AS t(customer_id, customer_name, email, country_code)
    """)
    (
        customers.coalesce(1).write.mode("overwrite")
        .option("header", "true").csv(volume_path(args, "customers"))
    )

    # 5001 is an update, 5005 is new, 5999 is deliberately invalid.
    orders = spark.sql("""
        SELECT * FROM VALUES
          (5001, 101, 1001, 2, 79.0, '2026-08-11T08:00:00', 'SHIPPED'),
          (5005, 104, 1004, 1, 99.0, '2026-08-12T15:00:00', 'CREATED'),
          (5999, CAST(NULL AS INT), 1002, 1, 299.0, '2026-08-12T16:00:00', 'CREATED')
        AS t(order_id, customer_id, product_id, quantity, price, order_timestamp, status)
    """)
    orders.coalesce(1).write.mode("overwrite").json(volume_path(args, "orders"))

    # e001 is older than the persisted watermark; e004/e005 are new.
    events = spark.sql("""
        SELECT * FROM VALUES
          ('e001', 101, 'page_view', '2026-08-11T08:00:00'),
          ('e004', 103, 'page_view', '2026-08-11T11:00:00'),
          ('e005', 104, 'purchase', '2026-08-11T12:00:00')
        AS t(event_id, customer_id, event_type, event_ts)
    """)
    events.coalesce(1).write.mode("overwrite").json(volume_path(args, "events"))
    print("Incremental demo batch written.")



def seed_external_templates(args) -> None:
    """Insert disabled JDBC/API templates only when they do not already exist."""
    target = f"{args.catalog}.{args.metadata_schema}.ingestion_sources"
    spark.sql(f"""
        MERGE INTO {target} AS target
        USING (
            SELECT
                'jdbc_accounts' AS source_id,
                'JDBC' AS source_type,
                false AS enabled,
                'postgresql' AS source_format,
                'db.example.com' AS source_path,
                '{args.catalog}' AS target_catalog,
                '{args.bronze_schema}' AS target_schema,
                'jdbc_accounts_md' AS target_table,
                'MERGE' AS load_type,
                array('account_id') AS merge_keys,
                map(
                    'port', '5432',
                    'database', 'app',
                    'dbtable', 'public.accounts',
                    'user', 'secret://metadata-ingestion/db-user',
                    'password', 'secret://metadata-ingestion/db-password'
                ) AS read_options,
                'updated_at' AS watermark_column,
                array('account_id') AS required_columns,
                40 AS ingestion_order,
                'Disabled database template. Uses bundled PostgreSQL connector on serverless; replace host/options and credentials, then enable.' AS description,
                current_timestamp() AS updated_at
            UNION ALL
            SELECT
                'api_products', 'API', false, 'json',
                'https://api.example.com/products', '{args.catalog}',
                '{args.bronze_schema}', 'api_products_md', 'APPEND', array(),
                map(
                    'records_path', 'data',
                    'timeout_seconds', '30',
                    'header.Authorization', 'secret://metadata-ingestion/api-authorization'
                ), 'updated_at', array('id'), 50,
                'Disabled HTTP GET API template. Replace URL/options and credentials, then enable.',
                current_timestamp()
        ) AS source
        ON target.source_id = source.source_id
        WHEN NOT MATCHED THEN INSERT *
    """)

def seed_metadata(args) -> None:
    """Upsert demo FILE sources and insert disabled JDBC/API templates if absent."""
    customers_path = volume_path(args, "customers")
    orders_path = volume_path(args, "orders")
    events_path = volume_path(args, "events")
    target = f"{args.catalog}.{args.metadata_schema}.ingestion_sources"

    # Existing FILE demos are intentionally refreshed to the repo's canonical config.
    spark.sql(f"""
        MERGE INTO {target} AS target
        USING (
            SELECT
                'customers' AS source_id,
                'FILE' AS source_type,
                true AS enabled,
                'csv' AS source_format,
                '{customers_path}' AS source_path,
                '{args.catalog}' AS target_catalog,
                '{args.bronze_schema}' AS target_schema,
                'customers_md' AS target_table,
                'FULL' AS load_type,
                array() AS merge_keys,
                map('header', 'true', 'inferSchema', 'true') AS read_options,
                CAST(NULL AS STRING) AS watermark_column,
                array('customer_id', 'email') AS required_columns,
                10 AS ingestion_order,
                'Small customer snapshot; full refresh demo.' AS description,
                current_timestamp() AS updated_at
            UNION ALL
            SELECT
                'orders', 'FILE', true, 'json', '{orders_path}', '{args.catalog}',
                '{args.bronze_schema}', 'orders_md', 'MERGE', array('order_id'),
                map('inferSchema', 'true'), CAST(NULL AS STRING),
                array('order_id', 'customer_id', 'product_id'), 20,
                'Order upsert demo using Delta MERGE.', current_timestamp()
            UNION ALL
            SELECT
                'events', 'FILE', true, 'json', '{events_path}', '{args.catalog}',
                '{args.bronze_schema}', 'events_md', 'APPEND', array(),
                map('inferSchema', 'true'), 'event_ts',
                array('event_id', 'event_ts'), 30,
                'Append-only event feed using a persisted watermark.', current_timestamp()
        ) AS source
        ON target.source_id = source.source_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    seed_external_templates(args)
    print("Metadata seeded: FILE demos enabled; JDBC/API templates disabled.")


def upgrade_metadata(args) -> None:
    """Upgrade metadata shape and add missing external templates without resetting data."""
    # Non-destructive: setup only creates missing objects/adds source_type support,
    # and external templates are insert-only.
    setup(args)
    seed_external_templates(args)
    print(
        "Metadata upgraded without resetting state/audit/quarantine, target tables, "
        "or existing source configurations."
    )


def ingest(args) -> None:
    ingest_all(
        spark,
        catalog=args.catalog,
        metadata_schema=args.metadata_schema,
        ops_schema=args.ops_schema,
    )


def verify(args) -> None:
    print("\n=== CONFIGURATION ===")
    configs = spark.sql(f"""
        SELECT source_id, source_type, enabled, source_format, source_path,
               target_catalog, target_schema, target_table, load_type,
               merge_keys, watermark_column, required_columns, ingestion_order
        FROM {args.catalog}.{args.metadata_schema}.ingestion_sources
        ORDER BY ingestion_order, source_id
    """)
    configs.show(truncate=False)

    print("\n=== STATE ===")
    spark.sql(f"""
        SELECT * FROM {args.catalog}.{args.metadata_schema}.ingestion_state
        ORDER BY source_id
    """).show(truncate=False)

    print("\n=== RECENT AUDIT ===")
    spark.sql(f"""
        SELECT source_id, load_type, rows_input, rows_valid, rows_quarantined,
               status, ended_at
        FROM {args.catalog}.{args.ops_schema}.ingestion_audit
        ORDER BY ended_at DESC
        LIMIT 20
    """).show(truncate=False)

    print("\n=== ENABLED TARGET COUNTS ===")
    for row in configs.collect():
        if not row["enabled"]:
            continue
        target = f"{row['target_catalog']}.{row['target_schema']}.{row['target_table']}"
        try:
            count = spark.table(target).count()
            print(f"{row['source_id']}: {target} -> {count}")
        except Exception as exc:
            print(f"{row['source_id']}: {target} -> unavailable ({type(exc).__name__})")

    print("\n=== QUARANTINE COUNT ===")
    print(spark.table(f"{args.catalog}.{args.ops_schema}.quarantine").count())


COMMANDS = {
    "setup": setup,
    "seed-initial-data": seed_initial_data,
    "seed-incremental-data": seed_incremental_data,
    "seed-metadata": seed_metadata,
    "upgrade-metadata": upgrade_metadata,
    "ingest": ingest,
    "verify": verify,
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=sorted(COMMANDS))
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--metadata-schema", required=True)
    parser.add_argument("--ops-schema", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--raw-volume", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    COMMANDS[args.command](args)


if __name__ == "__main__":
    main()
