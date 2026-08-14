from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from functools import reduce
from typing import Iterable, Optional
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

VALID_LOAD_TYPES = {"FULL", "APPEND", "MERGE"}
VALID_SOURCE_TYPES = {"FILE", "JDBC", "API"}


def normalize_load_type(value: str) -> str:
    value = (value or "").strip().upper()
    if value not in VALID_LOAD_TYPES:
        raise ValueError(
            f"Unsupported load_type={value!r}. Expected one of {sorted(VALID_LOAD_TYPES)}."
        )
    return value


def normalize_source_type(value: Optional[str]) -> str:
    # Backward compatibility: rows created before source_type existed are FILE sources.
    value = (value or "FILE").strip().upper()
    if value not in VALID_SOURCE_TYPES:
        raise ValueError(
            f"Unsupported source_type={value!r}. Expected one of {sorted(VALID_SOURCE_TYPES)}."
        )
    return value


def config_value(config: Row, key: str, default=None):
    values = config.asDict(recursive=False)
    value = values.get(key, default)
    return default if value is None else value


def safe_temp_view_name(source_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", source_id)
    return f"incoming_{cleaned}"


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def sql_string(value: Optional[object]) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def resolve_runtime_value(value: str) -> str:
    """Resolve runtime credential references without storing secrets in metadata.

    Supported forms:
      - env://NAME
      - secret://scope/key   (Databricks secret scope)
    """
    if not isinstance(value, str):
        return value

    if value.startswith("env://"):
        name = value[len("env://") :]
        resolved = os.environ.get(name)
        if resolved is None:
            raise ValueError(f"Required environment variable {name!r} is not set")
        return resolved

    if value.startswith("secret://"):
        reference = value[len("secret://") :]
        if "/" not in reference:
            raise ValueError(
                "secret:// references must use secret://<scope>/<key>"
            )
        scope, key = reference.split("/", 1)
        if not scope or not key:
            raise ValueError(
                "secret:// references must use secret://<scope>/<key>"
            )
        try:
            from databricks.sdk.runtime import dbutils
        except ImportError as exc:
            raise RuntimeError(
                "secret:// resolution requires the Databricks runtime. "
                "Use env://NAME outside Databricks."
            ) from exc
        return dbutils.secrets.get(scope=scope, key=key)

    return value


def resolved_options(config: Row) -> dict[str, str]:
    return {
        str(key): resolve_runtime_value(str(value))
        for key, value in dict(config_value(config, "read_options", {}) or {}).items()
    }


def table_exists(spark: SparkSession, catalog: str, schema: str, table: str) -> bool:
    query = f"""
        SELECT 1
        FROM {quote_identifier(catalog)}.information_schema.tables
        WHERE table_schema = {sql_string(schema)}
          AND table_name = {sql_string(table)}
        LIMIT 1
    """
    return spark.sql(query).count() > 0


def load_enabled_configs(spark: SparkSession, catalog: str, metadata_schema: str) -> list[Row]:
    table = f"{quote_identifier(catalog)}.{quote_identifier(metadata_schema)}.ingestion_sources"
    return spark.sql(f"""
        SELECT *
        FROM {table}
        WHERE enabled = true
        ORDER BY ingestion_order, source_id
    """).collect()


def add_lineage_columns(df: DataFrame, source_id: str, source_reference: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(source_reference))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_id", F.lit(source_id))
    )


def read_file_source(spark: SparkSession, config: Row) -> DataFrame:
    options = resolved_options(config)
    df = (
        spark.read.format(config["source_format"])
        .options(**options)
        .load(config["source_path"])
        .select("*", "_metadata")
    )
    return (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_id", F.lit(config["source_id"]))
        .drop("_metadata")
    )


def read_jdbc_source(spark: SparkSession, config: Row) -> DataFrame:
    """Read a database source into a Spark DataFrame.

    Databricks-bundled database connectors use source_format as the connector
    name and source_path as the host. Generic JDBC on serverless/standard
    compute should use a Unity Catalog JDBC connection.
    """
    options = resolved_options(config)
    source_format = str(config_value(config, "source_format", "jdbc")).strip().lower()

    if not options.get("dbtable") and not options.get("query"):
        raise ValueError(
            f"Database source {config['source_id']!r} requires read_options['dbtable'] "
            "or read_options['query']"
        )

    # Bundled connector path, for example source_format='postgresql'.
    if source_format != "jdbc":
        options.setdefault("host", config["source_path"])
        df = spark.read.format(source_format).options(**options).load()
        return add_lineage_columns(
            df,
            config["source_id"],
            f"{source_format}:{config['source_id']}",
        )

    # Generic JDBC path via a Unity Catalog JDBC connection.
    if not options.get("databricks.connection"):
        raise ValueError(
            f"Generic JDBC source {config['source_id']!r} requires "
            "read_options['databricks.connection'] on serverless/standard compute. "
            "For PostgreSQL, prefer source_format='postgresql'."
        )

    df = spark.read.format("jdbc").options(**options).load()
    return add_lineage_columns(df, config["source_id"], f"jdbc:{config['source_id']}")


def extract_api_records(payload, records_path: Optional[str]) -> list[dict]:
    current = payload
    if records_path:
        for part in records_path.split("."):
            if not isinstance(current, dict) or part not in current:
                raise ValueError(
                    f"API records_path={records_path!r} was not found in the JSON response"
                )
            current = current[part]

    if current is None:
        return []
    if isinstance(current, dict):
        return [current]
    if isinstance(current, list):
        if not all(isinstance(item, dict) for item in current):
            raise ValueError("API record arrays must contain JSON objects")
        return current
    raise ValueError("API response/records_path must resolve to a JSON object or array")


def build_api_url(base_url: str, params: dict[str, str]) -> str:
    parts = urlsplit(base_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def read_api_source(spark: SparkSession, config: Row) -> DataFrame:
    options = resolved_options(config)
    records_path = options.pop("records_path", None)
    timeout_seconds = int(options.pop("timeout_seconds", "30"))
    page_param = options.pop("page_param", None)
    start_page = int(options.pop("start_page", "1"))
    max_pages = int(options.pop("max_pages", "1"))

    headers = {
        key[len("header.") :]: value
        for key, value in list(options.items())
        if key.startswith("header.")
    }
    params = {
        key[len("param.") :]: value
        for key, value in list(options.items())
        if key.startswith("param.")
    }
    handled_keys = {
        key for key in options if key.startswith("header.") or key.startswith("param.")
    }
    unknown = sorted(set(options) - handled_keys)
    if unknown:
        raise ValueError(
            f"Unsupported API read_options for {config['source_id']!r}: {unknown}. "
            "Supported keys: records_path, timeout_seconds, page_param, start_page, "
            "max_pages, header.<name>, param.<name>."
        )

    all_records: list[dict] = []
    pages = max_pages if page_param else 1
    for page_offset in range(pages):
        request_params = dict(params)
        if page_param:
            request_params[page_param] = str(start_page + page_offset)

        url = build_api_url(config["source_path"], request_params)
        request = Request(url, headers=headers, method="GET")
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))

        records = extract_api_records(payload, records_path)
        all_records.extend(records)
        if page_param and not records:
            break

    if all_records:
        df = spark.createDataFrame(all_records)
    else:
        # An empty, zero-column DataFrame lets APPEND/MERGE behave as a no-op.
        df = spark.createDataFrame([], StructType([]))

    # Do not put the full URL into lineage because URLs can contain sensitive query params.
    return add_lineage_columns(df, config["source_id"], f"api:{config['source_id']}")


def read_source(spark: SparkSession, config: Row) -> DataFrame:
    source_type = normalize_source_type(config_value(config, "source_type", "FILE"))
    if source_type == "FILE":
        return read_file_source(spark, config)
    if source_type == "JDBC":
        return read_jdbc_source(spark, config)
    if source_type == "API":
        return read_api_source(spark, config)
    raise AssertionError(f"Unhandled source_type={source_type}")


def get_last_watermark(
    spark: SparkSession, catalog: str, metadata_schema: str, source_id: str
) -> Optional[str]:
    rows = spark.sql(f"""
        SELECT last_watermark
        FROM {quote_identifier(catalog)}.{quote_identifier(metadata_schema)}.ingestion_state
        WHERE source_id = {sql_string(source_id)}
        LIMIT 1
    """).collect()
    return rows[0]["last_watermark"] if rows else None


def apply_watermark(
    df: DataFrame, watermark_column: Optional[str], last_watermark: Optional[str]
) -> DataFrame:
    if not watermark_column or last_watermark is None:
        return df
    if watermark_column not in df.columns:
        raise ValueError(
            f"Configured watermark column {watermark_column!r} is missing. "
            f"Available columns: {df.columns}"
        )
    return df.filter(F.col(watermark_column) > F.lit(last_watermark))


def split_quality(
    df: DataFrame, required_columns: Iterable[str]
) -> tuple[DataFrame, DataFrame]:
    required = list(required_columns or [])
    if not required:
        return df, df.limit(0)
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing: {missing}; available={df.columns}")
    invalid_condition = reduce(
        lambda left, right: left | right,
        [F.col(c).isNull() for c in required],
    )
    return df.filter(~invalid_condition), df.filter(invalid_condition)


def write_quarantine(
    invalid: DataFrame,
    catalog: str,
    ops_schema: str,
    source_id: str,
    required_columns: Iterable[str],
) -> int:
    count = invalid.count()
    if count == 0:
        return 0
    record_columns = [F.col(c) for c in invalid.columns]
    output = invalid.select(
        F.lit(source_id).alias("source_id"),
        F.current_timestamp().alias("quarantined_at"),
        F.lit("NULL_REQUIRED_COLUMN:" + ",".join(required_columns or [])).alias("reason"),
        F.col("_source_file").cast("string").alias("source_file"),
        F.to_json(F.struct(*record_columns)).alias("record_json"),
    )
    output.write.mode("append").saveAsTable(f"{catalog}.{ops_schema}.quarantine")
    return count


def write_full(df: DataFrame, target_table: str) -> None:
    (
        df.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )


def write_append(
    spark: SparkSession, df: DataFrame, catalog: str, schema: str, table: str
) -> None:
    target = f"{catalog}.{schema}.{table}"
    if table_exists(spark, catalog, schema, table):
        df.write.mode("append").saveAsTable(target)
    else:
        df.write.mode("overwrite").saveAsTable(target)


def write_merge(
    spark: SparkSession,
    df: DataFrame,
    catalog: str,
    schema: str,
    table: str,
    merge_keys: Iterable[str],
    source_id: str,
) -> None:
    merge_keys = list(merge_keys or [])
    if not merge_keys:
        raise ValueError(f"MERGE source {source_id!r} requires at least one merge key")
    missing = [k for k in merge_keys if k not in df.columns]
    if missing:
        raise ValueError(f"Merge keys missing: {missing}; available={df.columns}")

    target = f"{catalog}.{schema}.{table}"
    incoming = df.dropDuplicates(merge_keys)
    if not table_exists(spark, catalog, schema, table):
        incoming.write.mode("overwrite").saveAsTable(target)
        return

    temp_view = safe_temp_view_name(source_id)
    incoming.createOrReplaceTempView(temp_view)
    join_condition = " AND ".join(
        f"target.{quote_identifier(k)} = source.{quote_identifier(k)}" for k in merge_keys
    )
    spark.sql(f"""
        MERGE INTO {target} AS target
        USING {quote_identifier(temp_view)} AS source
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def compute_watermark(df: DataFrame, watermark_column: Optional[str]) -> Optional[str]:
    if not watermark_column or watermark_column not in df.columns:
        return None
    value = df.agg(F.max(F.col(watermark_column))).collect()[0][0]
    return None if value is None else str(value)


def update_state(
    spark: SparkSession,
    catalog: str,
    metadata_schema: str,
    source_id: str,
    run_id: str,
    last_watermark: Optional[str],
    status: str,
) -> None:
    target = f"{catalog}.{metadata_schema}.ingestion_state"
    spark.sql(f"""
        MERGE INTO {target} AS target
        USING (
            SELECT
                {sql_string(source_id)} AS source_id,
                {sql_string(last_watermark)} AS last_watermark,
                {sql_string(run_id)} AS last_run_id,
                {sql_string(status)} AS status,
                current_timestamp() AS updated_at
        ) AS source
        ON target.source_id = source.source_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def write_audit(
    spark: SparkSession,
    catalog: str,
    ops_schema: str,
    *,
    run_id: str,
    source_id: str,
    target_table: str,
    load_type: str,
    started_at: datetime,
    rows_input: int,
    rows_valid: int,
    rows_quarantined: int,
    status: str,
    error_message: Optional[str],
) -> None:
    started = started_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    spark.sql(f"""
        INSERT INTO {catalog}.{ops_schema}.ingestion_audit
        SELECT
            {sql_string(run_id)},
            {sql_string(source_id)},
            {sql_string(target_table)},
            {sql_string(load_type)},
            TIMESTAMP {sql_string(started)},
            current_timestamp(),
            {int(rows_input)},
            {int(rows_valid)},
            {int(rows_quarantined)},
            {sql_string(status)},
            {sql_string(error_message)}
    """)


def ingest_one(
    spark: SparkSession,
    config: Row,
    *,
    catalog: str,
    metadata_schema: str,
    ops_schema: str,
) -> None:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    source_id = config["source_id"]
    source_type = normalize_source_type(config_value(config, "source_type", "FILE"))
    load_type = normalize_load_type(config["load_type"])
    target_catalog = config["target_catalog"]
    target_schema = config["target_schema"]
    target_table_name = config["target_table"]
    target_table = f"{target_catalog}.{target_schema}.{target_table_name}"
    rows_input = rows_valid = rows_quarantined = 0

    print(
        f"[START] source={source_id} source_type={source_type} "
        f"load_type={load_type} target={target_table}"
    )

    try:
        df = read_source(spark, config)
        last_watermark = get_last_watermark(
            spark, catalog, metadata_schema, source_id
        )
        df = apply_watermark(df, config["watermark_column"], last_watermark)
        rows_input = df.count()

        if rows_input == 0:
            valid, invalid = df, df.limit(0)
            rows_valid = rows_quarantined = 0
        else:
            valid, invalid = split_quality(df, config["required_columns"])
            rows_valid = valid.count()
            rows_quarantined = write_quarantine(
                invalid,
                catalog,
                ops_schema,
                source_id,
                config["required_columns"],
            )

        if load_type == "FULL":
            write_full(valid, target_table)
        elif load_type == "APPEND":
            if rows_valid:
                write_append(
                    spark, valid, target_catalog, target_schema, target_table_name
                )
        elif load_type == "MERGE":
            if rows_valid:
                write_merge(
                    spark,
                    valid,
                    target_catalog,
                    target_schema,
                    target_table_name,
                    config["merge_keys"],
                    source_id,
                )

        new_watermark = compute_watermark(valid, config["watermark_column"])
        if new_watermark is None:
            new_watermark = last_watermark

        update_state(
            spark,
            catalog,
            metadata_schema,
            source_id,
            run_id,
            new_watermark,
            "SUCCESS",
        )
        write_audit(
            spark,
            catalog,
            ops_schema,
            run_id=run_id,
            source_id=source_id,
            target_table=target_table,
            load_type=load_type,
            started_at=started_at,
            rows_input=rows_input,
            rows_valid=rows_valid,
            rows_quarantined=rows_quarantined,
            status="SUCCESS",
            error_message=None,
        )
        print(
            f"[SUCCESS] source={source_id} input={rows_input} "
            f"valid={rows_valid} quarantined={rows_quarantined}"
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        write_audit(
            spark,
            catalog,
            ops_schema,
            run_id=run_id,
            source_id=source_id,
            target_table=target_table,
            load_type=load_type,
            started_at=started_at,
            rows_input=rows_input,
            rows_valid=rows_valid,
            rows_quarantined=rows_quarantined,
            status="FAILED",
            error_message=error[:4000],
        )
        update_state(
            spark,
            catalog,
            metadata_schema,
            source_id,
            run_id,
            get_last_watermark(spark, catalog, metadata_schema, source_id),
            "FAILED",
        )
        print(f"[FAILED] source={source_id}: {error}")
        raise


def ingest_all(
    spark: SparkSession, *, catalog: str, metadata_schema: str, ops_schema: str
) -> None:
    configs = load_enabled_configs(spark, catalog, metadata_schema)
    print(f"Enabled metadata sources: {len(configs)}")
    for config in configs:
        ingest_one(
            spark,
            config,
            catalog=catalog,
            metadata_schema=metadata_schema,
            ops_schema=ops_schema,
        )
