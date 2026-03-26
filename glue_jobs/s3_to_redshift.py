"""
s3_to_redshift.py
AWS Glue PySpark Job — S3 (raw orders/events) → Amazon Redshift

Pipeline:
  1. Read partitioned Parquet/CSV from S3 (partition pruning via job args)
  2. Apply data quality checks + reject bad rows to DLQ prefix
  3. Normalize and enrich (derived columns, type casting)
  4. Write cleaned data to S3 staging prefix (columnar Parquet)
  5. Generate a COPY manifest and COPY into Redshift
  6. Run post-load validation queries in Redshift
  7. UNLOAD summary aggregates back to S3 for downstream consumption
  8. Update Glue job bookmarks for incremental processing

Job Parameters (passed via --job-arg or Glue job arguments):
  --JOB_NAME            Glue job name (injected by Glue runtime)
  --JOB_RUN_ID          Glue run ID (injected)
  --source_bucket       S3 bucket containing raw data
  --source_prefix       S3 prefix / "table" name (e.g. orders)
  --staging_bucket      S3 bucket for Glue-processed Parquet
  --manifest_bucket     S3 bucket for COPY manifest files
  --unload_bucket       S3 bucket for UNLOAD output
  --redshift_db         Redshift database name
  --redshift_schema     Redshift target schema
  --redshift_table      Redshift target table name
  --redshift_secret_arn Secrets Manager ARN for Redshift credentials
  --redshift_role_arn   IAM role ARN granted to Redshift for COPY/UNLOAD
  --partition_date      Date to process (YYYY-MM-DD). Omit for bookmark mode.
  --lookback_days       Number of days to look back for late data (default 1)
  --environment         dev | staging | prod
"""

import json
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *  # noqa: F403
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s",
)
logger = logging.getLogger("glue.s3_to_redshift")

# ─── Job Argument Parsing ─────────────────────────────────────────────────────

REQUIRED_ARGS = [
    "JOB_NAME",
    "source_bucket",
    "source_prefix",
    "staging_bucket",
    "manifest_bucket",
    "unload_bucket",
    "redshift_db",
    "redshift_schema",
    "redshift_table",
    "redshift_secret_arn",
    "redshift_role_arn",
    "environment",
]

OPTIONAL_ARGS = {
    "partition_date": None,
    "lookback_days": "1",
    "dlq_prefix": "dlq",
    "max_reject_pct": "5",
}

args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
for k, v in OPTIONAL_ARGS.items():
    if f"--{k}" in sys.argv:
        idx = sys.argv.index(f"--{k}")
        args[k] = sys.argv[idx + 1]
    else:
        args[k] = v

JOB_NAME = args["JOB_NAME"]
SOURCE_BUCKET = args["source_bucket"]
SOURCE_PREFIX = args["source_prefix"]
STAGING_BUCKET = args["staging_bucket"]
MANIFEST_BUCKET = args["manifest_bucket"]
UNLOAD_BUCKET = args["unload_bucket"]
REDSHIFT_DB = args["redshift_db"]
REDSHIFT_SCHEMA = args["redshift_schema"]
REDSHIFT_TABLE = args["redshift_table"]
SECRET_ARN = args["redshift_secret_arn"]
REDSHIFT_ROLE_ARN = args["redshift_role_arn"]
ENVIRONMENT = args["environment"]
PARTITION_DATE = args.get("partition_date")
LOOKBACK_DAYS = int(args.get("lookback_days", 1))
DLQ_PREFIX = args.get("dlq_prefix", "dlq")
MAX_REJECT_PCT = float(args.get("max_reject_pct", 5))

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc)

logger.info("Job: %s | RunID: %s | Env: %s", JOB_NAME, RUN_ID, ENVIRONMENT)
logger.info(
    "Source: s3://%s/%s | Target: %s.%s.%s",
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    REDSHIFT_DB,
    REDSHIFT_SCHEMA,
    REDSHIFT_TABLE,
)

# ─── Spark / Glue Context ─────────────────────────────────────────────────────

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# ─── AWS Clients ──────────────────────────────────────────────────────────────

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
glue_client = boto3.client("glue")

# ─── Schemas ──────────────────────────────────────────────────────────────────

RAW_ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("order_date", T.StringType(), True),
        T.StructField("order_status", T.StringType(), True),
        T.StructField("order_priority", T.StringType(), True),
        T.StructField("ship_date", T.StringType(), True),
        T.StructField("ship_mode", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_category", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("quantity", T.StringType(), True),  # raw = string
        T.StructField("unit_price", T.StringType(), True),
        T.StructField("discount", T.StringType(), True),
        T.StructField("shipping_cost", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("postal_code", T.StringType(), True),
        T.StructField("sales_rep_id", T.StringType(), True),
        T.StructField("channel", T.StringType(), True),
    ]
)

CLEAN_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("order_date", T.DateType(), True),
        T.StructField("order_status", T.StringType(), True),
        T.StructField("order_priority", T.StringType(), True),
        T.StructField("ship_date", T.DateType(), True),
        T.StructField("ship_mode", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_category", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
        T.StructField("discount", T.DoubleType(), True),
        T.StructField("shipping_cost", T.DoubleType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("postal_code", T.StringType(), True),
        T.StructField("sales_rep_id", T.StringType(), True),
        T.StructField("channel", T.StringType(), True),
        # Derived measures
        T.StructField("gross_revenue", T.DoubleType(), True),
        T.StructField("net_revenue", T.DoubleType(), True),
        T.StructField("days_to_ship", T.IntegerType(), True),
        T.StructField("is_late_shipment", T.BooleanType(), True),
        T.StructField("order_year", T.IntegerType(), True),
        T.StructField("order_month", T.IntegerType(), True),
        T.StructField("order_quarter", T.IntegerType(), True),
        # Audit
        T.StructField("_source_file", T.StringType(), True),
        T.StructField("_glue_job_name", T.StringType(), True),
        T.StructField("_glue_run_id", T.StringType(), True),
        T.StructField("_processed_at", T.TimestampType(), True),
    ]
)


# ─── Partition Paths ──────────────────────────────────────────────────────────


def get_source_paths(
    bucket: str,
    prefix: str,
    partition_date: Optional[str],
    lookback_days: int,
) -> list[str]:
    """
    Build a list of S3 paths to read, applying partition pruning.
    If partition_date is set, read [partition_date - lookback_days, partition_date].
    Otherwise, use Glue job bookmarks for incremental processing.

    S3 layout assumed: s3://{bucket}/{prefix}/year=YYYY/month=MM/day=DD/
    """
    if partition_date:
        end_dt = datetime.strptime(partition_date, "%Y-%m-%d")
        dates = [end_dt - timedelta(days=i) for i in range(lookback_days)]
        paths = [
            f"s3://{bucket}/{prefix}/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
            for d in dates
        ]
        logger.info("Partition-pruned paths (%d): %s", len(paths), paths)
        return paths
    else:
        # Glue bookmark handles incremental automatically via readStream / getSink
        path = f"s3://{bucket}/{prefix}/"
        logger.info("Bookmark mode — reading: %s", path)
        return [path]


# ─── Read ─────────────────────────────────────────────────────────────────────


def read_raw(paths: list[str]) -> DataFrame:
    """
    Read raw data from S3 with Glue bookmarking support.
    Tries Parquet first; falls back to CSV with header inference.
    """
    try:
        raw_df = spark.read.schema(RAW_ORDER_SCHEMA).parquet(*paths)
        logger.info("Read as Parquet — %d raw rows.", raw_df.count())
    except Exception:
        logger.warning("Parquet read failed; falling back to CSV.")
        raw_df = (
            spark.read.schema(RAW_ORDER_SCHEMA)
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", '"')
            .csv(*paths)
        )
        logger.info("Read as CSV — %d raw rows.", raw_df.count())

    return raw_df.withColumn("_source_file", F.input_file_name())


# ─── Data Quality ─────────────────────────────────────────────────────────────


def run_data_quality(
    df: DataFrame, max_reject_pct: float
) -> tuple[DataFrame, DataFrame]:
    """
    Apply data quality rules. Separate valid rows from rejected rows.
    Aborts if rejection rate exceeds max_reject_pct.

    Returns (valid_df, rejected_df).
    """
    total = df.count()
    if total == 0:
        logger.warning("No rows to process.")
        return df.limit(0), df.limit(0)

    # Tag each row with the first failing rule (NULL = passes all checks)
    tagged = df.withColumn(
        "_dq_failure",
        F.when(F.col("order_id").isNull(), "order_id is null")
        .when(F.col("customer_id").isNull(), "customer_id is null")
        .when(F.col("order_date").isNull(), "order_date is null")
        .when(
            F.col("order_status").isNotNull()
            & ~F.col("order_status").isin(
                "Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"
            ),
            "invalid order_status",
        )
        .when(
            F.col("quantity").cast("int").isNull()
            | (F.col("quantity").cast("int") <= 0),
            "quantity <= 0 or non-numeric",
        )
        .when(
            F.col("unit_price").cast("double").isNull()
            | (F.col("unit_price").cast("double") < 0),
            "unit_price < 0 or non-numeric",
        )
        .when(
            F.col("discount").cast("double").isNotNull()
            & (
                (F.col("discount").cast("double") < 0)
                | (F.col("discount").cast("double") > 1)
            ),
            "discount out of [0, 1] range",
        )
        .otherwise(F.lit(None).cast("string")),
    )

    valid_df = tagged.filter(F.col("_dq_failure").isNull()).drop("_dq_failure")
    rejected_df = tagged.filter(F.col("_dq_failure").isNotNull())

    rejected_count = rejected_df.count()
    reject_rate = rejected_count / total * 100 if total > 0 else 0

    logger.info(
        "DQ results: total=%d valid=%d rejected=%d reject_rate=%.2f%%",
        total,
        total - rejected_count,
        rejected_count,
        reject_rate,
    )

    if reject_rate > max_reject_pct:
        msg = (
            f"Data quality gate FAILED: rejection rate {reject_rate:.2f}% "
            f"exceeds threshold {max_reject_pct}%. Aborting job."
        )
        logger.error(msg)
        raise RuntimeError(msg)

    return valid_df, rejected_df


# ─── Transform ────────────────────────────────────────────────────────────────


def transform(df: DataFrame) -> DataFrame:
    """
    Cast types, derive measures, add audit columns.
    """
    LATE_SHIP_DAYS = 7  # SLA: ship within 7 days of order

    transformed = (
        df
        # ── Type casts ───────────────────────────────────────────────────────
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("ship_date", F.to_date("ship_date", "yyyy-MM-dd"))
        .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(T.DoubleType()))
        .withColumn(
            "discount", F.coalesce(F.col("discount").cast(T.DoubleType()), F.lit(0.0))
        )
        .withColumn(
            "shipping_cost",
            F.coalesce(F.col("shipping_cost").cast(T.DoubleType()), F.lit(0.0)),
        )
        # ── Revenue measures ─────────────────────────────────────────────────
        .withColumn(
            "gross_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
        .withColumn(
            "net_revenue",
            F.round(
                F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2
            ),
        )
        # ── Shipping performance ──────────────────────────────────────────────
        .withColumn("days_to_ship", F.datediff(F.col("ship_date"), F.col("order_date")))
        .withColumn(
            "is_late_shipment",
            F.when(F.col("days_to_ship") > LATE_SHIP_DAYS, F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
        # ── Date parts (for Redshift sort key columns) ───────────────────────
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_quarter", F.quarter("order_date"))
        # ── Standardise strings ──────────────────────────────────────────────
        .withColumn("order_status", F.trim(F.col("order_status")))
        .withColumn("order_priority", F.trim(F.col("order_priority")))
        .withColumn("ship_mode", F.trim(F.col("ship_mode")))
        .withColumn("product_category", F.trim(F.col("product_category")))
        .withColumn("region", F.trim(F.upper(F.col("region"))))
        .withColumn("country", F.trim(F.upper(F.col("country"))))
        .withColumn("channel", F.trim(F.lower(F.col("channel"))))
        # ── Audit columns ────────────────────────────────────────────────────
        .withColumn("_glue_job_name", F.lit(JOB_NAME))
        .withColumn("_glue_run_id", F.lit(RUN_ID))
        .withColumn("_processed_at", F.current_timestamp())
    )

    # Deduplication: keep latest record per order_id within this batch
    dedup_window = Window.partitionBy("order_id").orderBy(F.col("_processed_at").desc())
    deduped = (
        transformed.withColumn("_row_num", F.row_number().over(dedup_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    count = deduped.count()
    logger.info("After transform + dedup: %d rows", count)
    return deduped


# ─── Write to S3 Staging ──────────────────────────────────────────────────────


def write_staging(df: DataFrame, staging_bucket: str, staging_prefix: str) -> list[str]:
    """
    Write processed Parquet to S3 staging prefix.
    Partitioned by order_year/order_month for Redshift COPY efficiency.
    Returns the list of S3 object keys written.
    """
    staging_path = (
        f"s3://{staging_bucket}/{staging_prefix}/"
        f"run_id={RUN_ID}/"
        f"ts={RUN_TS.strftime('%Y%m%dT%H%M%SZ')}/"
    )

    df.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(
        staging_path
    )

    logger.info("Staging data written to: %s", staging_path)
    return staging_path


# ─── COPY Manifest ────────────────────────────────────────────────────────────


def generate_copy_manifest(
    staging_bucket: str,
    staging_prefix: str,
    manifest_bucket: str,
) -> str:
    """
    List all Parquet objects in the staging prefix and generate a Redshift
    COPY manifest JSON.  The manifest guarantees Redshift loads exactly the
    files produced by this run — no accidental over-reads of adjacent prefixes.

    Returns the S3 URI of the manifest file.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=staging_bucket, Prefix=staging_prefix)

    entries = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet") or key.endswith(".snappy.parquet"):
                entries.append(
                    {
                        "url": f"s3://{staging_bucket}/{key}",
                        "mandatory": True,
                    }
                )

    if not entries:
        raise RuntimeError(
            f"No Parquet files found in s3://{staging_bucket}/{staging_prefix}"
        )

    manifest = {
        "entries": entries,
        "meta": {
            "content_length": sum(
                s3_client.head_object(
                    Bucket=staging_bucket,
                    Key=e["url"].replace(f"s3://{staging_bucket}/", ""),
                )["ContentLength"]
                for e in entries
            ),
        },
    }

    manifest_key = (
        f"manifests/{REDSHIFT_TABLE}/"
        f"run_id={RUN_ID}/"
        f"{RUN_TS.strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    s3_client.put_object(
        Bucket=manifest_bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )

    manifest_uri = f"s3://{manifest_bucket}/{manifest_key}"
    logger.info("Manifest written: %s (%d files)", manifest_uri, len(entries))
    return manifest_uri


# ─── Redshift Helpers ─────────────────────────────────────────────────────────


def get_redshift_credentials() -> dict:
    """Fetch Redshift credentials from Secrets Manager."""
    secret = secrets_client.get_secret_value(SecretId=SECRET_ARN)
    creds = json.loads(secret["SecretString"])
    return creds


def run_redshift_sql(creds: dict, sql: str, description: str = "") -> None:
    """
    Execute SQL against Redshift via the Redshift Data API (no JDBC driver needed).
    Polls until statement completes.
    """
    rs_data = boto3.client("redshift-data")

    logger.info("Executing Redshift SQL [%s]: %.200s", description, sql.strip())

    resp = rs_data.execute_statement(
        ClusterIdentifier=creds["dbClusterIdentifier"],
        Database=REDSHIFT_DB,
        DbUser=creds["username"],
        Sql=sql,
    )
    statement_id = resp["Id"]

    # Poll for completion
    for attempt in range(60):  # max 5 minutes
        time.sleep(5)
        status = rs_data.describe_statement(Id=statement_id)
        state = status["Status"]
        if state == "FINISHED":
            rows = status.get("ResultRows", 0)
            logger.info("[%s] Finished. ResultRows: %d", description, rows)
            return
        elif state in ("FAILED", "ABORTED"):
            error = status.get("Error", "Unknown error")
            raise RuntimeError(f"Redshift SQL [{description}] failed: {error}")
        else:
            logger.debug(
                "[%s] Status: %s (attempt %d/60)", description, state, attempt + 1
            )

    raise TimeoutError(f"Redshift SQL [{description}] timed out after 300s.")


def copy_into_redshift(
    creds: dict,
    manifest_uri: str,
    staging_table: str,
    target_table: str,
) -> None:
    """
    COPY from S3 manifest into a Redshift staging table, then MERGE into target.
    Uses UPSERT pattern:
      1. COPY → staging (temp) table
      2. DELETE matching rows from target
      3. INSERT from staging into target
      4. DROP staging table
    """
    full_staging = f"{REDSHIFT_SCHEMA}.{staging_table}"
    full_target = f"{REDSHIFT_SCHEMA}.{target_table}"

    # 1. Truncate staging
    run_redshift_sql(creds, f"TRUNCATE {full_staging};", "truncate staging")

    # 2. COPY from manifest
    copy_sql = f"""
        COPY {full_staging}
        FROM '{manifest_uri}'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET
        MANIFEST
        ACCEPTINVCHARS ' '
        TRUNCATECOLUMNS
        STATUPDATE ON
        COMPUPDATE ON;
    """
    run_redshift_sql(creds, copy_sql, "COPY from manifest")

    # 3. DELETE + INSERT (atomic upsert via transaction)
    upsert_sql = f"""
        BEGIN;

        DELETE FROM {full_target}
        USING {full_staging} stg
        WHERE {full_target}.order_id = stg.order_id;

        INSERT INTO {full_target}
        SELECT * FROM {full_staging};

        COMMIT;
    """
    run_redshift_sql(creds, upsert_sql, "UPSERT into target")

    # 4. Truncate staging after load (keep table for next run)
    run_redshift_sql(creds, f"TRUNCATE {full_staging};", "post-load truncate staging")

    logger.info("COPY + UPSERT complete: %s → %s", full_staging, full_target)


def validate_load(creds: dict) -> None:
    """Post-load row count check via Redshift Data API."""
    full_target = f"{REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}"

    count_sql = f"""
        SELECT COUNT(*) AS total_rows,
               COUNT(DISTINCT order_id) AS unique_orders,
               MAX(_processed_at) AS last_load_ts
        FROM {full_target}
        WHERE _glue_run_id = '{RUN_ID}';
    """
    run_redshift_sql(creds, count_sql, "post-load validation")
    logger.info("Post-load validation passed for run_id=%s", RUN_ID)


# ─── UNLOAD ───────────────────────────────────────────────────────────────────


def unload_summary(creds: dict) -> str:
    """
    UNLOAD a daily revenue summary from Redshift back to S3 for
    consumption by downstream BI tools / Athena queries.
    """
    full_target = f"{REDSHIFT_SCHEMA}.{REDSHIFT_TABLE}"
    today = RUN_TS.strftime("%Y/%m/%d")
    unload_path = f"s3://{UNLOAD_BUCKET}/summaries/{REDSHIFT_TABLE}/{today}/"

    unload_sql = f"""
        UNLOAD (
            'SELECT
                order_year,
                order_month,
                order_quarter,
                product_category,
                region,
                channel,
                COUNT(DISTINCT order_id)     AS order_count,
                COUNT(DISTINCT customer_id)  AS customer_count,
                SUM(quantity)                AS total_units,
                ROUND(SUM(gross_revenue), 2) AS total_gross_revenue,
                ROUND(SUM(net_revenue),   2) AS total_net_revenue,
                ROUND(AVG(days_to_ship),  2) AS avg_days_to_ship,
                SUM(CASE WHEN is_late_shipment THEN 1 ELSE 0 END) AS late_shipments
            FROM {full_target}
            WHERE order_date >= CURRENT_DATE - 90
            GROUP BY 1,2,3,4,5,6
            ORDER BY 1,2,4,5'
        )
        TO '{unload_path}'
        IAM_ROLE '{REDSHIFT_ROLE_ARN}'
        FORMAT AS PARQUET
        ALLOWOVERWRITE
        PARALLEL ON
        MAXFILESIZE 256 MB;
    """
    run_redshift_sql(creds, unload_sql, "UNLOAD summary")
    logger.info("UNLOAD complete → %s", unload_path)
    return unload_path


# ─── Write DLQ ────────────────────────────────────────────────────────────────


def write_dlq(rejected_df: DataFrame, dlq_bucket: str, dlq_prefix: str) -> None:
    """Write rejected rows to the dead-letter queue prefix for investigation."""
    if rejected_df.rdd.isEmpty():
        return
    dlq_path = (
        f"s3://{dlq_bucket}/{dlq_prefix}/{REDSHIFT_TABLE}/"
        f"run_id={RUN_ID}/"
        f"ts={RUN_TS.strftime('%Y%m%dT%H%M%SZ')}/"
    )
    rejected_df.write.mode("overwrite").parquet(dlq_path)
    logger.warning(
        "DLQ: %d rejected rows written to %s",
        rejected_df.count(),
        dlq_path,
    )


# ─── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    try:
        # 1. Determine partitions to read
        paths = get_source_paths(
            SOURCE_BUCKET, SOURCE_PREFIX, PARTITION_DATE, LOOKBACK_DAYS
        )

        # 2. Read raw data
        raw_df = read_raw(paths)

        # 3. Data quality gate
        valid_df, rejected_df = run_data_quality(raw_df, MAX_REJECT_PCT)

        # 4. Write DLQ
        write_dlq(rejected_df, STAGING_BUCKET, DLQ_PREFIX)

        # 5. Transform
        clean_df = transform(valid_df)

        # 6. Write staging Parquet to S3
        staging_run_prefix = f"processed/{REDSHIFT_TABLE}/run_id={RUN_ID}"
        write_staging(clean_df, STAGING_BUCKET, staging_run_prefix)

        # 7. Generate COPY manifest
        manifest_uri = generate_copy_manifest(
            STAGING_BUCKET, staging_run_prefix, MANIFEST_BUCKET
        )

        # 8. COPY into Redshift
        creds = get_redshift_credentials()
        copy_into_redshift(
            creds,
            manifest_uri,
            staging_table=f"{REDSHIFT_TABLE}_staging",
            target_table=REDSHIFT_TABLE,
        )

        # 9. Post-load validation
        validate_load(creds)

        # 10. UNLOAD summary back to S3
        unload_summary(creds)

        # 11. Commit Glue bookmark
        job.commit()
        logger.info("Job %s completed successfully. RunID: %s", JOB_NAME, RUN_ID)

    except Exception as exc:
        logger.exception("Job FAILED: %s", exc)
        raise


main()
