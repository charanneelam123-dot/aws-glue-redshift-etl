"""
trigger_glue.py
AWS Lambda — S3 Event Trigger for Glue ETL Job

Triggered by: S3 PutObject / S3:ObjectCreated events via EventBridge or
              S3 Event Notification → SQS → Lambda.

Responsibilities:
  - Parse S3 event(s) to extract bucket, key, and partition date
  - Deduplicate trigger events (idempotency via DynamoDB lock table)
  - Validate file format and prefix match before invoking Glue
  - Start the Glue job with correct arguments
  - Publish metrics to CloudWatch for observability
  - Send failure notifications to SNS

Environment variables (set via Terraform):
  GLUE_JOB_NAME       Name of the Glue ETL job
  SOURCE_BUCKET       Expected source bucket (validation)
  STAGING_BUCKET      S3 staging bucket
  MANIFEST_BUCKET     S3 manifest bucket
  UNLOAD_BUCKET       S3 unload bucket
  REDSHIFT_DB         Redshift database name
  REDSHIFT_SCHEMA     Redshift schema
  REDSHIFT_TABLE      Redshift target table
  REDSHIFT_SECRET_ARN Secrets Manager ARN
  REDSHIFT_ROLE_ARN   IAM role ARN for Redshift COPY
  LOCK_TABLE_NAME     DynamoDB table for idempotency locks
  SNS_ALERT_TOPIC_ARN SNS topic for failure alerts
  ENVIRONMENT         dev | staging | prod
  VALID_PREFIXES      Comma-separated list of valid S3 key prefixes
  MAX_CONCURRENT_RUNS Maximum concurrent Glue job runs (default 3)
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

# ─── Logging ──────────────────────────────────────────────────────────────────

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ─── Environment ──────────────────────────────────────────────────────────────

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
SOURCE_BUCKET = os.environ["SOURCE_BUCKET"]
STAGING_BUCKET = os.environ["STAGING_BUCKET"]
MANIFEST_BUCKET = os.environ["MANIFEST_BUCKET"]
UNLOAD_BUCKET = os.environ["UNLOAD_BUCKET"]
REDSHIFT_DB = os.environ["REDSHIFT_DB"]
REDSHIFT_SCHEMA = os.environ["REDSHIFT_SCHEMA"]
REDSHIFT_TABLE = os.environ["REDSHIFT_TABLE"]
SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_ROLE_ARN = os.environ["REDSHIFT_ROLE_ARN"]
LOCK_TABLE_NAME = os.environ["LOCK_TABLE_NAME"]
SNS_TOPIC_ARN = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")
VALID_PREFIXES = os.environ.get("VALID_PREFIXES", "orders/").split(",")
MAX_CONCURRENT_RUNS = int(os.environ.get("MAX_CONCURRENT_RUNS", "3"))

# ─── AWS Clients ──────────────────────────────────────────────────────────────

glue_client = boto3.client("glue")
dynamodb = boto3.resource("dynamodb")
sns_client = boto3.client("sns")
cw_client = boto3.client("cloudwatch")
lock_table = dynamodb.Table(LOCK_TABLE_NAME)


# ─── Idempotency Lock ─────────────────────────────────────────────────────────


def acquire_lock(s3_key: str) -> bool:
    """
    Try to acquire an idempotency lock in DynamoDB.
    Returns True if lock acquired (first-time trigger for this key).
    Returns False if already processed (duplicate event).
    Lock TTL = 24 hours.
    """
    ttl = int(time.time()) + 86400  # 24-hour TTL
    try:
        lock_table.put_item(
            Item={
                "pk": f"glue_trigger#{s3_key}",
                "s3_key": s3_key,
                "job_name": GLUE_JOB_NAME,
                "locked_at": datetime.now(timezone.utc).isoformat(),
                "ttl": ttl,
            },
            ConditionExpression="attribute_not_exists(pk)",
        )
        logger.info("Lock acquired for: %s", s3_key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.warning("Duplicate event — lock already exists for: %s", s3_key)
            return False
        raise


def release_lock(s3_key: str) -> None:
    """Release the idempotency lock (on failure, to allow retry)."""
    try:
        lock_table.delete_item(Key={"pk": f"glue_trigger#{s3_key}"})
        logger.info("Lock released for: %s", s3_key)
    except Exception as exc:
        logger.warning("Failed to release lock for %s: %s", s3_key, exc)


# ─── S3 Event Parsing ─────────────────────────────────────────────────────────


def parse_s3_events(event: dict) -> list[dict[str, str]]:
    """
    Parse S3 event records from:
      - Direct S3 event notification
      - SQS-wrapped S3 events
      - EventBridge S3 events

    Returns list of {bucket, key, partition_date} dicts.
    """
    records = []
    raw_records = event.get("Records", [event])  # handle both direct and SQS-wrapped

    for record in raw_records:
        # SQS wrapping: body contains the real S3 event JSON
        if "body" in record:
            inner = json.loads(record["body"])
            inner_records = inner.get("Records", [])
        # EventBridge wrapping
        elif record.get("source") == "aws.s3":
            inner_records = [record]
        else:
            inner_records = [record]

        for inner in inner_records:
            # Skip test events
            if inner.get("Event") == "s3:TestEvent":
                continue

            s3_info = inner.get("s3", {})
            bucket = s3_info.get("bucket", {}).get("name", "")
            key = urllib.parse.unquote_plus(s3_info.get("object", {}).get("key", ""))

            if not bucket or not key:
                # EventBridge format
                detail = inner.get("detail", {})
                bucket = detail.get("bucket", {}).get("name", "")
                key = detail.get("object", {}).get("key", "")

            if bucket and key:
                partition_date = extract_partition_date(key)
                records.append(
                    {
                        "bucket": bucket,
                        "key": key,
                        "partition_date": partition_date,
                    }
                )

    logger.info("Parsed %d S3 event records.", len(records))
    return records


def extract_partition_date(s3_key: str) -> str:
    """
    Extract the partition date from an S3 key.
    Supports layouts:
      - year=YYYY/month=MM/day=DD/
      - YYYY/MM/DD/
      - dt=YYYY-MM-DD/
    Returns today's date as fallback.
    """
    import re

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Hive-style partitioning: year=2024/month=01/day=15
    m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", s3_key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # dt=YYYY-MM-DD
    m = re.search(r"dt=(\d{4}-\d{2}-\d{2})", s3_key)
    if m:
        return m.group(1)

    # Plain YYYY/MM/DD
    m = re.search(r"(\d{4})/(\d{2})/(\d{2})", s3_key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    logger.warning(
        "Could not extract partition date from key: %s. Using today.", s3_key
    )
    return today


# ─── Validation ───────────────────────────────────────────────────────────────


def validate_event(bucket: str, key: str) -> tuple[bool, str]:
    """
    Validate that the S3 event matches expectations:
    - Correct source bucket
    - Key starts with a valid prefix
    - File extension is .parquet, .csv, .json, or .gz
    """
    if bucket != SOURCE_BUCKET:
        return False, f"Unexpected bucket: {bucket} (expected {SOURCE_BUCKET})"

    if not any(key.startswith(p.strip()) for p in VALID_PREFIXES):
        return False, f"Key '{key}' does not match valid prefixes: {VALID_PREFIXES}"

    valid_extensions = (".parquet", ".csv", ".json", ".gz", ".snappy.parquet")
    if not any(key.endswith(ext) for ext in valid_extensions):
        return False, f"Unsupported file extension for key: {key}"

    return True, "OK"


# ─── Glue Concurrency Check ───────────────────────────────────────────────────


def check_concurrent_runs() -> int:
    """
    Count currently RUNNING Glue job runs.
    Returns the count. Raises if at or above MAX_CONCURRENT_RUNS.
    """
    resp = glue_client.get_job_runs(
        JobName=GLUE_JOB_NAME,
        MaxResults=50,
    )
    running = [
        r
        for r in resp.get("JobRuns", [])
        if r["JobRunState"] in ("STARTING", "RUNNING", "STOPPING")
    ]
    return len(running)


# ─── Start Glue Job ───────────────────────────────────────────────────────────


def start_glue_job(partition_date: str, trigger_key: str) -> str:
    """
    Start the Glue ETL job with all required arguments.
    Returns the JobRunId.
    """
    resp = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--source_bucket": SOURCE_BUCKET,
            "--source_prefix": "orders",
            "--staging_bucket": STAGING_BUCKET,
            "--manifest_bucket": MANIFEST_BUCKET,
            "--unload_bucket": UNLOAD_BUCKET,
            "--redshift_db": REDSHIFT_DB,
            "--redshift_schema": REDSHIFT_SCHEMA,
            "--redshift_table": REDSHIFT_TABLE,
            "--redshift_secret_arn": SECRET_ARN,
            "--redshift_role_arn": REDSHIFT_ROLE_ARN,
            "--partition_date": partition_date,
            "--lookback_days": "1",
            "--environment": ENVIRONMENT,
            "--trigger_s3_key": trigger_key,
        },
        Timeout=120,  # minutes
        MaxCapacity=10,  # DPUs (overridden by job config in Terraform)
    )
    job_run_id = resp["JobRunId"]
    logger.info(
        "Started Glue job '%s' | RunId: %s | partition_date: %s",
        GLUE_JOB_NAME,
        job_run_id,
        partition_date,
    )
    return job_run_id


# ─── CloudWatch Metrics ───────────────────────────────────────────────────────


def emit_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    """Emit a custom CloudWatch metric."""
    try:
        cw_client.put_metric_data(
            Namespace=f"GlueETL/{ENVIRONMENT}",
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [
                        {"Name": "JobName", "Value": GLUE_JOB_NAME},
                        {"Name": "Environment", "Value": ENVIRONMENT},
                    ],
                    "Value": value,
                    "Unit": unit,
                    "Timestamp": datetime.now(timezone.utc),
                }
            ],
        )
    except Exception as exc:
        logger.warning("CloudWatch metric emission failed: %s", exc)


# ─── SNS Alert ────────────────────────────────────────────────────────────────


def send_alert(subject: str, message: str) -> None:
    """Send an SNS alert on failure."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_ALERT_TOPIC_ARN not set — skipping alert.")
        return
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[{ENVIRONMENT.upper()}] Glue ETL Alert: {subject}",
            Message=message,
        )
    except Exception as exc:
        logger.error("SNS publish failed: %s", exc)


# ─── Handler ──────────────────────────────────────────────────────────────────


def handler(event: dict, context: Any) -> dict:
    """Lambda entrypoint."""
    logger.info("Event received: %s", json.dumps(event, default=str))

    s3_events = parse_s3_events(event)
    if not s3_events:
        logger.info("No actionable S3 events. Exiting.")
        return {"statusCode": 200, "body": "No S3 events to process."}

    results = []

    for s3_event in s3_events:
        bucket = s3_event["bucket"]
        key = s3_event["key"]
        partition_date = s3_event["partition_date"]

        # ── Validate ──────────────────────────────────────────────────────────
        valid, reason = validate_event(bucket, key)
        if not valid:
            logger.warning("Skipping invalid event [%s/%s]: %s", bucket, key, reason)
            emit_metric("SkippedEvents", 1)
            results.append({"key": key, "status": "skipped", "reason": reason})
            continue

        # ── Idempotency lock ──────────────────────────────────────────────────
        if not acquire_lock(key):
            emit_metric("DuplicateEvents", 1)
            results.append({"key": key, "status": "duplicate"})
            continue

        # ── Concurrency check ─────────────────────────────────────────────────
        try:
            running_count = check_concurrent_runs()
            if running_count >= MAX_CONCURRENT_RUNS:
                msg = (
                    f"Max concurrent runs ({MAX_CONCURRENT_RUNS}) reached. "
                    f"Releasing lock — event will be retried from SQS."
                )
                logger.warning(msg)
                release_lock(key)
                emit_metric("ThrottledTriggers", 1)
                results.append({"key": key, "status": "throttled"})
                continue
        except Exception as exc:
            logger.error("Concurrency check failed: %s", exc)
            release_lock(key)
            raise

        # ── Start Glue job ────────────────────────────────────────────────────
        try:
            job_run_id = start_glue_job(partition_date, key)
            emit_metric("JobsTriggered", 1)
            results.append(
                {
                    "key": key,
                    "status": "triggered",
                    "job_run_id": job_run_id,
                    "partition_date": partition_date,
                }
            )
        except Exception as exc:
            logger.exception("Failed to start Glue job for key %s: %s", key, exc)
            release_lock(key)
            emit_metric("TriggerFailures", 1)
            send_alert(
                subject=f"Glue trigger failed for {key}",
                message=(
                    f"Failed to start Glue job '{GLUE_JOB_NAME}'\n"
                    f"S3 key: s3://{bucket}/{key}\n"
                    f"Partition date: {partition_date}\n"
                    f"Error: {exc}"
                ),
            )
            results.append({"key": key, "status": "error", "error": str(exc)})

    logger.info("Processing complete. Results: %s", json.dumps(results, default=str))
    return {
        "statusCode": 200,
        "body": json.dumps(results, default=str),
    }
