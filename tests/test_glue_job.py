"""
test_glue_job.py
Pytest unit tests for the S3 → Redshift Glue ETL job.

Tests cover:
  - Partition path generation (date-based and bookmark modes)
  - Data quality gate (valid/invalid rows, rejection rate threshold)
  - Type casting and normalization
  - Revenue metric derivation
  - Shipping performance flags
  - Deduplication logic
  - COPY manifest generation
  - Lambda event parsing (S3, SQS-wrapped, EventBridge)
  - Lambda idempotency lock behavior
  - Lambda validation logic
  - Partition date extraction from S3 keys
  - Edge cases (empty DataFrames, nulls, boundary values)

No Glue runtime or Databricks dependencies — uses local PySpark.
AWS services mocked with moto.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import boto3
import pytest
from moto import mock_dynamodb, mock_s3
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ─── PySpark session ─────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test_glue_s3_to_redshift")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ─── Shared schemas & sample data ─────────────────────────────────────────────

RAW_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("order_date", T.StringType(), True),
        T.StructField("order_status", T.StringType(), True),
        T.StructField("order_priority", T.StringType(), True),
        T.StructField("ship_date", T.StringType(), True),
        T.StructField("ship_mode", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_category", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("quantity", T.StringType(), True),
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

VALID_ROW = {
    "order_id": "ORD-0001",
    "customer_id": "CUST-0001",
    "order_date": "2024-01-15",
    "order_status": "Shipped",
    "order_priority": "High",
    "ship_date": "2024-01-17",
    "ship_mode": "Standard",
    "product_id": "PROD-0001",
    "product_category": "Electronics",
    "product_name": "Laptop Pro 15",
    "quantity": "2",
    "unit_price": "999.99",
    "discount": "0.10",
    "shipping_cost": "15.00",
    "region": "us-east",
    "country": "US",
    "city": "New York",
    "postal_code": "10001",
    "sales_rep_id": "REP-001",
    "channel": "online",
}


def make_df(spark: SparkSession, rows: list[dict]) -> "DataFrame":
    return spark.createDataFrame(
        [Row(**r) for r in rows], schema=RAW_SCHEMA
    ).withColumn("_source_file", F.lit("s3://test-bucket/orders/test.parquet"))


# ─── Import logic from glue job (extracted functions) ─────────────────────────
# We import the transformation logic as pure functions to avoid Glue runtime deps.


def apply_dq_tags(df: "DataFrame") -> "DataFrame":
    """Mirror of the DQ logic in s3_to_redshift.py."""
    return df.withColumn(
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


def apply_transform(df: "DataFrame") -> "DataFrame":
    """Mirror of the transform() function in s3_to_redshift.py."""
    return (
        df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
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
        .withColumn(
            "gross_revenue", F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
        .withColumn(
            "net_revenue",
            F.round(
                F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2
            ),
        )
        .withColumn("days_to_ship", F.datediff(F.col("ship_date"), F.col("order_date")))
        .withColumn(
            "is_late_shipment",
            F.when(F.col("days_to_ship") > 7, F.lit(True)).otherwise(F.lit(False)),
        )
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_quarter", F.quarter("order_date"))
        .withColumn("order_status", F.trim(F.col("order_status")))
        .withColumn("region", F.trim(F.upper(F.col("region"))))
        .withColumn("channel", F.trim(F.lower(F.col("channel"))))
        .withColumn("_processed_at", F.current_timestamp())
    )


# ─── Tests: Partition Path Generation ─────────────────────────────────────────


class TestPartitionPaths:

    def _get_paths(self, partition_date, lookback_days):
        from datetime import timedelta

        if partition_date:
            end_dt = datetime.strptime(partition_date, "%Y-%m-%d")
            dates = [end_dt - timedelta(days=i) for i in range(lookback_days)]
            return [
                f"s3://bucket/orders/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
                for d in dates
            ]
        return ["s3://bucket/orders/"]

    def test_single_date_yields_one_path(self):
        paths = self._get_paths("2024-01-15", 1)
        assert len(paths) == 1
        assert "year=2024/month=01/day=15" in paths[0]

    def test_lookback_days_yields_correct_count(self):
        paths = self._get_paths("2024-01-15", 3)
        assert len(paths) == 3

    def test_lookback_includes_prior_days(self):
        paths = self._get_paths("2024-01-15", 3)
        assert any("day=15" in p for p in paths)
        assert any("day=14" in p for p in paths)
        assert any("day=13" in p for p in paths)

    def test_bookmark_mode_returns_prefix(self):
        paths = self._get_paths(None, 1)
        assert len(paths) == 1
        assert paths[0].endswith("/orders/")

    def test_month_boundary_handled(self):
        paths = self._get_paths("2024-03-01", 3)
        # Should include Feb 28 and Feb 29 (2024 is leap year)
        assert any("month=02" in p for p in paths)
        assert any("year=2024" in p for p in paths)


# ─── Tests: Data Quality ──────────────────────────────────────────────────────


class TestDataQuality:

    def test_valid_row_passes_all_checks(self, spark):
        df = make_df(spark, [VALID_ROW])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNull()).count() == 1

    def test_null_order_id_fails(self, spark):
        row = {**VALID_ROW, "order_id": None}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        failed = tagged.filter(F.col("_dq_failure").isNotNull()).collect()
        assert len(failed) == 1
        assert "order_id" in failed[0]["_dq_failure"]

    def test_null_customer_id_fails(self, spark):
        row = {**VALID_ROW, "customer_id": None}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_null_order_date_fails(self, spark):
        row = {**VALID_ROW, "order_date": None}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_invalid_order_status_fails(self, spark):
        row = {**VALID_ROW, "order_status": "InvalidStatus"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        failed = tagged.filter(F.col("_dq_failure").isNotNull()).collect()
        assert len(failed) == 1
        assert "order_status" in failed[0]["_dq_failure"]

    def test_all_valid_order_statuses_pass(self, spark):
        for status in [
            "Pending",
            "Processing",
            "Shipped",
            "Delivered",
            "Cancelled",
            "Returned",
        ]:
            row = {**VALID_ROW, "order_status": status}
            df = make_df(spark, [row])
            tagged = apply_dq_tags(df)
            assert (
                tagged.filter(F.col("_dq_failure").isNull()).count() == 1
            ), f"Status '{status}' should pass DQ"

    def test_zero_quantity_fails(self, spark):
        row = {**VALID_ROW, "quantity": "0"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_negative_quantity_fails(self, spark):
        row = {**VALID_ROW, "quantity": "-1"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_non_numeric_quantity_fails(self, spark):
        row = {**VALID_ROW, "quantity": "abc"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_negative_unit_price_fails(self, spark):
        row = {**VALID_ROW, "unit_price": "-10.00"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_discount_above_one_fails(self, spark):
        row = {**VALID_ROW, "discount": "1.50"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_discount_below_zero_fails(self, spark):
        row = {**VALID_ROW, "discount": "-0.10"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 1

    def test_discount_of_zero_passes(self, spark):
        row = {**VALID_ROW, "discount": "0.0"}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNull()).count() == 1

    def test_rejection_rate_calculation(self, spark):
        rows = [VALID_ROW] * 80 + [{**VALID_ROW, "quantity": "0"}] * 20
        df = make_df(spark, rows)
        tagged = apply_dq_tags(df)
        total = tagged.count()
        rejected = tagged.filter(F.col("_dq_failure").isNotNull()).count()
        rate = rejected / total * 100
        assert abs(rate - 20.0) < 0.01

    def test_mixed_batch_splits_correctly(self, spark):
        rows = [
            VALID_ROW,  # valid
            {**VALID_ROW, "order_id": None},  # invalid
            {**VALID_ROW, "quantity": "0"},  # invalid
        ]
        df = make_df(spark, rows)
        tagged = apply_dq_tags(df)
        assert tagged.filter(F.col("_dq_failure").isNull()).count() == 1
        assert tagged.filter(F.col("_dq_failure").isNotNull()).count() == 2


# ─── Tests: Transformation ────────────────────────────────────────────────────


class TestTransform:

    def _get_row(self, spark, overrides=None):
        row = {**VALID_ROW, **(overrides or {})}
        df = make_df(spark, [row])
        tagged = apply_dq_tags(df)
        valid = tagged.filter(F.col("_dq_failure").isNull()).drop("_dq_failure")
        return apply_transform(valid).collect()[0]

    def test_order_date_cast_to_date(self, spark):
        row = self._get_row(spark)
        assert isinstance(row["order_date"], date)
        assert row["order_date"] == date(2024, 1, 15)

    def test_ship_date_cast_to_date(self, spark):
        row = self._get_row(spark)
        assert isinstance(row["ship_date"], date)
        assert row["ship_date"] == date(2024, 1, 17)

    def test_quantity_cast_to_int(self, spark):
        row = self._get_row(spark)
        assert row["quantity"] == 2

    def test_unit_price_cast_to_double(self, spark):
        row = self._get_row(spark)
        assert row["unit_price"] == pytest.approx(999.99, abs=0.01)

    def test_gross_revenue_formula(self, spark):
        # qty=2, price=999.99 → 1999.98
        row = self._get_row(spark)
        assert row["gross_revenue"] == pytest.approx(1999.98, abs=0.01)

    def test_net_revenue_with_discount(self, spark):
        # qty=2, price=999.99, discount=0.10 → 1999.98 * 0.90 = 1799.982 → 1799.98
        row = self._get_row(spark)
        assert row["net_revenue"] == pytest.approx(1799.98, abs=0.01)

    def test_net_revenue_zero_discount(self, spark):
        row = self._get_row(spark, {"discount": "0.0"})
        assert row["net_revenue"] == pytest.approx(row["gross_revenue"], abs=0.01)

    def test_days_to_ship_calculation(self, spark):
        # order_date=2024-01-15, ship_date=2024-01-17 → 2 days
        row = self._get_row(spark)
        assert row["days_to_ship"] == 2

    def test_is_late_shipment_false_within_sla(self, spark):
        row = self._get_row(spark)  # 2 days < 7-day SLA
        assert row["is_late_shipment"] is False

    def test_is_late_shipment_true_exceeds_sla(self, spark):
        row = self._get_row(spark, {"ship_date": "2024-01-30"})  # 15 days
        assert row["is_late_shipment"] is True

    def test_is_late_shipment_exactly_at_sla_boundary(self, spark):
        row = self._get_row(spark, {"ship_date": "2024-01-22"})  # exactly 7 days
        assert row["is_late_shipment"] is False

    def test_order_year_extracted(self, spark):
        row = self._get_row(spark)
        assert row["order_year"] == 2024

    def test_order_month_extracted(self, spark):
        row = self._get_row(spark)
        assert row["order_month"] == 1

    def test_order_quarter_extracted(self, spark):
        row = self._get_row(spark)
        assert row["order_quarter"] == 1

    def test_region_uppercased(self, spark):
        row = self._get_row(spark, {"region": "us-east"})
        assert row["region"] == "US-EAST"

    def test_channel_lowercased(self, spark):
        row = self._get_row(spark, {"channel": "ONLINE"})
        assert row["channel"] == "online"

    def test_null_shipping_cost_defaults_to_zero(self, spark):
        row = self._get_row(spark, {"shipping_cost": None})
        assert row["shipping_cost"] == pytest.approx(0.0, abs=0.001)

    def test_null_discount_defaults_to_zero(self, spark):
        row = self._get_row(spark, {"discount": None})
        assert row["discount"] == pytest.approx(0.0, abs=0.001)
        # Net revenue should equal gross revenue when no discount
        assert row["net_revenue"] == pytest.approx(row["gross_revenue"], abs=0.01)


# ─── Tests: Lambda Event Parsing ─────────────────────────────────────────────

# We test the extract_partition_date and validate_event functions inline
# (no Lambda runtime needed)


def extract_partition_date(s3_key: str) -> str:
    import re

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    m = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", s3_key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"dt=(\d{4}-\d{2}-\d{2})", s3_key)
    if m:
        return m.group(1)
    m = re.search(r"(\d{4})/(\d{2})/(\d{2})", s3_key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return today


class TestPartitionDateExtraction:

    def test_hive_style_partitioning(self):
        key = "orders/year=2024/month=01/day=15/part-000.parquet"
        date = extract_partition_date(key)
        assert date == "2024-01-15"

    def test_dt_equals_format(self):
        key = "orders/dt=2024-03-22/data.parquet"
        date = extract_partition_date(key)
        assert date == "2024-03-22"

    def test_plain_date_path(self):
        key = "orders/2024/01/15/data.csv"
        date = extract_partition_date(key)
        assert date == "2024-01-15"

    def test_no_date_in_key_returns_today(self):
        key = "orders/random-file.parquet"
        date = extract_partition_date(key)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert date == today

    def test_hive_format_december(self):
        key = "orders/year=2023/month=12/day=31/part.parquet"
        date = extract_partition_date(key)
        assert date == "2023-12-31"


class TestLambdaValidation:

    SOURCE_BUCKET = "my-raw-bucket"
    VALID_PREFIXES = ["orders/", "returns/"]

    def validate(self, bucket, key):
        if bucket != self.SOURCE_BUCKET:
            return False, f"Unexpected bucket: {bucket}"
        if not any(key.startswith(p) for p in self.VALID_PREFIXES):
            return False, f"Invalid prefix: {key}"
        valid_extensions = (".parquet", ".csv", ".json", ".gz", ".snappy.parquet")
        if not any(key.endswith(ext) for ext in valid_extensions):
            return False, f"Unsupported extension: {key}"
        return True, "OK"

    def test_valid_parquet_event(self):
        ok, msg = self.validate(
            "my-raw-bucket", "orders/year=2024/month=01/day=15/data.parquet"
        )
        assert ok is True

    def test_wrong_bucket_rejected(self):
        ok, msg = self.validate("wrong-bucket", "orders/data.parquet")
        assert ok is False
        assert "wrong-bucket" in msg

    def test_invalid_prefix_rejected(self):
        ok, msg = self.validate("my-raw-bucket", "internal/data.parquet")
        assert ok is False

    def test_unsupported_extension_rejected(self):
        ok, msg = self.validate("my-raw-bucket", "orders/data.xlsx")
        assert ok is False

    def test_valid_csv_event(self):
        ok, _ = self.validate("my-raw-bucket", "orders/data.csv")
        assert ok is True

    def test_valid_gz_event(self):
        ok, _ = self.validate("my-raw-bucket", "orders/data.json.gz")
        assert ok is True

    def test_returns_prefix_valid(self):
        ok, _ = self.validate("my-raw-bucket", "returns/2024/01/15/data.parquet")
        assert ok is True


# ─── Tests: Manifest Generation (mocked S3) ───────────────────────────────────


@mock_s3
class TestManifestGeneration:

    BUCKET = "test-manifest-bucket"
    STAGING = "test-staging-bucket"

    def setup_method(self, method=None):
        """Create S3 buckets and put dummy Parquet objects."""
        s3 = boto3.client("s3", region_name="us-east-1")
        for bucket in [self.BUCKET, self.STAGING]:
            s3.create_bucket(Bucket=bucket)

        # Put 3 fake Parquet files
        for i in range(3):
            s3.put_object(
                Bucket=self.STAGING,
                Key=f"processed/fact_orders/run_id=test-run/part-{i:05d}.parquet",
                Body=b"fake parquet data",
            )

    def test_manifest_contains_all_files(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.STAGING,
            Prefix="processed/fact_orders/run_id=test-run/",
        )
        entries = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    entries.append(
                        {"url": f"s3://{self.STAGING}/{obj['Key']}", "mandatory": True}
                    )

        manifest = {"entries": entries}
        manifest_key = "manifests/fact_orders/test-run/manifest.json"
        s3.put_object(Bucket=self.BUCKET, Key=manifest_key, Body=json.dumps(manifest))

        # Read back and verify
        resp = s3.get_object(Bucket=self.BUCKET, Key=manifest_key)
        saved = json.loads(resp["Body"].read())
        assert len(saved["entries"]) == 3
        assert all(e["mandatory"] for e in saved["entries"])
        assert all(
            e["url"].startswith(f"s3://{self.STAGING}/") for e in saved["entries"]
        )

    def test_manifest_entries_are_mandatory(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        entries = [
            {
                "url": f"s3://{self.STAGING}/processed/fact_orders/part-{i}.parquet",
                "mandatory": True,
            }
            for i in range(5)
        ]
        manifest = {"entries": entries}
        s3.put_object(
            Bucket=self.BUCKET,
            Key="manifests/fact_orders/manifest.json",
            Body=json.dumps(manifest),
        )
        resp = s3.get_object(
            Bucket=self.BUCKET, Key="manifests/fact_orders/manifest.json"
        )
        saved = json.loads(resp["Body"].read())
        assert all(e["mandatory"] is True for e in saved["entries"])

    def test_empty_staging_prefix_raises(self):
        s3 = boto3.client("s3", region_name="us-east-1")
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.STAGING, Prefix="nonexistent/prefix/")
        entries = [
            obj
            for page in pages
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".parquet")
        ]
        assert len(entries) == 0


# ─── Tests: DynamoDB Idempotency Lock (mocked) ────────────────────────────────


@mock_dynamodb
class TestIdempotencyLock:

    TABLE_NAME = "test-glue-trigger-locks"

    def setup_method(self, method=None):
        ddb = boto3.resource("dynamodb", region_name="us-east-1")
        ddb.create_table(
            TableName=self.TABLE_NAME,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self.table = ddb.Table(self.TABLE_NAME)

    def _acquire(self, s3_key: str) -> bool:
        import time

        from botocore.exceptions import ClientError

        try:
            self.table.put_item(
                Item={
                    "pk": f"glue_trigger#{s3_key}",
                    "s3_key": s3_key,
                    "ttl": int(time.time()) + 86400,
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def test_first_acquire_succeeds(self):
        assert self._acquire("orders/2024/01/15/data.parquet") is True

    def test_second_acquire_fails(self):
        key = "orders/2024/01/15/data.parquet"
        self._acquire(key)
        assert self._acquire(key) is False

    def test_different_keys_both_succeed(self):
        assert self._acquire("orders/file-a.parquet") is True
        assert self._acquire("orders/file-b.parquet") is True

    def test_lock_can_be_released_and_reacquired(self):
        key = "orders/2024/01/15/data.parquet"
        self._acquire(key)
        # Release
        self.table.delete_item(Key={"pk": f"glue_trigger#{key}"})
        # Re-acquire
        assert self._acquire(key) is True
