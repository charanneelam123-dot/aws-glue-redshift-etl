# aws-glue-redshift-etl

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS_Glue-4.0-FF9900?logo=amazonaws&logoColor=white)
![Redshift](https://img.shields.io/badge/Amazon_Redshift-RA3-8C4FFF?logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-1.6-7B42BC?logo=terraform&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
![CI](https://img.shields.io/github/actions/workflow/status/your-org/aws-glue-redshift-etl/ci.yml?label=CI)

Production-grade **AWS Glue + Amazon Redshift ETL pipeline** with Terraform-provisioned infrastructure, Lambda-based event triggering, least-privilege IAM, and COPY/UNLOAD patterns.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION                                                              │
│  External systems / upstream pipelines drop Parquet / CSV files into        │
│  S3 Raw bucket under a Hive-partitioned prefix:                             │
│                                                                              │
│  s3://glue-redshift-etl-raw-<id>/orders/                                    │
│      year=YYYY/month=MM/day=DD/<files>.parquet                              │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │  S3:ObjectCreated → SQS
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  EVENT TRIGGER LAYER                                                         │
│                                                                              │
│  S3 Event Notification                                                       │
│      → SQS Queue (glue-trigger)    ← dead-letter queue on failure           │
│      → Lambda (trigger_glue.py)                                              │
│          ├── Parse S3 event (S3 native / SQS-wrapped / EventBridge)          │
│          ├── Validate: bucket, prefix, extension                             │
│          ├── Acquire DynamoDB idempotency lock (TTL 24h)                    │
│          ├── Check Glue concurrent runs < MAX_CONCURRENT_RUNS               │
│          ├── start_job_run() with partition_date + all job args             │
│          ├── Emit CloudWatch metrics (triggered / duplicate / throttled)    │
│          └── SNS alert on failure                                           │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │  Glue.start_job_run()
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  AWS GLUE ETL JOB  (s3_to_redshift.py — PySpark, Glue 4.0, G.1X × 10)     │
│                                                                              │
│  1. Partition Pruning                                                        │
│     get_source_paths() builds s3://raw/orders/year=Y/month=M/day=D/ paths  │
│     for [partition_date - lookback_days, partition_date]                    │
│     Falls back to Glue job bookmarks for incremental mode                   │
│                                                                              │
│  2. Read (Auto-detects Parquet or CSV)                                       │
│     spark.readStream with cloudFiles / spark.read with schema enforcement   │
│     + input_file_name() for _source_file audit column                      │
│                                                                              │
│  3. Data Quality Gate                                                        │
│     Tag each row with first failing rule:                                   │
│     - null order_id / customer_id / order_date                              │
│     - invalid order_status (not in enum)                                    │
│     - quantity ≤ 0 or non-numeric                                           │
│     - unit_price < 0                                                        │
│     - discount outside [0, 1]                                               │
│     Abort if rejection rate > MAX_REJECT_PCT (default 5%)                  │
│                                                                              │
│  4. Write rejected rows → s3://staging/dlq/<table>/run_id=<id>/             │
│                                                                              │
│  5. Transform                                                                │
│     - Cast types (string → date/int/double)                                 │
│     - gross_revenue = qty × unit_price                                      │
│     - net_revenue   = qty × unit_price × (1 - discount)                    │
│     - days_to_ship  = datediff(ship_date, order_date)                      │
│     - is_late_shipment = days_to_ship > 7                                  │
│     - Standardise region (UPPER), channel (lower)                          │
│     - Dedup by (vendor+pickup+pu_loc+do_loc) via ROW_NUMBER window         │
│     - Append ETL audit columns (_glue_run_id, _processed_at, etc.)         │
│                                                                              │
│  6. Write Parquet → s3://staging/processed/<table>/run_id=<id>/             │
│     Partitioned by (order_year, order_month) for COPY efficiency            │
│                                                                              │
│  7. Generate COPY Manifest                                                   │
│     List staging objects → build manifest JSON → s3://manifest/...         │
│     Each entry marked mandatory=true for COPY integrity validation          │
│                                                                              │
│  8. COPY into Redshift (via Redshift Data API — no JDBC driver)             │
│     TRUNCATE staging → COPY from manifest → DELETE+INSERT (upsert)         │
│     FORMAT AS PARQUET, MANIFEST, TRUNCATECOLUMNS, STATUPDATE ON            │
│                                                                              │
│  9. Post-load validation: row count + distinct orders check                 │
│                                                                              │
│  10. UNLOAD 90-day summary → s3://unload/summaries/<table>/YYYY/MM/DD/      │
│      FORMAT AS PARQUET, PARALLEL ON, MAXFILESIZE 256 MB                    │
│                                                                              │
│  11. Commit Glue job bookmark                                               │
└────────────────────────────────┬─────────────────────────────────────────────┘
                                 │  Redshift Data API (COPY + UNLOAD)
                                 ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  AMAZON REDSHIFT  (RA3.xlplus × 2, encrypted, Enhanced VPC Routing)        │
│                                                                              │
│  orders.fact_orders          DISTSTYLE EVEN                                 │
│  orders.fact_orders_staging  DISTSTYLE EVEN  ← COPY target                 │
│  orders.dim_customer         DISTKEY(customer_id)  SCD Type 2              │
│  orders.dim_product          DISTKEY(product_id)                           │
│  orders.dim_date             DISTSTYLE ALL                                  │
│  orders.etl_audit_log        DISTSTYLE ALL                                  │
│  orders.v_monthly_revenue    VIEW (pre-aggregated for BI)                   │
│  orders.v_current_customers  VIEW (is_current = TRUE filter)                │
│                                                                              │
│  COPY strategy: S3 Parquet manifest → staging → DELETE+INSERT upsert       │
│  UNLOAD: 90-day summary aggregates → S3 Parquet (Athena / QuickSight)       │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  INFRASTRUCTURE (Terraform)                                                  │
│                                                                              │
│  S3 Buckets (4)       — versioned, KMS-encrypted, lifecycle rules           │
│  KMS CMK              — single key for all services, auto-rotate             │
│  IAM Roles (3)        — Glue, Redshift COPY, Lambda (least privilege)       │
│  Glue Job             — G.1X × 10, Glue 4.0, bookmark enabled              │
│  Lambda               — Python 3.11, X-Ray tracing, reserved concurrency    │
│  SQS Queue + DLQ      — KMS encrypted, visibility timeout, redrive policy   │
│  DynamoDB             — PAY_PER_REQUEST, TTL, PITR, KMS encrypted           │
│  SNS Topic            — KMS encrypted, email subscriptions                  │
│  Redshift Cluster     — encrypted, audit logging, enhanced VPC routing      │
│  Secrets Manager      — Redshift credentials, KMS encrypted                 │
│  CloudWatch Alarms    — Glue failures, Lambda errors, SQS DLQ messages      │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  CI / CD (GitHub Actions)                                                    │
│                                                                              │
│  lint ──▶ security ──▶ unit-tests ──▶ ci-gate                              │
│       └──▶ sql-validate ──────────────────┘                                 │
│       └──▶ terraform-validate ──▶ checkov ─┘                               │
│                                    └──▶ terraform-plan (label-gated)        │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
aws-glue-redshift-etl/
├── glue_jobs/
│   └── s3_to_redshift.py         PySpark Glue 4.0 ETL job (full pipeline)
├── lambda/
│   └── trigger_glue.py           S3 event → Glue trigger (SQS+DynamoDB+SNS)
├── terraform/
│   ├── main.tf                   All AWS resources (S3,KMS,IAM,Glue,RS,Lambda,SQS,SNS)
│   ├── variables.tf              50+ typed & documented input variables
│   └── outputs.tf                All resource ARNs/names for downstream use
├── sql/
│   └── ddl/
│       └── create_tables.sql     Redshift DDL (DISTKEY/SORTKEY/ENCODE + COPY/UNLOAD)
├── tests/
│   └── test_glue_job.py          50+ pytest tests (local Spark + moto mocks)
├── .github/workflows/
│   └── ci.yml                    7-job CI pipeline
└── requirements.txt
```

---

## IAM Least-Privilege Summary

| Role | Grants |
|---|---|
| **Glue ETL** | S3 R/W on all 4 ETL buckets, KMS decrypt/generate, SecretsManager `GetSecretValue`, Redshift Data API execute/describe, CloudWatch Logs |
| **Redshift COPY** | S3 R/W on staging/manifest/unload buckets only, KMS decrypt/generate |
| **Lambda Trigger** | `glue:StartJobRun/GetJobRuns`, DynamoDB put/get/delete on locks table, SQS receive/delete, SNS publish on alerts topic, CloudWatch PutMetricData, CloudWatch Logs |

---

## Setup

### Prerequisites
- AWS CLI configured (`aws configure`)
- Terraform 1.6+
- Python 3.11+
- Java 11+ (for local PySpark tests)

### 1. Clone & install

```bash
git clone https://github.com/your-org/aws-glue-redshift-etl.git
cd aws-glue-redshift-etl
pip install -r requirements.txt
```

### 2. Run tests locally

```bash
PYSPARK_PYTHON=$(which python) \
SPARK_LOCAL_IP=127.0.0.1 \
AWS_DEFAULT_REGION=us-east-1 \
pytest tests/ -v --cov=glue_jobs --cov=lambda --cov-report=term-missing
```

### 3. Deploy infrastructure

```bash
cd terraform

# Create a tfvars file
cat > terraform.tfvars << EOF
environment       = "dev"
aws_region        = "us-east-1"
aws_account_id    = "123456789012"
vpc_id            = "vpc-xxxxxxxx"
private_subnet_ids = ["subnet-aaa", "subnet-bbb"]
kms_admin_arns    = ["arn:aws:iam::123456789012:role/AdminRole"]
redshift_master_username = "admin"
redshift_master_password = "YourSecureP@ssw0rd"
alert_email_addresses    = ["you@company.com"]
EOF

terraform init
terraform plan -var-file=terraform.tfvars
terraform apply -var-file=terraform.tfvars
```

### 4. Create Redshift tables

```bash
# Connect to Redshift using the cluster endpoint from Terraform outputs
psql -h $(terraform output -raw redshift_endpoint | cut -d: -f1) \
     -p 5439 \
     -U admin \
     -d analytics \
     -f ../sql/ddl/create_tables.sql
```

### 5. Upload Glue script

```bash
# Terraform does this automatically via aws_s3_object resource.
# Manual upload:
aws s3 cp glue_jobs/s3_to_redshift.py \
    s3://$(terraform output -raw s3_glue_assets_bucket)/scripts/s3_to_redshift.py
```

### 6. Trigger a manual Glue run

```bash
aws glue start-job-run \
  --job-name "$(terraform output -raw glue_job_name)" \
  --arguments '{
    "--partition_date":  "2024-01-15",
    "--lookback_days":   "1"
  }'
```

---

## Environment Variables (Lambda)

| Variable | Description |
|---|---|
| `GLUE_JOB_NAME` | Glue ETL job name |
| `SOURCE_BUCKET` | S3 bucket for raw input files |
| `STAGING_BUCKET` | S3 bucket for processed Parquet |
| `MANIFEST_BUCKET` | S3 bucket for COPY manifests |
| `UNLOAD_BUCKET` | S3 bucket for UNLOAD output |
| `REDSHIFT_DB` | Redshift database name |
| `REDSHIFT_SCHEMA` | Redshift schema |
| `REDSHIFT_TABLE` | Redshift target table |
| `REDSHIFT_SECRET_ARN` | Secrets Manager ARN for credentials |
| `REDSHIFT_ROLE_ARN` | IAM role ARN for COPY/UNLOAD |
| `LOCK_TABLE_NAME` | DynamoDB idempotency table |
| `SNS_ALERT_TOPIC_ARN` | SNS topic for failure alerts |
| `VALID_PREFIXES` | Comma-separated valid S3 key prefixes |
| `MAX_CONCURRENT_RUNS` | Max parallel Glue runs (default 3) |

---

## Redshift Table Design

| Table | Distribution | Sort Key | Purpose |
|---|---|---|---|
| `fact_orders` | `EVEN` | `(order_date, product_category, region, order_status)` | Main transaction fact |
| `fact_orders_staging` | `EVEN` | — | COPY target for UPSERT |
| `dim_customer` | `KEY(customer_id)` | `(customer_id, effective_from)` | SCD Type 2 |
| `dim_product` | `KEY(product_id)` | `(product_category, product_id)` | Product catalogue |
| `dim_date` | `ALL` | `full_date` | Date dimension (small, broadcast) |
| `etl_audit_log` | `ALL` | `started_at` | ETL run metadata |

