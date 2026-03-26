# variables.tf — Input variable definitions for the AWS Glue + Redshift ETL stack

# ─── Project / Environment ────────────────────────────────────────────────────

variable "project" {
  description = "Project name prefix applied to all resource names and tags."
  type        = string
  default     = "glue-redshift-etl"
}

variable "environment" {
  description = "Deployment environment: dev, staging, or prod."
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "AWS account ID (used in IAM policy ARN construction)."
  type        = string
}

variable "tags" {
  description = "Additional tags applied to all taggable resources."
  type        = map(string)
  default     = {}
}

# ─── Networking ───────────────────────────────────────────────────────────────

variable "vpc_id" {
  description = "VPC ID where Redshift and Glue connections run."
  type        = string
}

variable "private_subnet_ids" {
  description = "List of private subnet IDs for Redshift and Glue (minimum 2 AZs)."
  type        = list(string)
}

variable "redshift_security_group_ingress_cidrs" {
  description = "CIDR blocks allowed to connect to Redshift on port 5439."
  type        = list(string)
  default     = []
}

# ─── S3 Buckets ───────────────────────────────────────────────────────────────

variable "source_bucket_name" {
  description = "S3 bucket for raw input data. Leave blank to create a new bucket."
  type        = string
  default     = ""
}

variable "s3_raw_prefix" {
  description = "S3 prefix for raw orders data (e.g. 'orders')."
  type        = string
  default     = "orders"
}

variable "s3_lifecycle_expire_raw_days" {
  description = "Days after which raw objects are expired (deleted)."
  type        = number
  default     = 90
}

variable "s3_lifecycle_expire_staging_days" {
  description = "Days after which staging (processed Parquet) objects expire."
  type        = number
  default     = 30
}

variable "s3_lifecycle_expire_unload_days" {
  description = "Days after which UNLOAD output files expire."
  type        = number
  default     = 7
}

# ─── KMS ──────────────────────────────────────────────────────────────────────

variable "kms_key_deletion_window_days" {
  description = "KMS key deletion window in days (7–30)."
  type        = number
  default     = 30
}

variable "kms_admin_arns" {
  description = "IAM ARNs that can administer (rotate, delete) the KMS key."
  type        = list(string)
}

# ─── Glue ─────────────────────────────────────────────────────────────────────

variable "glue_job_name" {
  description = "Name of the Glue ETL job."
  type        = string
  default     = "s3-to-redshift-orders"
}

variable "glue_max_capacity" {
  description = "Glue job DPU capacity (used for non-worker-type jobs)."
  type        = number
  default     = 10
}

variable "glue_worker_type" {
  description = "Glue worker type: Standard, G.1X, G.2X, G.4X, G.8X."
  type        = string
  default     = "G.1X"
}

variable "glue_num_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 10
}

variable "glue_max_retries" {
  description = "Number of times Glue retries the job on failure."
  type        = number
  default     = 1
}

variable "glue_timeout_minutes" {
  description = "Glue job timeout in minutes."
  type        = number
  default     = 120
}

variable "glue_max_concurrent_runs" {
  description = "Maximum concurrent Glue job runs."
  type        = number
  default     = 3
}

variable "glue_python_version" {
  description = "Python version for the Glue job (3 = Python 3)."
  type        = string
  default     = "3"
}

variable "glue_glue_version" {
  description = "Glue version (e.g. '4.0')."
  type        = string
  default     = "4.0"
}

# ─── Redshift ─────────────────────────────────────────────────────────────────

variable "redshift_cluster_identifier" {
  description = "Redshift cluster identifier."
  type        = string
  default     = "analytics-cluster"
}

variable "redshift_database_name" {
  description = "Redshift database name."
  type        = string
  default     = "analytics"
}

variable "redshift_master_username" {
  description = "Redshift master username."
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Redshift master password (stored in Secrets Manager)."
  type        = string
  sensitive   = true
}

variable "redshift_node_type" {
  description = "Redshift node type (e.g. dc2.large, ra3.xlplus)."
  type        = string
  default     = "ra3.xlplus"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes (1 = single-node)."
  type        = number
  default     = 2
}

variable "redshift_port" {
  description = "Redshift cluster port."
  type        = number
  default     = 5439
}

variable "redshift_schema" {
  description = "Redshift schema for ETL target tables."
  type        = string
  default     = "orders"
}

variable "redshift_target_table" {
  description = "Redshift target table name."
  type        = string
  default     = "fact_orders"
}

variable "redshift_snapshot_identifier" {
  description = "Optional: restore from a Redshift snapshot."
  type        = string
  default     = null
}

variable "redshift_automated_snapshot_retention_period" {
  description = "Days to retain automated Redshift snapshots (0 = disabled)."
  type        = number
  default     = 7
}

# ─── Lambda ───────────────────────────────────────────────────────────────────

variable "lambda_memory_mb" {
  description = "Lambda function memory in MB."
  type        = number
  default     = 256
}

variable "lambda_timeout_secs" {
  description = "Lambda function timeout in seconds."
  type        = number
  default     = 60
}

variable "lambda_reserved_concurrency" {
  description = "Lambda reserved concurrency (-1 = unreserved)."
  type        = number
  default     = 10
}

variable "sqs_visibility_timeout_secs" {
  description = "SQS message visibility timeout (should be >= lambda_timeout_secs * 6)."
  type        = number
  default     = 360
}

variable "sqs_message_retention_secs" {
  description = "SQS message retention period in seconds."
  type        = number
  default     = 86400   # 24 hours
}

variable "sqs_max_receive_count" {
  description = "Max receive count before message goes to DLQ."
  type        = number
  default     = 3
}

# ─── Alerting ─────────────────────────────────────────────────────────────────

variable "alert_email_addresses" {
  description = "Email addresses to subscribe to the SNS alert topic."
  type        = list(string)
  default     = []
}

# ─── DynamoDB ─────────────────────────────────────────────────────────────────

variable "dynamodb_lock_table_name" {
  description = "DynamoDB table name for Lambda idempotency locks."
  type        = string
  default     = "glue-trigger-locks"
}

variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode: PAY_PER_REQUEST or PROVISIONED."
  type        = string
  default     = "PAY_PER_REQUEST"
}
