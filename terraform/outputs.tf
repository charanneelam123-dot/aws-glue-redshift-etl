# outputs.tf — Exported values for consumption by other stacks / CI pipelines

# ─── S3 ───────────────────────────────────────────────────────────────────────

output "s3_raw_bucket" {
  description = "S3 bucket name for raw input data."
  value       = aws_s3_bucket.buckets["raw"].bucket
}

output "s3_staging_bucket" {
  description = "S3 bucket name for Glue-processed Parquet staging data."
  value       = aws_s3_bucket.buckets["staging"].bucket
}

output "s3_manifest_bucket" {
  description = "S3 bucket name for Redshift COPY manifests."
  value       = aws_s3_bucket.buckets["manifest"].bucket
}

output "s3_unload_bucket" {
  description = "S3 bucket name for Redshift UNLOAD output."
  value       = aws_s3_bucket.buckets["unload"].bucket
}

output "s3_glue_assets_bucket" {
  description = "S3 bucket for Glue job scripts, temp data, and Spark logs."
  value       = aws_s3_bucket.buckets["glue"].bucket
}

# ─── KMS ──────────────────────────────────────────────────────────────────────

output "kms_key_arn" {
  description = "ARN of the KMS CMK used for ETL encryption."
  value       = aws_kms_key.etl.arn
}

output "kms_key_alias" {
  description = "Alias of the KMS CMK."
  value       = aws_kms_alias.etl.name
}

# ─── IAM ──────────────────────────────────────────────────────────────────────

output "glue_role_arn" {
  description = "ARN of the IAM role used by the Glue ETL job."
  value       = aws_iam_role.glue.arn
}

output "redshift_copy_role_arn" {
  description = "ARN of the IAM role attached to Redshift for COPY/UNLOAD operations."
  value       = aws_iam_role.redshift_copy.arn
}

output "lambda_role_arn" {
  description = "ARN of the IAM role used by the Lambda trigger function."
  value       = aws_iam_role.lambda_trigger.arn
}

# ─── Glue ─────────────────────────────────────────────────────────────────────

output "glue_job_name" {
  description = "Name of the deployed Glue ETL job."
  value       = aws_glue_job.s3_to_redshift.name
}

output "glue_catalog_database" {
  description = "Glue Data Catalog database name."
  value       = aws_glue_catalog_database.etl.name
}

# ─── Redshift ─────────────────────────────────────────────────────────────────

output "redshift_cluster_id" {
  description = "Redshift cluster identifier."
  value       = aws_redshift_cluster.main.cluster_identifier
}

output "redshift_endpoint" {
  description = "Redshift cluster endpoint (host:port)."
  value       = "${aws_redshift_cluster.main.endpoint}:${var.redshift_port}"
  sensitive   = true
}

output "redshift_database" {
  description = "Redshift database name."
  value       = aws_redshift_cluster.main.database_name
}

# ─── Secrets Manager ──────────────────────────────────────────────────────────

output "redshift_secret_arn" {
  description = "ARN of the Secrets Manager secret holding Redshift credentials."
  value       = aws_secretsmanager_secret.redshift.arn
  sensitive   = true
}

# ─── Lambda ───────────────────────────────────────────────────────────────────

output "lambda_function_name" {
  description = "Name of the Lambda trigger function."
  value       = aws_lambda_function.trigger_glue.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda trigger function."
  value       = aws_lambda_function.trigger_glue.arn
}

# ─── SQS ──────────────────────────────────────────────────────────────────────

output "sqs_trigger_queue_url" {
  description = "URL of the SQS queue that triggers the Lambda function."
  value       = aws_sqs_queue.trigger.url
}

output "sqs_trigger_queue_arn" {
  description = "ARN of the SQS trigger queue."
  value       = aws_sqs_queue.trigger.arn
}

output "sqs_dlq_url" {
  description = "URL of the SQS dead-letter queue."
  value       = aws_sqs_queue.trigger_dlq.url
}

# ─── DynamoDB ─────────────────────────────────────────────────────────────────

output "dynamodb_lock_table_name" {
  description = "DynamoDB table name for Lambda idempotency locks."
  value       = aws_dynamodb_table.locks.name
}

# ─── SNS ──────────────────────────────────────────────────────────────────────

output "sns_alert_topic_arn" {
  description = "ARN of the SNS topic for ETL failure alerts."
  value       = aws_sns_topic.alerts.arn
}

# ─── CloudWatch ───────────────────────────────────────────────────────────────

output "glue_log_group" {
  description = "CloudWatch log group for Glue job logs."
  value       = aws_cloudwatch_log_group.glue.name
}

output "lambda_log_group" {
  description = "CloudWatch log group for Lambda function logs."
  value       = aws_cloudwatch_log_group.lambda.name
}
