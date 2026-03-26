###############################################################################
# main.tf
# AWS Glue + Redshift ETL Infrastructure
#
# Resources provisioned:
#   KMS          — single CMK for all S3 + Redshift encryption
#   S3           — raw, staging, manifest, unload buckets (versioned, encrypted)
#   IAM          — Glue role, Redshift COPY role, Lambda role (least privilege)
#   Glue         — ETL job, connection, crawler, catalog database
#   Redshift     — cluster, subnet group, parameter group, security group
#   Secrets Mgr  — Redshift credentials secret
#   Lambda       — S3-event trigger, SQS integration
#   SQS          — event queue + DLQ for Lambda
#   DynamoDB     — idempotency lock table for Lambda
#   SNS          — alert topic for failures and SLA breaches
#   CloudWatch   — log groups, ETL pipeline alarm
###############################################################################

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.30"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "s3" {
    # Configure via -backend-config or workspace-specific tfvars
    # bucket         = "your-tf-state-bucket"
    # key            = "glue-redshift-etl/terraform.tfstate"
    # region         = "us-east-1"
    # dynamodb_table = "terraform-state-lock"
    # encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(
      {
        Project     = var.project
        Environment = var.environment
        ManagedBy   = "Terraform"
        Owner       = "data-engineering"
      },
      var.tags,
    )
  }
}

locals {
  name_prefix  = "${var.project}-${var.environment}"
  account_id   = var.aws_account_id
  region       = var.aws_region
}

# ─── Random suffix (avoid global S3 name collisions) ─────────────────────────

resource "random_id" "suffix" {
  byte_length = 4
}

# ─── KMS — Customer Managed Key ───────────────────────────────────────────────

resource "aws_kms_key" "etl" {
  description             = "${local.name_prefix} — ETL encryption key"
  deletion_window_in_days = var.kms_key_deletion_window_days
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowAccountRoot"
        Effect    = "Allow"
        Principal = { AWS = "arn:aws:iam::${local.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid    = "AllowKeyAdmins"
        Effect = "Allow"
        Principal = {
          AWS = var.kms_admin_arns
        }
        Action   = ["kms:Create*", "kms:Describe*", "kms:Enable*", "kms:List*",
                    "kms:Put*", "kms:Update*", "kms:Revoke*", "kms:Disable*",
                    "kms:Get*", "kms:Delete*", "kms:ScheduleKeyDeletion", "kms:CancelKeyDeletion"]
        Resource = "*"
      },
      {
        Sid    = "AllowS3ServiceUse"
        Effect = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action   = ["kms:GenerateDataKey*", "kms:Decrypt"]
        Resource = "*"
      },
      {
        Sid    = "AllowGlueAndRedshift"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.glue.arn,
            aws_iam_role.redshift_copy.arn,
            aws_iam_role.lambda_trigger.arn,
          ]
        }
        Action   = ["kms:GenerateDataKey*", "kms:Decrypt", "kms:Encrypt"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_kms_alias" "etl" {
  name          = "alias/${local.name_prefix}-etl"
  target_key_id = aws_kms_key.etl.key_id
}

# ─── S3 Buckets ───────────────────────────────────────────────────────────────

locals {
  bucket_suffix = random_id.suffix.hex
  buckets = {
    raw      = "${local.name_prefix}-raw-${local.bucket_suffix}"
    staging  = "${local.name_prefix}-staging-${local.bucket_suffix}"
    manifest = "${local.name_prefix}-manifest-${local.bucket_suffix}"
    unload   = "${local.name_prefix}-unload-${local.bucket_suffix}"
    glue     = "${local.name_prefix}-glue-assets-${local.bucket_suffix}"
  }
}

resource "aws_s3_bucket" "buckets" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "buckets" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "buckets" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.buckets[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.etl.arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "buckets" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.buckets[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.buckets["raw"].id
  rule {
    id     = "expire-raw"
    status = "Enabled"
    filter { prefix = "" }
    expiration { days = var.s3_lifecycle_expire_raw_days }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "staging" {
  bucket = aws_s3_bucket.buckets["staging"].id
  rule {
    id     = "expire-staging"
    status = "Enabled"
    filter { prefix = "processed/" }
    expiration { days = var.s3_lifecycle_expire_staging_days }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "unload" {
  bucket = aws_s3_bucket.buckets["unload"].id
  rule {
    id     = "expire-unload"
    status = "Enabled"
    filter { prefix = "summaries/" }
    expiration { days = var.s3_lifecycle_expire_unload_days }
  }
}

# S3 event notification → SQS (for Lambda trigger)
resource "aws_s3_bucket_notification" "raw" {
  bucket = aws_s3_bucket.buckets["raw"].id
  queue {
    queue_arn     = aws_sqs_queue.trigger.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "${var.s3_raw_prefix}/"
    filter_suffix = ".parquet"
  }
  depends_on = [aws_sqs_queue_policy.trigger]
}

# ─── IAM ──────────────────────────────────────────────────────────────────────

# Glue ETL role
resource "aws_iam_role" "glue" {
  name = "${local.name_prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_custom" {
  name = "${local.name_prefix}-glue-custom"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
        ]
        Resource = concat(
          [for k, b in aws_s3_bucket.buckets : b.arn],
          [for k, b in aws_s3_bucket.buckets : "${b.arn}/*"],
        )
      },
      {
        Sid    = "KMSAccess"
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:GenerateDataKey*", "kms:Encrypt"]
        Resource = [aws_kms_key.etl.arn]
      },
      {
        Sid    = "SecretsManager"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue"]
        Resource = [aws_secretsmanager_secret.redshift.arn]
      },
      {
        Sid    = "RedshiftData"
        Effect = "Allow"
        Action = [
          "redshift-data:ExecuteStatement",
          "redshift-data:DescribeStatement",
          "redshift-data:GetStatementResult",
          "redshift-data:ListStatements",
        ]
        Resource = ["*"]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws-glue/*"]
      },
    ]
  })
}

# Redshift COPY/UNLOAD role (attached to cluster, not to IAM users)
resource "aws_iam_role" "redshift_copy" {
  name = "${local.name_prefix}-redshift-copy-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_copy" {
  name = "${local.name_prefix}-redshift-copy"
  role = aws_iam_role.redshift_copy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3CopyUnload"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject",
          "s3:ListBucket", "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.buckets["staging"].arn,
          "${aws_s3_bucket.buckets["staging"].arn}/*",
          aws_s3_bucket.buckets["manifest"].arn,
          "${aws_s3_bucket.buckets["manifest"].arn}/*",
          aws_s3_bucket.buckets["unload"].arn,
          "${aws_s3_bucket.buckets["unload"].arn}/*",
        ]
      },
      {
        Sid      = "KMSDecrypt"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey*"]
        Resource = [aws_kms_key.etl.arn]
      },
    ]
  })
}

# Lambda trigger role
resource "aws_iam_role" "lambda_trigger" {
  name = "${local.name_prefix}-lambda-trigger-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_trigger" {
  name = "${local.name_prefix}-lambda-trigger"
  role = aws_iam_role.lambda_trigger.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueStartJob"
        Effect = "Allow"
        Action = ["glue:StartJobRun", "glue:GetJobRuns", "glue:GetJob"]
        Resource = ["arn:aws:glue:${local.region}:${local.account_id}:job/${var.glue_job_name}"]
      },
      {
        Sid    = "DynamoDBLocks"
        Effect = "Allow"
        Action = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:DeleteItem"]
        Resource = [aws_dynamodb_table.locks.arn]
      },
      {
        Sid    = "SQS"
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage", "sqs:DeleteMessage",
          "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility",
        ]
        Resource = [aws_sqs_queue.trigger.arn]
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = [aws_sns_topic.alerts.arn]
      },
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = ["cloudwatch:PutMetricData"]
        Resource = ["*"]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/*"]
      },
      {
        Sid    = "KMSDecrypt"
        Effect = "Allow"
        Action = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.etl.arn]
      },
    ]
  })
}

# ─── Glue ─────────────────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "etl" {
  name        = replace("${local.name_prefix}_etl", "-", "_")
  description = "Glue Data Catalog database for ${local.name_prefix} ETL"
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.buckets["glue"].id
  key    = "scripts/s3_to_redshift.py"
  source = "${path.module}/../glue_jobs/s3_to_redshift.py"
  etag   = filemd5("${path.module}/../glue_jobs/s3_to_redshift.py")
}

resource "aws_glue_job" "s3_to_redshift" {
  name              = var.glue_job_name
  role_arn          = aws_iam_role.glue.arn
  glue_version      = var.glue_glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  max_retries       = var.glue_max_retries
  timeout           = var.glue_timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.buckets["glue"].bucket}/scripts/s3_to_redshift.py"
    python_version  = var.glue_python_version
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.buckets["glue"].bucket}/spark-logs/"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.buckets["glue"].bucket}/tmp/"
    "--source_bucket"                    = aws_s3_bucket.buckets["raw"].bucket
    "--source_prefix"                    = var.s3_raw_prefix
    "--staging_bucket"                   = aws_s3_bucket.buckets["staging"].bucket
    "--manifest_bucket"                  = aws_s3_bucket.buckets["manifest"].bucket
    "--unload_bucket"                    = aws_s3_bucket.buckets["unload"].bucket
    "--redshift_db"                      = var.redshift_database_name
    "--redshift_schema"                  = var.redshift_schema
    "--redshift_table"                   = var.redshift_target_table
    "--redshift_secret_arn"              = aws_secretsmanager_secret.redshift.arn
    "--redshift_role_arn"                = aws_iam_role.redshift_copy.arn
    "--environment"                      = var.environment
  }

  execution_property {
    max_concurrent_runs = var.glue_max_concurrent_runs
  }

  notification_property {
    notify_delay_after = 60
  }

  depends_on = [aws_s3_object.glue_script]
}

resource "aws_cloudwatch_log_group" "glue" {
  name              = "/aws-glue/jobs/${var.glue_job_name}"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.etl.arn
}

# ─── Redshift ─────────────────────────────────────────────────────────────────

resource "aws_redshift_subnet_group" "main" {
  name       = "${local.name_prefix}-subnet-group"
  subnet_ids = var.private_subnet_ids
}

resource "aws_security_group" "redshift" {
  name        = "${local.name_prefix}-redshift-sg"
  description = "Redshift cluster security group — allows Glue + BI tools"
  vpc_id      = var.vpc_id

  ingress {
    description = "Redshift port from Glue"
    from_port   = var.redshift_port
    to_port     = var.redshift_port
    protocol    = "tcp"
    cidr_blocks = var.redshift_security_group_ingress_cidrs
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }
}

resource "aws_redshift_parameter_group" "main" {
  name   = "${local.name_prefix}-params"
  family = "redshift-1.0"

  parameter {
    name  = "enable_user_activity_logging"
    value = "true"
  }
  parameter {
    name  = "require_ssl"
    value = "true"
  }
  parameter {
    name  = "max_cursor_result_set_size"
    value = "0"
  }
}

resource "aws_redshift_cluster" "main" {
  cluster_identifier        = "${local.name_prefix}-${var.redshift_cluster_identifier}"
  database_name             = var.redshift_database_name
  master_username           = var.redshift_master_username
  master_password           = var.redshift_master_password
  node_type                 = var.redshift_node_type
  number_of_nodes           = var.redshift_number_of_nodes
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  vpc_security_group_ids    = [aws_security_group.redshift.id]
  cluster_parameter_group_name = aws_redshift_parameter_group.main.name

  encrypted             = true
  kms_key_id            = aws_kms_key.etl.arn
  enhanced_vpc_routing  = true
  publicly_accessible   = false
  skip_final_snapshot   = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${local.name_prefix}-final-snapshot" : null
  snapshot_identifier   = var.redshift_snapshot_identifier
  automated_snapshot_retention_period = var.redshift_automated_snapshot_retention_period

  iam_roles = [aws_iam_role.redshift_copy.arn]
  port      = var.redshift_port

  logging {
    enable        = true
    bucket_name   = aws_s3_bucket.buckets["glue"].bucket
    s3_key_prefix = "redshift-audit-logs/"
  }

  depends_on = [aws_iam_role.redshift_copy]
}

# ─── Secrets Manager ──────────────────────────────────────────────────────────

resource "aws_secretsmanager_secret" "redshift" {
  name                    = "${local.name_prefix}/redshift/credentials"
  description             = "Redshift credentials for ${local.name_prefix}"
  kms_key_id              = aws_kms_key.etl.arn
  recovery_window_in_days = var.environment == "prod" ? 30 : 7
}

resource "aws_secretsmanager_secret_version" "redshift" {
  secret_id = aws_secretsmanager_secret.redshift.id
  secret_string = jsonencode({
    username            = var.redshift_master_username
    password            = var.redshift_master_password
    engine              = "redshift"
    host                = aws_redshift_cluster.main.endpoint
    port                = var.redshift_port
    dbname              = var.redshift_database_name
    dbClusterIdentifier = aws_redshift_cluster.main.cluster_identifier
  })
}

# ─── DynamoDB — Idempotency Locks ─────────────────────────────────────────────

resource "aws_dynamodb_table" "locks" {
  name         = "${local.name_prefix}-${var.dynamodb_lock_table_name}"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery { enabled = true }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.etl.arn
  }
}

# ─── SQS ──────────────────────────────────────────────────────────────────────

resource "aws_sqs_queue" "trigger_dlq" {
  name                       = "${local.name_prefix}-trigger-dlq"
  message_retention_seconds  = 1209600    # 14 days
  kms_master_key_id          = aws_kms_key.etl.arn
}

resource "aws_sqs_queue" "trigger" {
  name                       = "${local.name_prefix}-trigger"
  visibility_timeout_seconds = var.sqs_visibility_timeout_secs
  message_retention_seconds  = var.sqs_message_retention_secs
  kms_master_key_id          = aws_kms_key.etl.arn

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.trigger_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
}

resource "aws_sqs_queue_policy" "trigger" {
  queue_url = aws_sqs_queue.trigger.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowS3Notification"
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.trigger.arn
      Condition = {
        ArnLike = { "aws:SourceArn" = aws_s3_bucket.buckets["raw"].arn }
      }
    }]
  })
}

# ─── Lambda ───────────────────────────────────────────────────────────────────

data "archive_file" "lambda_trigger" {
  type        = "zip"
  source_file = "${path.module}/../lambda/trigger_glue.py"
  output_path = "${path.module}/../lambda/trigger_glue.zip"
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name_prefix}-trigger-glue"
  retention_in_days = 30
  kms_key_id        = aws_kms_key.etl.arn
}

resource "aws_lambda_function" "trigger_glue" {
  function_name    = "${local.name_prefix}-trigger-glue"
  role             = aws_iam_role.lambda_trigger.arn
  handler          = "trigger_glue.handler"
  runtime          = "python3.11"
  filename         = data.archive_file.lambda_trigger.output_path
  source_code_hash = data.archive_file.lambda_trigger.output_base64sha256
  timeout          = var.lambda_timeout_secs
  memory_size      = var.lambda_memory_mb
  reserved_concurrent_executions = var.lambda_reserved_concurrency

  environment {
    variables = {
      GLUE_JOB_NAME       = var.glue_job_name
      SOURCE_BUCKET       = aws_s3_bucket.buckets["raw"].bucket
      STAGING_BUCKET      = aws_s3_bucket.buckets["staging"].bucket
      MANIFEST_BUCKET     = aws_s3_bucket.buckets["manifest"].bucket
      UNLOAD_BUCKET       = aws_s3_bucket.buckets["unload"].bucket
      REDSHIFT_DB         = var.redshift_database_name
      REDSHIFT_SCHEMA     = var.redshift_schema
      REDSHIFT_TABLE      = var.redshift_target_table
      REDSHIFT_SECRET_ARN = aws_secretsmanager_secret.redshift.arn
      REDSHIFT_ROLE_ARN   = aws_iam_role.redshift_copy.arn
      LOCK_TABLE_NAME     = aws_dynamodb_table.locks.name
      SNS_ALERT_TOPIC_ARN = aws_sns_topic.alerts.arn
      ENVIRONMENT         = var.environment
      VALID_PREFIXES      = "${var.s3_raw_prefix}/"
      MAX_CONCURRENT_RUNS = tostring(var.glue_max_concurrent_runs)
    }
  }

  tracing_config { mode = "Active" }

  depends_on = [
    aws_cloudwatch_log_group.lambda,
    aws_iam_role_policy.lambda_trigger,
  ]
}

resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn                   = aws_sqs_queue.trigger.arn
  function_name                      = aws_lambda_function.trigger_glue.arn
  batch_size                         = 1    # process one S3 event at a time for isolation
  maximum_batching_window_in_seconds = 0
  function_response_types            = ["ReportBatchItemFailures"]
}

# ─── SNS ──────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "alerts" {
  name              = "${local.name_prefix}-etl-alerts"
  kms_master_key_id = aws_kms_key.etl.arn
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_email_addresses)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

# ─── CloudWatch Alarms ────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "glue_job_failure" {
  alarm_name          = "${local.name_prefix}-glue-job-failure"
  alarm_description   = "Glue ETL job failed"
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.numFailedTasks"
  dimensions          = { JobName = var.glue_job_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${local.name_prefix}-lambda-trigger-errors"
  alarm_description   = "Lambda S3 trigger function errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.trigger_glue.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
}

resource "aws_cloudwatch_metric_alarm" "sqs_dlq_messages" {
  alarm_name          = "${local.name_prefix}-sqs-dlq-messages"
  alarm_description   = "Messages appearing in trigger DLQ — events not processed"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.trigger_dlq.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  treat_missing_data  = "notBreaching"
}
