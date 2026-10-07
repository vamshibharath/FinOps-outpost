terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
  backend "s3" {} # configured per environment: -backend-config=envs/<env>.hcl
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      application   = "finops-attribution"
      business_unit = "finops"
      environment   = var.environment
      managed_by    = "terraform"
    }
  }
}

locals {
  prefix = "finops-${var.environment}"
}

# ---------------------------------------------------------------- data lake
resource "aws_s3_bucket" "lake" {
  bucket = "${local.prefix}-billing-lake"
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

# Raw landing files are only needed for replay/backfill: tier them down, then expire.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "landing-tiering"
    status = "Enabled"
    filter { prefix = "landing/" }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    expiration { days = var.landing_retention_days }
  }
  rule {
    id     = "athena-results-cleanup"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
}

# ---------------------------------------------------------------- catalog + Athena
resource "aws_glue_catalog_database" "billing" {
  name = replace("${local.prefix}_billing", "-", "_")
}

resource "aws_glue_catalog_table" "raw_focus" {
  name          = "raw_focus"
  database_name = aws_glue_catalog_database.billing.name
  table_type    = "EXTERNAL_TABLE"

  # Partition projection: no crawler runs, no MSCK REPAIR, partitions resolved at query time.
  parameters = {
    "classification"                          = "parquet"
    "projection.enabled"                      = "true"
    "projection.provider.type"                = "enum"
    "projection.provider.values"              = "AWS,Google_Cloud,Microsoft,Oracle"
    "projection.billing_period.type"          = "date"
    "projection.billing_period.format"        = "yyyy-MM"
    "projection.billing_period.range"         = "2023-01,NOW"
    "projection.billing_period.interval"      = "1"
    "projection.billing_period.interval.unit" = "MONTHS"
    "storage.location.template"               = "s3://${aws_s3_bucket.lake.bucket}/lake/raw_focus/provider=$${provider}/billing_period=$${billing_period}/"
  }

  partition_keys {
    name = "provider"
    type = "string"
  }
  partition_keys {
    name = "billing_period"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/lake/raw_focus/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    dynamic "columns" {
      for_each = var.focus_columns
      content {
        name = columns.key
        type = columns.value
      }
    }
  }
}

# Cost guardrail on the cost platform itself: cap bytes scanned per query.
resource "aws_athena_workgroup" "finops" {
  name = "${local.prefix}-wg"
  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
      encryption_configuration { encryption_option = "SSE_S3" }
    }
  }
}

# ---------------------------------------------------------------- Glue ingestion job
resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-ingest"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

# Least privilege: this bucket only, no wildcard S3 access.
resource "aws_iam_role_policy" "glue_lake" {
  name = "lake-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      Resource = "${aws_s3_bucket.lake.arn}/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn },
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      Resource = "arn:aws:logs:*:*:/aws-glue/*" },
    ]
  })
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.lake.id
  key    = "scripts/focus_to_parquet.py"
  source = "${path.module}/../../ingestion/glue/focus_to_parquet.py"
  etag   = filemd5("${path.module}/../../ingestion/glue/focus_to_parquet.py")
}

resource "aws_glue_job" "ingest" {
  name              = "${local.prefix}-focus-ingest"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = var.glue_workers
  timeout           = 60
  execution_class   = "FLEX" # cheaper spare-capacity execution for a non-latency-critical batch job

  command {
    script_location = "s3://${aws_s3_bucket.lake.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }
  default_arguments = {
    "--lake_path"                        = "s3://${aws_s3_bucket.lake.bucket}/lake/raw_focus/"
    "--quarantine_path"                  = "s3://${aws_s3_bucket.lake.bucket}/quarantine/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
  }
}

# ---------------------------------------------------------------- Aurora: allocation reference data
# Small operational store for governed reference data (tag aliases, allocation rules,
# account -> owner hierarchy) edited by FinOps; synced to the warehouse each run.
resource "aws_rds_cluster" "reference" {
  cluster_identifier          = "${local.prefix}-reference"
  engine                      = "aurora-postgresql"
  engine_mode                 = "provisioned"
  engine_version              = "16.4"
  database_name               = "finops_reference"
  master_username             = "finops_admin"
  manage_master_user_password = true # secret in Secrets Manager, never in state/vars
  storage_encrypted           = true
  db_subnet_group_name        = var.db_subnet_group_name
  vpc_security_group_ids      = var.db_security_group_ids
  skip_final_snapshot         = var.environment != "prod"
  deletion_protection         = var.environment == "prod"

  serverlessv2_scaling_configuration {
    min_capacity = 0 # scales to zero when idle: reference data is read a few times a day
    max_capacity = 2
  }
}

resource "aws_rds_cluster_instance" "reference" {
  identifier         = "${local.prefix}-reference-1"
  cluster_identifier = aws_rds_cluster.reference.id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.reference.engine
  engine_version     = aws_rds_cluster.reference.engine_version
}
