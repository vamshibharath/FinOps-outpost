output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_database" {
  value = aws_glue_catalog_database.billing.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.finops.name
}

output "glue_job_name" {
  value = aws_glue_job.ingest.name
}

output "reference_db_endpoint" {
  value = aws_rds_cluster.reference.endpoint
}
