output "raw_bucket_name" {
  description = "Raw data bucket name."
  value       = aws_s3_bucket.raw.bucket
}

output "curated_bucket_name" {
  description = "Curated data bucket name."
  value       = aws_s3_bucket.curated.bucket
}

output "athena_results_bucket_name" {
  description = "Athena results bucket name."
  value       = aws_s3_bucket.athena_results.bucket
}

output "glue_scripts_bucket_name" {
  description = "Glue scripts bucket name."
  value       = aws_s3_bucket.glue_scripts.bucket
}
