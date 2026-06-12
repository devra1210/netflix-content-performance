output "raw_bucket_name" {
  description = "Bucket for source/raw content performance files."
  value       = module.storage.raw_bucket_name
}

output "curated_bucket_name" {
  description = "Bucket for cleaned and modeled datasets."
  value       = module.storage.curated_bucket_name
}

output "athena_results_bucket_name" {
  description = "Bucket where Athena query results are written."
  value       = module.storage.athena_results_bucket_name
}

output "glue_database_name" {
  description = "Glue database used by Athena."
  value       = module.analytics.glue_database_name
}

output "athena_workgroup_name" {
  description = "Athena workgroup for content performance queries."
  value       = module.analytics.athena_workgroup_name
}

output "kinesis_stream_name" {
  description = "Kinesis stream that receives Kaggle dataset records."
  value       = module.streaming.stream_name
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis stream that receives Kaggle dataset records."
  value       = module.streaming.stream_arn
}
