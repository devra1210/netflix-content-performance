locals {
  name_prefix = "${var.project_name}-${var.environment}"
}

module "storage" {
  source = "./modules/storage"

  name_prefix           = local.name_prefix
  force_destroy_buckets = var.force_destroy_buckets
}

module "analytics" {
  source = "./modules/analytics"

  name_prefix           = local.name_prefix
  athena_results_bucket = module.storage.athena_results_bucket_name
}

module "streaming" {
  source = "./modules/streaming"

  name_prefix             = local.name_prefix
  kinesis_stream_mode     = var.kinesis_stream_mode
  kinesis_shard_count     = var.kinesis_shard_count
  kinesis_retention_hours = var.kinesis_retention_hours
}
