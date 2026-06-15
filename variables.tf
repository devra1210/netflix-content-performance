variable "aws_region" {
  description = "AWS region where resources will be created."
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "Short project name used in resource naming."
  type        = string
  default     = "ncp"
}

variable "environment" {
  description = "Deployment environment name."
  type        = string
  default     = "dev"
}

variable "force_destroy_buckets" {
  description = "Allow Terraform to delete non-empty S3 buckets. Keep false outside throwaway environments."
  type        = bool
  default     = false
}

variable "kinesis_stream_mode" {
  description = "Kinesis stream capacity mode. Use ON_DEMAND for simple Kaggle dataset ingestion, or PROVISIONED for fixed shard capacity."
  type        = string
  default     = "ON_DEMAND"

  validation {
    condition     = contains(["ON_DEMAND", "PROVISIONED"], var.kinesis_stream_mode)
    error_message = "kinesis_stream_mode must be either ON_DEMAND or PROVISIONED."
  }
}

variable "kinesis_shard_count" {
  description = "Number of shards when kinesis_stream_mode is PROVISIONED."
  type        = number
  default     = 1

  validation {
    condition     = var.kinesis_shard_count >= 1
    error_message = "kinesis_shard_count must be at least 1."
  }
}

variable "kinesis_retention_hours" {
  description = "How long records are retained in the Kinesis stream."
  type        = number
  default     = 24

  validation {
    condition     = var.kinesis_retention_hours >= 24 && var.kinesis_retention_hours <= 8760
    error_message = "kinesis_retention_hours must be between 24 and 8760."
  }
}
