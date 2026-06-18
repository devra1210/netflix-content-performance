variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "athena_results_bucket" {
  description = "S3 bucket where Athena query results are written."
  type        = string
}

variable "raw_bucket" {
  description = "S3 bucket for raw data."
  type        = string
}

variable "curated_bucket" {
  description = "S3 bucket for curated data."
  type        = string
}

variable "glue_scripts_bucket" {
  description = "S3 bucket for Glue job scripts."
  type        = string
}
