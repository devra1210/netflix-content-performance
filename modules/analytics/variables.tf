variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "athena_results_bucket" {
  description = "S3 bucket where Athena query results are written."
  type        = string
}
