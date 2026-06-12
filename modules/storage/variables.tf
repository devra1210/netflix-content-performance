variable "name_prefix" {
  description = "Resource name prefix."
  type        = string
}

variable "force_destroy_buckets" {
  description = "Allow Terraform to delete non-empty S3 buckets."
  type        = bool
}
