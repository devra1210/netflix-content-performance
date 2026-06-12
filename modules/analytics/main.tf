resource "aws_glue_catalog_database" "content_performance" {
  name        = replace(var.name_prefix, "-", "_")
  description = "Catalog database for content performance datasets."
}

resource "aws_athena_workgroup" "content_performance" {
  name        = var.name_prefix
  description = "Athena workgroup for content performance analysis."
  state       = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.athena_results_bucket}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
