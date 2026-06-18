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

# IAM role for Glue jobs
resource "aws_iam_role" "glue_job_role" {
  name = "${var.name_prefix}-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "${var.name_prefix}-glue-s3-access"
  role   = aws_iam_role.glue_job_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.raw_bucket}",
          "arn:aws:s3:::${var.raw_bucket}/*",
          "arn:aws:s3:::${var.curated_bucket}",
          "arn:aws:s3:::${var.curated_bucket}/*",
          "arn:aws:s3:::${var.glue_scripts_bucket}",
          "arn:aws:s3:::${var.glue_scripts_bucket}/*"
        ]
      }
    ]
  })
}

# Glue Jobs
resource "aws_glue_job" "clean_movies" {
  name     = "${var.name_prefix}-clean-movies"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_movies.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-movies"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "clean_licensing" {
  name     = "${var.name_prefix}-clean-licensing"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_licensing.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-licensing"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "clean_users" {
  name     = "${var.name_prefix}-clean-users"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_users.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-users"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "clean_watch_history" {
  name     = "${var.name_prefix}-clean-watch-history"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_watch_history.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-watch-history"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "clean_recommendation_logs" {
  name     = "${var.name_prefix}-clean-recommendation-logs"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_recommendation_logs.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-recommendation-logs"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "clean_search_logs" {
  name     = "${var.name_prefix}-clean-search-logs"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_search_logs.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-search-logs"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "clean_review_sentiment" {
  name     = "${var.name_prefix}-clean-review-sentiment"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/clean_review_sentiment.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-clean-review-sentiment"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}

resource "aws_glue_job" "join_content_metrics" {
  name     = "${var.name_prefix}-join-content-metrics"
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    script_location = "s3://${var.glue_scripts_bucket}/scripts/join_content_metrics.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--RAW_BUCKET"     = var.raw_bucket
    "--CURATED_BUCKET" = var.curated_bucket
    "--JOB_NAME"       = "${var.name_prefix}-join-content-metrics"
  }

  worker_type  = "G.1X"
  number_of_workers = 2
  glue_version = "4.0"
  max_retries  = 0
  timeout      = 2880
}
