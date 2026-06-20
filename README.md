# Netflix Content Performance Terraform

This is a simple AWS analytics architecture for content performance data.

## Architecture

```text
Raw CSV/JSON/Parquet files
        |
        v
Kinesis stream ---> S3 raw bucket
        |
        v
ETL or notebook job, added later
        |
        v
S3 curated bucket ---> Glue Data Catalog ---> Athena Workgroup
                                      |
                                      v
                              Query results bucket
```

## Resources

- S3 raw data bucket for landed source files.
- S3 curated data bucket for modeled datasets.
- S3 Athena results bucket for query output.
- Kinesis Data Stream for Kaggle dataset records.
- Glue database for table metadata.
- Athena workgroup with an enforced result location.

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

For local test environments, set `force_destroy_buckets = true` if you want Terraform to delete non-empty buckets during `terraform destroy`.

The Kinesis stream defaults to `ON_DEMAND`, which is the simplest option for intermittent Kaggle dataset loads. Use `PROVISIONED` and adjust `kinesis_shard_count` if you need fixed capacity.

## Redshift Star Schema

The Redshift warehouse is modeled in the `netflix_roi` database under the `analytics` schema.

1. Connect to Redshift as an admin user and create the database if needed by running [warehouse/create_database.sql](/Users/devivaraprasadsunkari/netflix-content-performance/warehouse/create_database.sql):

   ```sql
   CREATE DATABASE netflix_roi;
   ```

2. Connect to `netflix_roi` and run [warehouse/create_tables.sql](/Users/devivaraprasadsunkari/netflix-content-performance/warehouse/create_tables.sql) to create:

   - `analytics.dim_title`
   - `analytics.dim_region`
   - `analytics.dim_date`
   - `analytics.fact_content_performance`

3. After the Glue jobs write curated Parquet to S3, replace the placeholders in [warehouse/load_data.sql](/Users/devivaraprasadsunkari/netflix-content-performance/warehouse/load_data.sql) and run it from `netflix_roi`.

## Suggested Next Steps

- Add a producer script that downloads the Kaggle dataset and writes each row to Kinesis.
- Add a Kinesis Data Firehose delivery stream if you want records automatically persisted into the raw S3 bucket.
- Add Glue tables for specific datasets such as titles, views, completion rate, and region-level performance.
- Add an ETL layer with AWS Glue, Lambda, dbt, or a scheduled container job.
- Add a dashboard layer with QuickSight, Superset, or a small web app.
