# Netflix Content Performance Analytics

This project builds an AWS analytics pipeline for measuring Netflix-style content performance and ROI. It lands raw Kaggle-style CSV data in S3, cleans and joins it with AWS Glue, loads a Redshift star schema, and uses dbt to produce dashboard-ready marts.

## Architecture

```text
Local Kaggle-style CSV files
        |
        | ingestion/upload_to_s3.py
        v
S3 raw bucket
        |
        | AWS Glue cleaning jobs
        v
S3 curated bucket
        |
        | warehouse/load_data.sql
        v
Redshift database: netflix_roi
        |
        | dbt staging views and mart tables
        v
Dashboard-ready content_performance mart
```

The Terraform stack also provisions a Kinesis stream for streaming/event ingestion experiments, a Glue Data Catalog database, and an Athena workgroup for S3-based analysis.

## What This Project Contains

- **Infrastructure as Code:** Terraform modules for S3 storage, Glue/Athena analytics resources, and Kinesis streaming.
- **Raw ingestion:** Python scripts for uploading local CSV datasets to the raw S3 zone and generating simulated event/licensing data.
- **Glue transformations:** PySpark Glue jobs that clean movies, users, watch history, licensing, recommendations, search logs, and review sentiment.
- **Curated metric build:** A Glue join job that produces title-region-year content performance metrics in curated Parquet.
- **Redshift warehouse:** SQL scripts to create the `netflix_roi` database, build an `analytics` star schema, and load curated Parquet from S3.
- **dbt models:** Staging views and a final `content_performance` mart with ROI and churn-risk flags.
- **Data-quality tests:** pytest checks for key fact-table quality rules.

## Repository Layout

```text
ingestion/                 Local ingestion and sample data generation scripts
modules/                   Terraform modules for storage, analytics, and streaming
transform/glue_jobs/       AWS Glue PySpark ETL jobs
transform/dbt/             dbt project, staging models, and marts
warehouse/                 Redshift database, schema, and load SQL
tests/                     Glue coverage and Redshift data-quality tests
data/                      Local source CSV files, not required in Git
```

## Provision AWS Resources

Copy the example Terraform variables and deploy the stack:

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform plan
terraform apply
```

For throwaway local test environments, set this in `terraform.tfvars` if you want Terraform to delete non-empty buckets during `terraform destroy`:

```hcl
force_destroy_buckets = true
```

Useful outputs include:

```bash
terraform output
```

The outputs include the raw bucket, curated bucket, Glue scripts bucket, Athena results bucket, and Kinesis stream names.

## Configure Local Environment

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Create a local `.env` file with the deployed bucket names and Redshift connection values:

```text
RAW_BUCKET=<raw-bucket-name>
CURATED_BUCKET=<curated-bucket-name>
AWS_REGION=us-west-2

REDSHIFT_ENDPOINT=<redshift-endpoint>
REDSHIFT_DB=netflix_roi
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=<admin-password>
REDSHIFT_IAM_ROLE_ARN=<redshift-s3-read-role-arn>
```

## Ingest Raw Data

Place the expected CSV files in `data/`:

- `movies.csv`
- `watch_history.csv`
- `users.csv`
- `reviews.csv`
- `licensing_costs.csv`
- `search_logs.csv`
- `recommendation_logs.csv`

If needed, generate simulated licensing costs from movie metadata:

```bash
python ingestion/generate_licensing.py
```

Upload the raw CSVs to S3:

```bash
python ingestion/upload_to_s3.py
```

You can also generate simulated streaming play events:

```bash
python ingestion/stream_generator.py --count 10000
```

## Run Glue Transformations

Upload the Glue job scripts to the Glue scripts bucket under the `scripts/` prefix, then run the Glue jobs in this order:

1. `clean_movies.py`
2. `clean_licensing.py`
3. `clean_users.py`
4. `clean_watch_history.py`
5. `clean_recommendation_logs.py`
6. `clean_search_logs.py`
7. `clean_review_sentiment.py`
8. `join_content_metrics.py`

The final Glue job writes curated Parquet to:

```text
s3://<curated-bucket>/content_performance/
```

## Load Redshift

The Redshift warehouse is modeled in the `netflix_roi` database under the `analytics` schema.

Create the database while connected to an existing admin database, usually `dev`:

```bash
psql -h <redshift-endpoint> -p 5439 -U admin -d dev -f warehouse/create_database.sql
```

Create the star schema:

```bash
psql -h <redshift-endpoint> -p 5439 -U admin -d netflix_roi -f warehouse/create_tables.sql
```

Before loading data, replace these placeholders in `warehouse/load_data.sql`:

```text
{CURATED_BUCKET}
{REDSHIFT_IAM_ROLE_ARN}
```

Then load the curated Parquet into Redshift:

```bash
psql -h <redshift-endpoint> -p 5439 -U admin -d netflix_roi -f warehouse/load_data.sql
```

The star schema contains:

- `analytics.dim_title`
- `analytics.dim_region`
- `analytics.dim_date`
- `analytics.fact_content_performance`

## Run dbt Models

The dbt project reads from Redshift source tables in `netflix_roi.analytics`.

From the dbt project directory:

```bash
cd transform/dbt
dbt debug
dbt run
```

dbt creates:

- staging views in the configured staging schema
- the final `content_performance` mart in the configured marts schema

The final mart joins fact and dimension tables and adds:

- `roi_flag`, including `low ROI` when `cost_per_hour_watched > 50`
- `high_churn_risk`, true when `churn_rate_post_title > 0.3`

## Run Tests

Run the local test suite:

```bash
pytest
```

The Redshift data-quality tests check:

- no null `title_id` values in the fact table
- no negative `cost_per_hour_watched` values
- `sentiment_score` stays between `0` and `1`
- no duplicate `performance_id` values
- fact table row count is greater than `1000`

If Redshift connection values are missing or still placeholders in `.env`, those live database tests are skipped.

## Current Notes

- Terraform currently provisions the data-lake and Glue/Athena/Kinesis resources. Redshift database/schema creation is handled by SQL scripts in `warehouse/`.
- The dbt models read from Redshift, not directly from S3.
- `clean_reviews.py` is a compatibility entry point for the review sentiment cleaner.
- The Kinesis stream is available for streaming experiments, while the main batch pipeline uses local CSV upload to S3.
