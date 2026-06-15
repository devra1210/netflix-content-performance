-- Replace placeholders before running:
--   {CURATED_BUCKET} with your curated bucket name
--   {REDSHIFT_IAM_ROLE_ARN} with the IAM role Redshift uses to read S3

BEGIN;

CREATE TEMP TABLE staging_content_performance (
    performance_id VARCHAR(64),
    title_id VARCHAR(64),
    title_name VARCHAR(512),
    genre VARCHAR(512),
    release_year INTEGER,
    content_type VARCHAR(32),
    popularity DOUBLE PRECISION,
    total_watch_hours DOUBLE PRECISION,
    license_cost_usd DOUBLE PRECISION,
    cost_per_hour_watched DOUBLE PRECISION,
    sentiment_score DOUBLE PRECISION,
    churn_rate_post_title DOUBLE PRECISION,
    unique_viewers BIGINT,
    region VARCHAR(16),
    year INTEGER
);

COPY staging_content_performance
FROM 's3://{CURATED_BUCKET}/content_performance/'
IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
FORMAT AS PARQUET;

INSERT INTO analytics.dim_title (title_id, title_name, genre, release_year, content_type)
SELECT DISTINCT
    staging.title_id,
    staging.title_name,
    staging.genre,
    staging.release_year,
    staging.content_type
FROM staging_content_performance staging
WHERE staging.title_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM analytics.dim_title existing
      WHERE existing.title_id = staging.title_id
  );

INSERT INTO analytics.dim_region (region_code, region_name)
SELECT DISTINCT
    COALESCE(staging.region, 'UNKNOWN') AS region_code,
    COALESCE(staging.region, 'UNKNOWN') AS region_name
FROM staging_content_performance staging
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.dim_region existing
    WHERE existing.region_code = COALESCE(staging.region, 'UNKNOWN')
);

INSERT INTO analytics.dim_date (date_id, year, month, week, day)
SELECT DISTINCT
    (staging.year * 10000) + 101 AS date_id,
    staging.year,
    1 AS month,
    1 AS week,
    1 AS day
FROM staging_content_performance staging
WHERE staging.year IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM analytics.dim_date existing
      WHERE existing.date_id = (staging.year * 10000) + 101
  );

INSERT INTO analytics.fact_content_performance (
    performance_id,
    title_id,
    region_code,
    date_id,
    total_watch_hours,
    license_cost_usd,
    cost_per_hour_watched,
    sentiment_score,
    churn_rate_post_title
)
SELECT
    performance_id,
    title_id,
    COALESCE(region, 'UNKNOWN') AS region_code,
    (year * 10000) + 101 AS date_id,
    total_watch_hours,
    license_cost_usd,
    cost_per_hour_watched,
    sentiment_score,
    churn_rate_post_title
FROM staging_content_performance
WHERE performance_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM analytics.fact_content_performance existing
    WHERE existing.performance_id = staging_content_performance.performance_id
);

COMMIT;
