-- Load the Netflix ROI star schema from curated Parquet.

BEGIN;

CREATE TEMP TABLE staging_content_performance (
    performance_id VARCHAR(64),
    title_id VARCHAR(64),
    region VARCHAR(32),
    year INTEGER,
    total_watch_hours DOUBLE PRECISION,
    churn_rate_post_title DOUBLE PRECISION,
    unique_viewers BIGINT,
    license_region VARCHAR(32),
    license_cost_usd DOUBLE PRECISION,
    license_year INTEGER,
    title_name VARCHAR(512),
    genre VARCHAR(512),
    secondary_genre VARCHAR(512),
    release_year INTEGER,
    content_type VARCHAR(32),
    popularity DOUBLE PRECISION,
    duration_minutes DOUBLE PRECISION,
    rating VARCHAR(64),
    language VARCHAR(32),
    country_of_origin VARCHAR(128),
    imdb_rating DOUBLE PRECISION,
    is_netflix_original BOOLEAN,
    added_to_platform DATE,
    sentiment_score DOUBLE PRECISION,
    cost_per_hour_watched DOUBLE PRECISION
);

COPY staging_content_performance
FROM 's3://{CURATED_BUCKET}/content_performance/'
IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
FORMAT AS PARQUET;

INSERT INTO analytics.dim_title (
    title_id,
    title_name,
    genre,
    secondary_genre,
    release_year,
    content_type,
    duration_minutes,
    rating,
    language,
    country_of_origin,
    imdb_rating,
    is_netflix_original,
    added_to_platform,
    popularity
)
SELECT
    title_rows.title_id,
    title_rows.title_name,
    title_rows.genre,
    title_rows.secondary_genre,
    title_rows.release_year,
    title_rows.content_type,
    title_rows.duration_minutes,
    title_rows.rating,
    title_rows.language,
    title_rows.country_of_origin,
    title_rows.imdb_rating,
    title_rows.is_netflix_original,
    title_rows.added_to_platform,
    title_rows.popularity
FROM (
    SELECT
        title_id,
        MAX(title_name) AS title_name,
        MAX(genre) AS genre,
        MAX(secondary_genre) AS secondary_genre,
        MAX(release_year) AS release_year,
        MAX(content_type) AS content_type,
        MAX(duration_minutes) AS duration_minutes,
        MAX(rating) AS rating,
        MAX(language) AS language,
        MAX(country_of_origin) AS country_of_origin,
        MAX(imdb_rating) AS imdb_rating,
        CASE
            WHEN MAX(CASE WHEN is_netflix_original THEN 1 ELSE 0 END) = 1 THEN TRUE
            ELSE FALSE
        END AS is_netflix_original,
        MAX(added_to_platform) AS added_to_platform,
        MAX(popularity) AS popularity
    FROM staging_content_performance
    WHERE title_id IS NOT NULL
    GROUP BY title_id
) title_rows
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.dim_title existing
    WHERE existing.title_id = title_rows.title_id
);

INSERT INTO analytics.dim_region (region_code, region_name)
SELECT DISTINCT
    COALESCE(NULLIF(region, ''), 'UNKNOWN') AS region_code,
    COALESCE(NULLIF(region, ''), 'UNKNOWN') AS region_name
FROM staging_content_performance staging
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.dim_region existing
    WHERE existing.region_code = COALESCE(NULLIF(staging.region, ''), 'UNKNOWN')
);

INSERT INTO analytics.dim_date (
    date_id,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week,
    day,
    day_of_week
)
SELECT DISTINCT
    (year * 10000) + 101 AS date_id,
    TO_DATE(year::VARCHAR || '-01-01', 'YYYY-MM-DD') AS full_date,
    year,
    1 AS quarter,
    1 AS month,
    'January' AS month_name,
    1 AS week,
    1 AS day,
    1 AS day_of_week
FROM staging_content_performance staging
WHERE year IS NOT NULL
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
    unique_viewers,
    license_cost_usd,
    cost_per_hour_watched,
    sentiment_score,
    churn_rate_post_title
)
SELECT
    performance_id,
    title_id,
    COALESCE(NULLIF(region, ''), 'UNKNOWN') AS region_code,
    (year * 10000) + 101 AS date_id,
    total_watch_hours,
    unique_viewers,
    license_cost_usd,
    cost_per_hour_watched,
    sentiment_score,
    churn_rate_post_title
FROM staging_content_performance staging
WHERE performance_id IS NOT NULL
  AND title_id IS NOT NULL
  AND year IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM analytics.fact_content_performance existing
      WHERE existing.performance_id = staging.performance_id
  );

COMMIT;
