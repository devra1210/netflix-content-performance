CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_title (
    title_id VARCHAR(64) PRIMARY KEY,
    title_name VARCHAR(512),
    genre VARCHAR(512),
    release_year INTEGER,
    content_type VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS analytics.dim_region (
    region_code VARCHAR(16) PRIMARY KEY,
    region_name VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    week INTEGER NOT NULL,
    day INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics.fact_content_performance (
    performance_id VARCHAR(64) PRIMARY KEY,
    title_id VARCHAR(64) NOT NULL REFERENCES analytics.dim_title(title_id),
    region_code VARCHAR(16) NOT NULL REFERENCES analytics.dim_region(region_code),
    date_id INTEGER NOT NULL REFERENCES analytics.dim_date(date_id),
    total_watch_hours DECIMAL(18, 2),
    license_cost_usd DECIMAL(18, 2),
    cost_per_hour_watched DECIMAL(18, 4),
    sentiment_score DECIMAL(6, 5),
    churn_rate_post_title DECIMAL(6, 5)
);

INSERT INTO analytics.dim_region (region_code, region_name)
SELECT seed.region_code, seed.region_name
FROM (
    SELECT 'US' AS region_code, 'United States' AS region_name UNION ALL
    SELECT 'CA', 'Canada' UNION ALL
    SELECT 'GB', 'United Kingdom' UNION ALL
    SELECT 'IN', 'India' UNION ALL
    SELECT 'BR', 'Brazil' UNION ALL
    SELECT 'MX', 'Mexico' UNION ALL
    SELECT 'DE', 'Germany' UNION ALL
    SELECT 'JP', 'Japan' UNION ALL
    SELECT 'UNKNOWN', 'Unknown'
) seed
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.dim_region existing
    WHERE existing.region_code = seed.region_code
);
