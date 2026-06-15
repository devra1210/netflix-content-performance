with performance as (
    select *
    from {{ source('analytics', 'fact_content_performance') }}
),

titles as (
    select *
    from {{ source('analytics', 'dim_title') }}
),

regions as (
    select *
    from {{ source('analytics', 'dim_region') }}
),

dates as (
    select *
    from {{ source('analytics', 'dim_date') }}
)

select
    performance.performance_id,
    performance.title_id,
    titles.title_name,
    titles.genre,
    titles.release_year,
    titles.content_type,
    performance.region_code,
    regions.region_name,
    dates.year,
    dates.month,
    dates.week,
    dates.day,
    performance.total_watch_hours,
    performance.license_cost_usd,
    performance.cost_per_hour_watched,
    performance.sentiment_score,
    performance.churn_rate_post_title,
    case
        when performance.cost_per_hour_watched > 50 then 'low ROI'
        when performance.cost_per_hour_watched between 20 and 50 then 'medium ROI'
        else 'high ROI'
    end as roi_flag,
    case
        when performance.churn_rate_post_title > 0.3 then true
        else false
    end as high_churn_risk
from performance
left join titles
    on performance.title_id = titles.title_id
left join regions
    on performance.region_code = regions.region_code
left join dates
    on performance.date_id = dates.date_id
