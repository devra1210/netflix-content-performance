select
    performance_id,
    title_id,
    region_code,
    date_id,
    license_cost_usd
from {{ source('analytics', 'fact_content_performance') }}
