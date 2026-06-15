select *
from {{ source('analytics', 'fact_content_performance') }}
