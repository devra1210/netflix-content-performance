select *
from {{ source('analytics', 'dim_title') }}
