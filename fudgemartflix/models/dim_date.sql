with source as (
    select * from {{ source('conformed', 'datedimension') }}
)

select
    datekey                                             as date_key,
    date                                                as full_date,
    dayname                                             as day_name,
    monthname                                           as month_name,
    quartername                                         as quarter,
    year::varchar                                       as year,
    case when weekday = 'N' then 'Yes' else 'No' end    as is_weekend,
    'No'::varchar                                       as is_holiday  -- not available in source; placeholder
from source