select
    zone_id,
    borough,
    zone_name,
    service_zone,
    case when borough = 'Manhattan' then true else false end as is_manhattan
from {{ ref('stg_taxi_zones') }}