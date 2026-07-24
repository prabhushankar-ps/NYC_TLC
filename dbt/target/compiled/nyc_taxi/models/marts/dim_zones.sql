select
    zone_id,
    borough,
    zone_name,
    service_zone,
    case when borough = 'Manhattan' then true else false end as is_manhattan
from NYC_TAXI.ANALYTICS_staging.stg_taxi_zones