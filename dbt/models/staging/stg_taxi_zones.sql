select
    locationid    as zone_id,
    borough,
    zone          as zone_name,
    service_zone
from {{ source('raw', 'raw_taxi_zones') }}
where locationid is not null