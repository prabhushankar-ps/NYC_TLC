
  
    

create or replace transient table NYC_TAXI.ANALYTICS_marts.dim_zones
    
    
    
    
    

    as (select
    zone_id,
    borough,
    zone_name,
    service_zone,
    case when borough = 'Manhattan' then true else false end as is_manhattan
from NYC_TAXI.ANALYTICS_staging.stg_taxi_zones
    )
;


  