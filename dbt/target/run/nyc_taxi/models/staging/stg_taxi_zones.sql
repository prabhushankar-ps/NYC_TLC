
  create or replace   view NYC_TAXI.ANALYTICS_staging.stg_taxi_zones
  
  
  
  
  as (
    select
    locationid    as zone_id,
    borough,
    zone          as zone_name,
    service_zone
from NYC_TAXI.RAW.raw_taxi_zones
where locationid is not null
  );

