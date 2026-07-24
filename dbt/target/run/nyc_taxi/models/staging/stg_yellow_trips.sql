
  create or replace   view NYC_TAXI.ANALYTICS_staging.stg_yellow_trips
  
  
  
  
  as (
    with source as (

    select * from NYC_TAXI.RAW.raw_yellow_trips

),

renamed as (

    select
        vendorid                          as vendor_id,
        tpep_pickup_datetime              as pickup_at,
        tpep_dropoff_datetime             as dropoff_at,
        passenger_count::int              as passenger_count,
        trip_distance                     as trip_distance_miles,
        ratecodeid::int                   as rate_code_id,
        store_and_fwd_flag,
        pulocationid                      as pickup_zone_id,
        dolocationid                      as dropoff_zone_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        datediff('second', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_seconds,
        _source_file,
        _loaded_at

    from source

)

select *
from renamed
where pickup_at >= '2024-01-01'
  and pickup_at <  '2025-01-01'
  and dropoff_at > pickup_at
  and trip_distance_miles > 0
  and total_amount > 0
  );

