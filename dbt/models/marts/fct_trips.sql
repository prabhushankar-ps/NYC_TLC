{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='_source_file'
    )
}}


with trips as (

    select * from {{ ref('stg_yellow_trips') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select coalesce(max(_loaded_at), '1900-01-01'::timestamp_ltz)
        from {{ this }}
    )
    {% endif %}

),

zones as (

    select * from {{ ref('dim_zones') }}

)

select
    -- surrogate key
    md5(
        t.vendor_id || '|' || t.pickup_at || '|' || t.dropoff_at || '|'
        || t.pickup_zone_id || '|' || t.total_amount
    )                                             as trip_key,

    -- grain / time
    t.pickup_at,
    t.dropoff_at,
    t.pickup_at::date                             as pickup_date,
    hour(t.pickup_at)                             as pickup_hour,
    dayname(t.pickup_at)                          as pickup_day_name,
    iff(dayofweek(t.pickup_at) in (0, 6), true, false) as is_weekend,

    -- geography
    t.pickup_zone_id,
    pu.borough                                    as pickup_borough,
    pu.zone_name                                  as pickup_zone,
    t.dropoff_zone_id,
    do.borough                                    as dropoff_borough,
    do.zone_name                                  as dropoff_zone,
    iff(pu.borough = do.borough, true, false)     as is_intra_borough,

    -- trip facts
    t.passenger_count,
    t.trip_distance_miles,
    t.trip_duration_seconds,
    round(t.trip_duration_seconds / 60.0, 2)      as trip_duration_minutes,
    round(
        t.trip_distance_miles / nullif(t.trip_duration_seconds / 3600.0, 0), 2
    )                                             as avg_speed_mph,

    -- money
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.congestion_surcharge,
    t.airport_fee,
    t.total_amount,
    round(t.tip_amount / nullif(t.fare_amount, 0) * 100, 2) as tip_pct_of_fare,

    -- descriptors
    case t.payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Unmapped'
    end                                           as payment_method,

    case t.rate_code_id
        when 1 then 'Standard'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau/Westchester'
        when 5 then 'Negotiated'
        when 6 then 'Group ride'
        else 'Unknown'
    end                                           as rate_type,

    t._source_file,
    t._loaded_at

from trips t
left join zones pu on t.pickup_zone_id  = pu.zone_id
left join zones do on t.dropoff_zone_id = do.zone_id