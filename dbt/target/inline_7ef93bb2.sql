select * from (
with trips as (

    select * from {{ ref('stg_yellow_trips') }}
    {% if is_incremental() %}
    where _loaded_at > (
        select coalesce(max(_loaded_at), '1900-01-01'::timestamp_ltz)
        from {{ this }}
    )
    {% endif %}

)
SELECT * FROM TRIPS
) as __preview_sbq__ limit 1000