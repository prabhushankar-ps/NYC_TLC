
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select pickup_zone_id as from_field
    from NYC_TAXI.ANALYTICS_marts.fct_trips
    where pickup_zone_id is not null
),

parent as (
    select zone_id as to_field
    from NYC_TAXI.ANALYTICS_marts.dim_zones
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test