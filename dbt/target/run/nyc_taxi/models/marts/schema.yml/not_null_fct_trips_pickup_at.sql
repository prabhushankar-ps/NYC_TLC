
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_at
from NYC_TAXI.ANALYTICS_marts.fct_trips
where pickup_at is null



  
  
      
    ) dbt_internal_test