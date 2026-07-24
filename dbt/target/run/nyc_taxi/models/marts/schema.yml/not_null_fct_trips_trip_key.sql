
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_key
from NYC_TAXI.ANALYTICS_marts.fct_trips
where trip_key is null



  
  
      
    ) dbt_internal_test