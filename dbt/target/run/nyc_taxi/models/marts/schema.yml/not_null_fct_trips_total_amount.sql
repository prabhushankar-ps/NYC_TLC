
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from NYC_TAXI.ANALYTICS_marts.fct_trips
where total_amount is null



  
  
      
    ) dbt_internal_test