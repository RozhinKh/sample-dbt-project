
  
    

create or replace transient table BAIN_ANALYTICS.DEV.stg_portfolios
    
    
    
    as (-- Pipeline A: Staging Layer
-- stg_portfolios.sql
-- Purpose: Clean and standardize portfolio dimension data
-- Models downstream: 1 (int_portfolio_attributes)



select
    portfolio_id,
    portfolio_name,
    portfolio_type,
    fund_id,
    status,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_portfolios
where status = 'ACTIVE'
    )
;


  