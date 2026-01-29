
  create or replace   view BAIN_ANALYTICS.DEV.stg_valuations
  
  
  
  
  as (
    -- Pipeline C: Staging Layer
-- stg_valuations.sql



select
    valuation_id,
    portfolio_id,
    valuation_date,
    nav,
    nav_usd,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_valuations
  );

