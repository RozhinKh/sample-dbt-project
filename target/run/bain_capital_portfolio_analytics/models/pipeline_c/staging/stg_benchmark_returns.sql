
  create or replace   view BAIN_ANALYTICS.DEV.stg_benchmark_returns
  
  
  
  
  as (
    -- Pipeline C: Staging Layer
-- stg_benchmark_returns.sql



select
    benchmark_id,
    return_date,
    daily_return,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_benchmark_returns
  );

