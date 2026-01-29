
  create or replace   view BAIN_ANALYTICS.DEV.stg_brokers
  
  
  
  
  as (
    -- Pipeline B: Staging Layer
-- stg_brokers.sql
-- Purpose: Clean broker/counterparty data



select
    broker_id,
    broker_name,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_brokers
  );

