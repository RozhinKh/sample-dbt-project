-- Pipeline B: Staging Layer
-- stg_securities.sql
-- Purpose: Clean security master data



select
    security_id,
    ticker,
    security_name,
    asset_class,
    sector,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_securities