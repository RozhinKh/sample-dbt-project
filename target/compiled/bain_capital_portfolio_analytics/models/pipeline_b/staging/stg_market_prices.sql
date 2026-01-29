-- Pipeline B: Staging Layer
-- stg_market_prices.sql
-- Purpose: Clean market price data



select
    security_id,
    price_date,
    close_price,
    volume,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_market_prices