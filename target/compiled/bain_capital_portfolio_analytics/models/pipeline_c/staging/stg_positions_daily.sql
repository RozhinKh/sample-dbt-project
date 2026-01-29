-- Pipeline C: Staging Layer
-- stg_positions_daily.sql



select
    position_id,
    portfolio_id,
    security_id,
    position_date,
    cast(quantity as numeric(18, 2)) as quantity,
    cast(market_value_usd as numeric(18, 2)) as market_value_usd,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_positions_daily