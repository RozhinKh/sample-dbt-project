-- Pipeline C: Fact Layer
-- fact_position_snapshot.sql



select
    position_id,
    portfolio_id,
    security_id,
    position_date,
    quantity,
    market_value_usd,
    ticker,
    security_name,
    asset_class,
    sector,
    daily_pnl,
    daily_return_pct
from BAIN_ANALYTICS.DEV.int_position_returns