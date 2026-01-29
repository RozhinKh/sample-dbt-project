-- Pipeline C: Report Layer
-- report_top_performers.sql
-- Purpose: Top and bottom performing securities ranking



select
    position_id,
    portfolio_id,
    security_id,
    ticker,
    security_name,
    asset_class,
    sector,
    position_date,
    market_value_usd,
    daily_return_pct_display,
    return_20d_cumulative_pct,
    performance_tier
from BAIN_ANALYTICS.DEV.int_top_performers