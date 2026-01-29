-- Pipeline C: Report Layer
-- report_position_lifecycle.sql
-- Purpose: Position lifecycle stage analysis and transitions



select
    position_id,
    portfolio_id,
    security_id,
    ticker,
    security_name,
    asset_class,
    sector,
    position_date,
    quantity,
    market_value_usd,
    daily_return_pct,
    days_in_portfolio,
    position_duration_days,
    position_stage,
    lifecycle_stage
from BAIN_ANALYTICS.DEV.int_position_lifecycle