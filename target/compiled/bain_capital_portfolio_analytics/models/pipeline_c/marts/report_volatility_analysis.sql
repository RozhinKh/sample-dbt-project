-- Pipeline C: Report Layer
-- report_volatility_analysis.sql
-- Purpose: Rolling volatility and risk metric analysis



select
    portfolio_id,
    valuation_date,
    nav_usd,
    daily_return_pct,
    volatility_30d,
    volatility_90d,
    volatility_200d,
    volatility_30d_pct,
    volatility_90d_pct,
    volatility_200d_pct,
    avg_return_30d_pct,
    avg_return_90d_pct
from BAIN_ANALYTICS.DEV.int_rolling_volatility