-- Pipeline C: Report Layer
-- report_portfolio_analytics.sql



with perf as (
    select * from BAIN_ANALYTICS.DEV.fact_portfolio_performance
)

select
    portfolio_id,
    valuation_date,
    nav_usd,
    daily_return_pct,
    valuation_year,
    valuation_month,
    valuation_quarter,
    volatility_90d,
    sharpe_ratio,
    excess_return,
    case
        when daily_return_pct > 0 then 'POSITIVE'
        when daily_return_pct < 0 then 'NEGATIVE'
        else 'NEUTRAL'
    end as return_direction
from perf