-- Pipeline C: Intermediate Layer
-- int_portfolio_period_returns.sql
-- Purpose: Period-based portfolio return calculations



with pr as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
)

select
    portfolio_id,
    to_char(valuation_date, 'YYYY') as valuation_year,
    to_char(valuation_date, 'YYYY-MM') as valuation_month,
    count(*) as period_trading_days,
    round(sum(daily_return_pct), 8) as period_cumulative_return,
    round(avg(daily_return_pct), 8) as period_avg_daily_return,
    round(stddev_samp(daily_return_pct), 8) as period_volatility,
    round(max(nav_usd), 2) as period_max_nav,
    round(min(nav_usd), 2) as period_min_nav,
    round(first_value(nav_usd) over (partition by portfolio_id, to_char(valuation_date, 'YYYY-MM') order by valuation_date), 2) as period_starting_nav,
    round(last_value(nav_usd) over (partition by portfolio_id, to_char(valuation_date, 'YYYY-MM') order by valuation_date rows between unbounded preceding and unbounded following), 2) as period_ending_nav
from pr
group by portfolio_id, valuation_year, valuation_month