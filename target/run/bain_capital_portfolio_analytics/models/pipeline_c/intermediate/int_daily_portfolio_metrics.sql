
  create or replace   view BAIN_ANALYTICS.DEV.int_daily_portfolio_metrics
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_daily_portfolio_metrics.sql
-- Purpose: Daily portfolio performance metrics



with pr as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
)

select
    portfolio_id,
    valuation_date,
    cast(nav_usd as numeric(18, 2)) as nav_usd,
    cast(daily_return_pct as numeric(18, 8)) as daily_return_pct,
    cast(daily_return_pct * nav_usd as numeric(18, 2)) as daily_return_usd,
    lead(nav_usd) over (partition by portfolio_id order by valuation_date) as next_day_nav,
    lag(nav_usd) over (partition by portfolio_id order by valuation_date) as prev_day_nav,
    count(*) over (partition by portfolio_id) as trading_days
from pr
  );

