
  create or replace   view BAIN_ANALYTICS.DEV.int_position_performance_summary
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_position_performance_summary.sql
-- Purpose: Summary metrics for position performance



with pos_ret as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
)

select
    security_id,
    ticker,
    security_name,
    sector,
    count(*) as days_held,
    min(position_date) as first_position_date,
    max(position_date) as last_position_date,
    round(avg(daily_pnl), 2) as avg_daily_pnl,
    round(sum(daily_pnl), 2) as total_pnl,
    round(stddev_samp(daily_return_pct), 8) as daily_return_volatility,
    round(max(daily_return_pct), 8) as max_daily_return,
    round(min(daily_return_pct), 8) as min_daily_return
from pos_ret
group by security_id, ticker, security_name, sector
  );

