
  create or replace   view BAIN_ANALYTICS.DEV.int_trade_by_date
  
  
  
  
  as (
    -- Pipeline B: Intermediate Layer
-- int_trade_by_date.sql
-- Purpose: Daily trading volume and metrics



with trades as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
)

select
    trade_date,
    count(*) as daily_trade_count,
    count(distinct portfolio_id) as daily_portfolios,
    count(distinct security_id) as daily_securities,
    sum(quantity) as daily_quantity,
    sum(trade_value) as daily_volume,
    avg(price) as daily_avg_price,
    sum(case when trade_type = 'BUY' then 1 else 0 end) as buy_trades,
    sum(case when trade_type = 'SELL' then 1 else 0 end) as sell_trades
from trades
group by trade_date
  );

