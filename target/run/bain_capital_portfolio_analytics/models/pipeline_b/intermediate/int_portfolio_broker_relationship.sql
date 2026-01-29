
  create or replace   view BAIN_ANALYTICS.DEV.int_portfolio_broker_relationship
  
  
  
  
  as (
    -- Pipeline B: Intermediate Layer
-- int_portfolio_broker_relationship.sql
-- Purpose: Analyze portfolio-broker trading relationships



with trades as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
)

select
    portfolio_id,
    broker_id,
    broker_name,
    count(*) as trades_with_broker,
    sum(quantity) as total_quantity,
    sum(trade_value) as total_volume,
    sum(commission) as total_commissions,
    round(sum(commission) / nullif(sum(trade_value), 0) * 100, 4) as commission_rate_pct,
    min(trade_date) as first_trade_date,
    max(trade_date) as last_trade_date
from trades
group by portfolio_id, broker_id, broker_name
  );

