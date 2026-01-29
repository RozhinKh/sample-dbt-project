
  create or replace   view BAIN_ANALYTICS.DEV.int_trade_by_broker
  
  
  
  
  as (
    -- Pipeline B: Intermediate Layer
-- int_trade_by_broker.sql
-- Purpose: Aggregate trading metrics by broker



with trades as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
)

select
    broker_id,
    broker_name,
    count(*) as trade_count,
    sum(quantity) as total_quantity,
    sum(trade_value) as total_trade_volume,
    avg(price) as avg_price,
    min(price) as min_price,
    max(price) as max_price,
    sum(commission) as total_commissions
from trades
group by broker_id, broker_name
  );

