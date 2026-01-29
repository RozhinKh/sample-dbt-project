-- Pipeline B: Intermediate Layer
-- int_trade_by_portfolio.sql
-- Purpose: Aggregate trading metrics by portfolio



with trades as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
)

select
    portfolio_id,
    count(*) as trade_count,
    sum(quantity) as total_quantity,
    sum(trade_value) as total_trade_volume,
    sum(commission) as total_commissions,
    avg(price) as avg_price,
    max(trade_value) as max_trade_value,
    min(trade_value) as min_trade_value,
    round(avg(commission), 2) as avg_commission
from trades
group by portfolio_id