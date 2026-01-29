-- Pipeline B: Intermediate Layer
-- int_trade_by_sector.sql
-- Purpose: Aggregate trading metrics by sector



with trades as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
)

select
    sector,
    count(*) as trade_count,
    count(distinct portfolio_id) as portfolio_count,
    sum(quantity) as total_quantity,
    sum(trade_value) as total_trade_volume,
    avg(price) as avg_price,
    sum(commission) as total_commissions,
    round(sum(trade_value) / nullif(sum(commission), 0), 2) as commission_ratio
from trades
group by sector