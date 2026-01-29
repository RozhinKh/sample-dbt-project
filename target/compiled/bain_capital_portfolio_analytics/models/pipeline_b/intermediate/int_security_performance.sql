-- Pipeline B: Intermediate Layer
-- int_security_performance.sql
-- Purpose: Per-security trading performance metrics



with trades as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
)

select
    security_id,
    ticker,
    security_name,
    asset_class,
    sector,
    count(*) as security_trade_count,
    count(distinct portfolio_id) as portfolios_traded,
    count(distinct broker_id) as brokers_used,
    sum(quantity) as total_quantity_traded,
    sum(trade_value) as total_trade_value,
    avg(price) as avg_trading_price,
    min(price) as min_trading_price,
    max(price) as max_trading_price,
    round(stddev_pop(price), 4) as price_volatility,
    sum(commission) as total_commissions
from trades
group by security_id, ticker, security_name, asset_class, sector