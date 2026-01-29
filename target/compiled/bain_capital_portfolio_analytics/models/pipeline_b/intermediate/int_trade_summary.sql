-- Pipeline B: Intermediate Layer
-- int_trade_summary.sql
-- Purpose: Summarize trades by security and date (for aggregation upstream)



with metrics as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
),

summary as (
    select
        portfolio_id,
        security_id,
        trade_date,
        ticker,
        security_name,
        asset_class,
        sector,
        broker_name,
        count(*) as trade_count,
        sum(quantity) as total_quantity,
        sum(trade_value) as total_trade_value,
        sum(commission) as total_commission,
        avg(price) as avg_price
    from metrics
    group by portfolio_id, security_id, trade_date, ticker, security_name, asset_class, sector, broker_name
)

select * from summary