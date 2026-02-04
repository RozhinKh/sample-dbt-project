-- Pipeline B: Intermediate Layer
-- int_trade_execution_analysis.sql
-- Purpose: Advanced trade execution analysis with market impact and execution costs



with trades_detail as (
    select * from BAIN_ANALYTICS.DEV.int_trade_metrics
),

with_window_aggregates as (
    select
        trade_id,
        trade_date,
        portfolio_id,
        security_id,
        ticker,
        quantity,
        price,
        price as execution_price,
        trade_value,
        commission,
        round(avg(price) over (partition by trade_date, security_id), 2) as daily_avg_price,
        min(price) over (partition by trade_date, security_id) as daily_min_price,
        max(price) over (partition by trade_date, security_id) as daily_max_price,
        (max(price) over (partition by trade_date, security_id) - min(price) over (partition by trade_date, security_id)) as daily_price_range,
        round(stddev_pop(price) over (partition by trade_date, security_id), 4) as daily_volatility,
        round((price - min(price) over (partition by trade_date, security_id)) / nullif((max(price) over (partition by trade_date, security_id) - min(price) over (partition by trade_date, security_id)), 0), 8) as execution_percentile
    from trades_detail
)

select
    trade_id,
    trade_date,
    portfolio_id,
    security_id,
    ticker,
    quantity,
    execution_price,
    trade_value,
    commission,
    daily_avg_price,
    daily_min_price,
    daily_max_price,
    daily_price_range,
    daily_volatility,
    execution_percentile,
    round(100 * (price - daily_avg_price) / nullif(daily_avg_price, 0), 4) as price_vs_avg_pct,
    case
        when execution_percentile > 0.75 then 'POOR'
        when execution_percentile > 0.50 then 'FAIR'
        when execution_percentile > 0.25 then 'GOOD'
        else 'EXCELLENT'
    end as execution_quality,
    round(commission / nullif(trade_value, 0), 8) as commission_ratio,
    round(100 * commission / nullif(trade_value, 0), 6) as commission_ratio_bps
from with_window_aggregates