-- Pipeline B: Intermediate Layer
-- int_trade_execution_analysis.sql
-- Purpose: Advanced trade execution analysis with market impact and execution costs

{{ config(
    materialized='view',
    tags=['intermediate', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'intermediate'}
) }}

with trades_detail as (
    select * from {{ ref('int_trade_metrics') }}
),

market_context as (
    select
        trade_date,
        security_id,
        avg(price) as daily_avg_price,
        min(price) as daily_min_price,
        max(price) as daily_max_price,
        max(price) - min(price) as daily_price_range,
        stddev_pop(price) as daily_volatility
    from trades_detail
    group by 1, 2
)

select
    td.trade_id,
    td.trade_date,
    td.portfolio_id,
    td.security_id,
    td.ticker,
    td.quantity,
    td.price as execution_price,
    td.trade_value,
    td.commission,
    mc.daily_avg_price,
    mc.daily_min_price,
    mc.daily_max_price,
    mc.daily_price_range,
    mc.daily_volatility,
    round((td.price - mc.daily_min_price) / nullif(mc.daily_price_range, 0), 8) as execution_percentile,
    round(100 * (td.price - mc.daily_avg_price) / nullif(mc.daily_avg_price, 0), 4) as price_vs_avg_pct,
    case
        when (td.price - mc.daily_min_price) / nullif(mc.daily_price_range, 0) > 0.75 then 'POOR'
        when (td.price - mc.daily_min_price) / nullif(mc.daily_price_range, 0) > 0.50 then 'FAIR'
        when (td.price - mc.daily_min_price) / nullif(mc.daily_price_range, 0) > 0.25 then 'GOOD'
        else 'EXCELLENT'
    end as execution_quality,
    round(td.commission / nullif(td.trade_value, 0), 8) as commission_ratio,
    round(100 * td.commission / nullif(td.trade_value, 0), 6) as commission_ratio_bps
from trades_detail td
join market_context mc
    on td.security_id = mc.security_id
    and td.trade_date = mc.trade_date
