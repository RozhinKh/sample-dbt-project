-- Pipeline C: Intermediate Layer
-- int_risk_metrics.sql

{{ config(
    materialized='view',
    tags=['intermediate', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'intermediate'}
) }}

with returns as (
    select * from {{ ref('int_portfolio_returns') }}
),

risk_calcs as (
    select
        portfolio_id,
        valuation_date,
        daily_return_pct,
        stddev(daily_return_pct) over (partition by portfolio_id order by valuation_date rows between 89 preceding and current row) as volatility_90d,
        stddev(daily_return_pct) over (partition by portfolio_id order by valuation_date rows between 364 preceding and current row) as volatility_1y,
        avg(daily_return_pct) over (partition by portfolio_id order by valuation_date rows between 89 preceding and current row) as avg_return_90d,
        min(daily_return_pct) over (partition by portfolio_id order by valuation_date rows between 364 preceding and current row) as max_drawdown
    from returns
)

select
    portfolio_id,
    valuation_date,
    daily_return_pct,
    volatility_90d,
    volatility_1y,
    avg_return_90d,
    max_drawdown,
    case when volatility_90d > 0 then avg_return_90d / volatility_90d else 0 end as sharpe_ratio
from risk_calcs
