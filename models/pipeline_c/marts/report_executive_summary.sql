-- Pipeline C: Executive Report
-- report_executive_summary.sql
-- Purpose: High-level portfolio performance and risk metrics for executive review

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_c', 'executive'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'report', 'audience': 'executive'}
) }}

with port_perf as (
    select * from {{ ref('int_portfolio_returns') }}
),

rolling_vol as (
    select * from {{ ref('int_rolling_volatility') }}
),

max_drawdown as (
    select
        portfolio_id,
        valuation_date,
        min(drawdown_pct_display) as max_drawdown_pct
    from {{ ref('int_portfolio_drawdown') }}
    group by 1, 2
)

select
    pp.portfolio_id,
    pp.valuation_date,
    round(pp.nav_usd, 2) as portfolio_nav_usd,
    round(100 * pp.daily_return_pct, 4) as daily_return_pct,
    round(100 * sum(pp.daily_return_pct) over (
        partition by pp.portfolio_id
        order by pp.valuation_date
        rows between 29 preceding and current row
    ), 4) as return_30d_pct,
    round(100 * sum(pp.daily_return_pct) over (
        partition by pp.portfolio_id
        order by pp.valuation_date
        rows between 89 preceding and current row
    ), 4) as return_90d_pct,
    round(rv.volatility_30d_pct, 4) as volatility_30d_pct,
    round(rv.volatility_90d_pct, 4) as volatility_90d_pct,
    md.max_drawdown_pct,
    case
        when rv.volatility_90d < 0.01 then 'VERY_LOW'
        when rv.volatility_90d < 0.02 then 'LOW'
        when rv.volatility_90d < 0.03 then 'MODERATE'
        else 'HIGH'
    end as risk_level,
    case
        when pp.daily_return_pct > 0.01 then 'STRONG'
        when pp.daily_return_pct > 0 then 'POSITIVE'
        when pp.daily_return_pct < -0.01 then 'WEAK'
        else 'NEUTRAL'
    end as daily_performance_trend
from port_perf pp
join rolling_vol rv
    on pp.portfolio_id = rv.portfolio_id
    and pp.valuation_date = rv.valuation_date
join max_drawdown md
    on pp.portfolio_id = md.portfolio_id
    and pp.valuation_date = md.valuation_date
