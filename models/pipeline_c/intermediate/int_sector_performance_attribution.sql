-- Pipeline C: Intermediate Layer
-- int_sector_performance_attribution.sql
-- Purpose: Analyze sector-level contribution to portfolio returns

{{ config(
    materialized='view',
    tags=['intermediate', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'intermediate'}
) }}

with pos_perf as (
    select * from {{ ref('int_position_returns') }}
),

sector_metrics as (
    select
        portfolio_id,
        position_date,
        sector,
        count(distinct position_id) as position_count,
        sum(market_value_usd) as sector_value,
        sum(daily_pnl) as sector_pnl,
        round(sum(market_value_usd * daily_return_pct) / nullif(sum(market_value_usd), 0), 8) as weighted_return_pct
    from pos_perf
    group by 1, 2, 3
),

port_totals as (
    select
        portfolio_id,
        position_date,
        sum(market_value_usd) as total_portfolio_value,
        sum(daily_pnl) as total_portfolio_pnl
    from pos_perf
    group by 1, 2
)

select
    sm.portfolio_id,
    sm.position_date,
    sm.sector,
    sm.position_count,
    sm.sector_value,
    pt.total_portfolio_value,
    round(sm.sector_value / nullif(pt.total_portfolio_value, 0), 8) as sector_weight,
    round(100 * sm.sector_value / nullif(pt.total_portfolio_value, 0), 4) as sector_weight_pct,
    sm.sector_pnl,
    pt.total_portfolio_pnl,
    sm.weighted_return_pct as sector_return_pct,
    round(sm.sector_pnl / nullif(pt.total_portfolio_value, 0), 8) as sector_contribution_to_portfolio,
    round(100 * sm.sector_pnl / nullif(pt.total_portfolio_pnl, 0), 4) as sector_pnl_contribution_pct
from sector_metrics sm
join port_totals pt
    on sm.portfolio_id = pt.portfolio_id
    and sm.position_date = pt.position_date
