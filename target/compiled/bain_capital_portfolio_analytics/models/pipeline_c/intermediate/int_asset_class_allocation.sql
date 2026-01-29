-- Pipeline C: Intermediate Layer
-- int_asset_class_allocation.sql
-- Purpose: Analyze asset class allocation and performance within portfolios



with pos_perf as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
),

asset_metrics as (
    select
        portfolio_id,
        position_date,
        asset_class,
        count(distinct position_id) as position_count,
        count(distinct security_id) as security_count,
        sum(market_value_usd) as asset_class_value,
        sum(daily_pnl) as asset_class_pnl,
        sum(quantity) as total_quantity,
        min(daily_return_pct) as min_return_pct,
        max(daily_return_pct) as max_return_pct,
        round(sum(market_value_usd * daily_return_pct) / nullif(sum(market_value_usd), 0), 8) as weighted_return_pct,
        round(stddev_pop(daily_return_pct), 8) as return_volatility
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
    am.portfolio_id,
    am.position_date,
    am.asset_class,
    am.position_count,
    am.security_count,
    am.asset_class_value,
    pt.total_portfolio_value,
    round(am.asset_class_value / nullif(pt.total_portfolio_value, 0), 8) as allocation_weight,
    round(100 * am.asset_class_value / nullif(pt.total_portfolio_value, 0), 4) as allocation_weight_pct,
    am.asset_class_pnl,
    pt.total_portfolio_pnl,
    am.total_quantity,
    am.weighted_return_pct,
    round(100 * am.weighted_return_pct, 4) as asset_class_return_pct,
    am.min_return_pct,
    am.max_return_pct,
    am.return_volatility,
    round(100 * am.return_volatility, 4) as return_volatility_pct,
    round(am.asset_class_pnl / nullif(pt.total_portfolio_value, 0), 8) as asset_class_contribution,
    case
        when am.allocation_weight > 0.30 then 'HIGH_CONCENTRATION'
        when am.allocation_weight > 0.10 then 'MODERATE'
        else 'LOW_CONCENTRATION'
    end as concentration_level
from asset_metrics am
join port_totals pt
    on am.portfolio_id = pt.portfolio_id
    and am.position_date = pt.position_date