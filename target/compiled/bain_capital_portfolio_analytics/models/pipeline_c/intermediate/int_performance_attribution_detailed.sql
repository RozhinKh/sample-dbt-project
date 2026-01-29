-- Pipeline C: Intermediate Layer
-- int_performance_attribution_detailed.sql
-- Purpose: Detailed performance attribution across multiple dimensions



with pos_data as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
),

portfolio_metrics as (
    select
        portfolio_id,
        position_date,
        sum(daily_pnl) as total_portfolio_pnl,
        sum(market_value_usd * daily_return_pct) as weighted_pnl,
        sum(market_value_usd) as total_nav
    from pos_data
    group by 1, 2
)

select
    pd.position_id,
    pd.portfolio_id,
    pd.ticker,
    pd.security_name,
    pd.sector,
    pd.asset_class,
    pd.position_date,
    pd.quantity,
    pd.market_value_usd,
    pd.daily_pnl,
    pd.daily_return_pct,
    pm.total_portfolio_pnl,
    pm.total_nav,
    round(pd.market_value_usd / nullif(pm.total_nav, 0), 8) as position_weight,
    round(pd.daily_pnl / nullif(pm.total_portfolio_pnl, 0), 8) as pnl_contribution_pct,
    round(100 * pd.daily_pnl / nullif(pm.total_nav, 0), 4) as pnl_contribution_bps,
    round((pd.market_value_usd / nullif(pm.total_nav, 0)) * pd.daily_return_pct, 8) as weighted_return_contribution,
    case
        when pd.daily_pnl > 0 and pd.daily_return_pct > 0.005 then 'STRONG_GAIN'
        when pd.daily_pnl > 0 and pd.daily_return_pct > 0 then 'POSITIVE'
        when pd.daily_pnl > 0 then 'GAIN_LOW_RETURN'
        when pd.daily_pnl < 0 and pd.daily_return_pct < -0.005 then 'STRONG_LOSS'
        when pd.daily_pnl < 0 then 'LOSS'
        else 'NEUTRAL'
    end as performance_category,
    rank() over (
        partition by pd.portfolio_id, pd.position_date
        order by abs(pd.daily_pnl) desc
    ) as pnl_impact_rank,
    round(sum(abs(pd.daily_pnl)) over (
        partition by pd.portfolio_id, pd.position_date
        order by abs(pd.daily_pnl) desc
        rows between unbounded preceding and current row
    ) / nullif(sum(abs(pd.daily_pnl)) over (
        partition by pd.portfolio_id, pd.position_date
    ), 0), 8) as cumulative_pnl_contribution
from pos_data pd
join portfolio_metrics pm
    on pd.portfolio_id = pm.portfolio_id
    and pd.position_date = pm.position_date