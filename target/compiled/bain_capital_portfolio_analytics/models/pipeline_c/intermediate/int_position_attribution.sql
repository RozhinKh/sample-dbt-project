-- Pipeline C: Intermediate Layer
-- int_position_attribution.sql
-- Purpose: Analyze position-level contribution to portfolio performance



with pos_perf as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
),

port_nav as (
    select
        portfolio_id,
        position_date,
        sum(market_value_usd) as total_portfolio_value
    from pos_perf
    group by 1, 2
)

select
    pp.position_id,
    pp.portfolio_id,
    pp.security_id,
    pp.position_date,
    pp.ticker,
    pp.security_name,
    pp.asset_class,
    pp.sector,
    pp.market_value_usd,
    pn.total_portfolio_value,
    round(pp.market_value_usd / nullif(pn.total_portfolio_value, 0), 8) as position_weight,
    round(100 * pp.market_value_usd / nullif(pn.total_portfolio_value, 0), 4) as position_weight_pct,
    pp.daily_pnl,
    pp.daily_return_pct,
    round(pp.daily_pnl / nullif(pn.total_portfolio_value, 0), 8) as position_contribution_to_portfolio_pnl,
    round(100 * pp.daily_pnl / nullif(pn.total_portfolio_value, 0), 4) as position_contribution_bps
from pos_perf pp
join port_nav pn
    on pp.portfolio_id = pn.portfolio_id
    and pp.position_date = pn.position_date