
  create or replace   view BAIN_ANALYTICS.DEV.int_position_risk_decomposition
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_position_risk_decomposition.sql
-- Purpose: Break down portfolio risk by position, sector, and asset class with contribution analysis



with pos_returns as (
    select * from BAIN_ANALYTICS.DEV.int_position_returns
),

port_risk_metrics as (
    select
        portfolio_id,
        position_date,
        sum(market_value_usd) as total_portfolio_value,
        count(distinct position_id) as total_positions,
        count(distinct sector) as sector_count,
        count(distinct asset_class) as asset_class_count
    from pos_returns
    group by 1, 2
)

select
    pr.position_id,
    pr.portfolio_id,
    pr.security_id,
    pr.ticker,
    pr.position_date,
    pr.sector,
    pr.asset_class,
    pr.market_value_usd,
    prm.total_portfolio_value,
    round(pr.market_value_usd / nullif(prm.total_portfolio_value, 0), 8) as position_weight,
    round(100 * pr.market_value_usd / nullif(prm.total_portfolio_value, 0), 4) as position_weight_pct,
    pr.daily_pnl,
    pr.daily_return_pct,
    round(pr.daily_return_pct * pr.market_value_usd / nullif(prm.total_portfolio_value, 0), 8) as marginal_contribution_to_return,
    prm.total_positions,
    round(1.0 / nullif(prm.total_positions, 0), 8) as equal_weight_benchmark,
    round((pr.market_value_usd / nullif(prm.total_portfolio_value, 0)) - (1.0 / nullif(prm.total_positions, 0)), 8) as weight_vs_equal_weight,
    case
        when pr.market_value_usd / nullif(prm.total_portfolio_value, 0) > 0.10 then 'CONCENTRATED'
        when pr.market_value_usd / nullif(prm.total_portfolio_value, 0) > 0.05 then 'SIGNIFICANT'
        when pr.market_value_usd / nullif(prm.total_portfolio_value, 0) > 0.02 then 'MODERATE'
        else 'SMALL'
    end as position_size_category
from pos_returns pr
join port_risk_metrics prm
    on pr.portfolio_id = prm.portfolio_id
    and pr.position_date = prm.position_date
  );

