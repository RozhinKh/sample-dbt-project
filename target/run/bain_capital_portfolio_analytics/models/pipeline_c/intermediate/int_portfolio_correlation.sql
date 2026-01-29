
  create or replace   view BAIN_ANALYTICS.DEV.int_portfolio_correlation
  
  
  
  
  as (
    -- Pipeline C: Intermediate Layer
-- int_portfolio_correlation.sql
-- Purpose: Analyze correlation between portfolios and sectors



with port_returns as (
    select
        portfolio_id,
        valuation_date,
        daily_return_pct,
        row_number() over (partition by portfolio_id order by valuation_date) as return_seq
    from BAIN_ANALYTICS.DEV.int_portfolio_returns
),

sector_returns as (
    select
        portfolio_id,
        position_date as valuation_date,
        sector,
        sum(market_value_usd * daily_return_pct) / nullif(sum(market_value_usd), 0) as sector_daily_return_pct
    from BAIN_ANALYTICS.DEV.int_position_returns
    group by 1, 2, 3
)

select
    pr.portfolio_id,
    pr.valuation_date,
    sr.sector,
    pr.daily_return_pct as portfolio_return_pct,
    sr.sector_daily_return_pct,
    covar_pop(pr.daily_return_pct, sr.sector_daily_return_pct) over (
        partition by pr.portfolio_id, sr.sector
        order by pr.valuation_date
        rows between 29 preceding and current row
    ) as covariance_30d,
    round(100 * covar_pop(pr.daily_return_pct, sr.sector_daily_return_pct) over (
        partition by pr.portfolio_id, sr.sector
        order by pr.valuation_date
        rows between 29 preceding and current row
    ), 6) as covariance_30d_pct
from port_returns pr
join sector_returns sr
    on pr.portfolio_id = sr.portfolio_id
    and pr.valuation_date = sr.valuation_date
  );

