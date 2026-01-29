
  
    

create or replace transient table BAIN_ANALYTICS.DEV.fact_portfolio_performance
    
    
    
    as (-- Pipeline C: Fact Layer
-- fact_portfolio_performance.sql
-- Purpose: Portfolio performance fact table - joins multiple intermediates



with returns as (
    select * from BAIN_ANALYTICS.DEV.int_portfolio_returns
),
risk as (
    select * from BAIN_ANALYTICS.DEV.int_risk_metrics
)

select
    pr.portfolio_id,
    pr.valuation_date,
    pr.nav_usd,
    pr.daily_return_pct,
    pr.valuation_year,
    pr.valuation_month,
    pr.valuation_quarter,
    rm.volatility_90d,
    rm.volatility_1y,
    rm.sharpe_ratio,
    rm.max_drawdown
from returns pr
left join risk rm on pr.portfolio_id = rm.portfolio_id and pr.valuation_date = rm.valuation_date
    )
;


  