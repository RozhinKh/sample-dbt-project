-- Pipeline C: Fact Layer
-- fact_portfolio_performance.sql
-- Purpose: Portfolio performance fact table - joins multiple intermediates

{{ config(
    materialized='table',
    tags=['marts', 'fact', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'fact'}
) }}

with returns as (
    select * from {{ ref('int_portfolio_returns') }}
),
risk as (
    select * from {{ ref('int_risk_metrics') }}
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
