-- Pipeline A: Intermediate Layer
-- int_portfolio_attributes.sql
-- Purpose: Enrich portfolios with dimensional attributes
-- Models downstream: 1 (fact_portfolio_cashflow)



with portfolios as (
    select * from BAIN_ANALYTICS.DEV.stg_portfolios
)

select
    p.portfolio_id,
    p.portfolio_name,
    p.portfolio_type,
    p.fund_id,
    case
        when p.portfolio_type = 'EQUITY' then 1
        when p.portfolio_type = 'FIXED_INCOME' then 2
        when p.portfolio_type = 'BALANCED' then 3
        else 4
    end as portfolio_type_priority,
    p.dbt_loaded_at
from portfolios p