-- Pipeline A: Intermediate Layer
-- int_cashflow_aggregated.sql
-- Purpose: Pre-aggregate cashflows by portfolio, month, type (push aggregation upstream)
-- Models downstream: 1 (fact_portfolio_cashflow)



with cashflows as (
    select * from BAIN_ANALYTICS.DEV.stg_cashflows
),

-- Pre-aggregate at source to reduce rows before fact table join
monthly_aggregated as (
    select
        portfolio_id,
        date_trunc('month', cashflow_date) as cashflow_month,
        cashflow_type,
        currency,
        count(*) as transaction_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(amount) as min_amount,
        max(amount) as max_amount
    from cashflows
    group by portfolio_id, date_trunc('month', cashflow_date), cashflow_type, currency
)

select
    portfolio_id,
    cashflow_month,
    extract(year from cashflow_month) as cashflow_year,
    extract(month from cashflow_month) as cashflow_month_num,
    extract(quarter from cashflow_month) as cashflow_quarter,
    cashflow_type,
    currency,
    transaction_count,
    total_amount,
    avg_amount,
    min_amount,
    max_amount
from monthly_aggregated