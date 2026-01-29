-- Pipeline A: Staging Layer
-- stg_cashflows.sql
-- Purpose: Clean and standardize cashflow transaction data
-- Models downstream: 1 (int_cashflow_aggregated)



select
    concat(portfolio_id, '-', cashflow_date, '-', row_number() over (partition by portfolio_id, cashflow_date order by cashflow_id)) as cashflow_id,
    portfolio_id,
    cashflow_date,
    cashflow_type,
    amount,
    currency,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_cashflows
where amount <> 0