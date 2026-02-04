-- Pipeline A: Staging Layer
-- stg_cashflows.sql
-- Purpose: Clean and standardize cashflow transaction data
-- Models downstream: 1 (int_cashflow_aggregated)



select
    portfolio_id,
    cashflow_date,
    cashflow_type,
    amount,
    currency
from BAIN_ANALYTICS.DEV.sample_cashflows
where amount <> 0