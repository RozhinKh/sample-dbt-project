-- Pipeline C: Staging Layer
-- stg_portfolio_benchmarks.sql



select
    portfolio_id,
    benchmark_id,
    is_primary,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_portfolio_benchmarks