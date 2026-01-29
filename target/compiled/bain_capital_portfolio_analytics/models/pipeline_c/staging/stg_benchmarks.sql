-- Pipeline C: Staging Layer
-- stg_benchmarks.sql



select
    benchmark_id,
    benchmark_name,
    benchmark_ticker,
    current_timestamp() as dbt_loaded_at
from BAIN_ANALYTICS.DEV.sample_benchmarks