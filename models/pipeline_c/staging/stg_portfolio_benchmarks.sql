-- Pipeline C: Staging Layer
-- stg_portfolio_benchmarks.sql

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'staging'}
) }}

select
    portfolio_id,
    benchmark_id,
    is_primary,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_portfolio_benchmarks') }}
