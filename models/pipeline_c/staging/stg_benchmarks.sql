-- Pipeline C: Staging Layer
-- stg_benchmarks.sql

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'staging'}
) }}

select
    benchmark_id,
    benchmark_name,
    benchmark_ticker,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_benchmarks') }}
