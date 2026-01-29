-- Pipeline C: Staging Layer
-- stg_benchmark_returns.sql

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'staging'}
) }}

select
    benchmark_id,
    return_date,
    daily_return,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_benchmark_returns') }}
