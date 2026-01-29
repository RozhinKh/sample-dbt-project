-- Pipeline C: Staging Layer
-- stg_valuations.sql

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'staging'}
) }}

select
    valuation_id,
    portfolio_id,
    valuation_date,
    nav,
    nav_usd,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_valuations') }}
