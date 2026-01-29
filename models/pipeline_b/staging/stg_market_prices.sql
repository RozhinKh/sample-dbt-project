-- Pipeline B: Staging Layer
-- stg_market_prices.sql
-- Purpose: Clean market price data

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'staging'}
) }}

select
    security_id,
    price_date,
    close_price,
    volume,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_market_prices') }}
