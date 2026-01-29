-- Pipeline B: Staging Layer
-- stg_securities.sql
-- Purpose: Clean security master data

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'staging'}
) }}

select
    security_id,
    ticker,
    security_name,
    asset_class,
    sector,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_securities') }}
