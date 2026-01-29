-- Pipeline B: Staging Layer
-- stg_brokers.sql
-- Purpose: Clean broker/counterparty data

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'staging'}
) }}

select
    broker_id,
    broker_name,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_brokers') }}
