-- Pipeline B: Staging Layer
-- stg_trades.sql
-- Purpose: Clean and standardize trade transaction data

{{ config(
    materialized='view',
    tags=['staging', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'staging'}
) }}

select
    trade_id,
    portfolio_id,
    security_id,
    broker_id,
    trade_date,
    trade_type,
    cast(quantity as numeric(18, 2)) as quantity,
    cast(price as numeric(18, 2)) as price,
    cast(net_amount as numeric(18, 2)) as commission,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_trades') }}
where quantity > 0
