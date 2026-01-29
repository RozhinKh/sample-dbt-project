-- Pipeline A: Staging Layer
-- stg_portfolios.sql
-- Purpose: Clean and standardize portfolio dimension data
-- Models downstream: 1 (int_portfolio_attributes)

{{ config(
    materialized='table',
    tags=['staging', 'pipeline_a'],
    meta={'pipeline': 'a', 'layer': 'staging'}
) }}

select
    portfolio_id,
    portfolio_name,
    portfolio_type,
    fund_id,
    status,
    current_timestamp() as dbt_loaded_at
from {{ source('raw', 'sample_portfolios') }}
where status = 'ACTIVE'
