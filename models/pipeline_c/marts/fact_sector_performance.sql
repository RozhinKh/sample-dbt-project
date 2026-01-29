-- Pipeline C: Fact Layer
-- fact_sector_performance.sql

{{ config(
    materialized='view',
    tags=['marts', 'fact', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'fact'}
) }}

select
    portfolio_id,
    position_date,
    sector,
    position_count,
    sector_value,
    total_quantity,
    sector_weight
from {{ ref('int_sector_allocation') }}
