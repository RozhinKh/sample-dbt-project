-- Pipeline C: Intermediate Layer
-- int_position_enriched.sql

{{ config(
    materialized='view',
    tags=['intermediate', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'intermediate'}
) }}

with positions as (
    select * from {{ ref('stg_positions_daily') }}
),
securities as (
    select * from {{ ref('stg_securities') }}
)

select
    p.position_id,
    p.portfolio_id,
    p.security_id,
    p.position_date,
    p.quantity,
    p.market_value_usd,
    s.ticker,
    s.security_name,
    s.asset_class,
    s.sector
from positions p
left join securities s on p.security_id = s.security_id
