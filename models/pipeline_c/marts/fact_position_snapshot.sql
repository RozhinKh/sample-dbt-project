-- Pipeline C: Fact Layer
-- fact_position_snapshot.sql

{{ config(
    materialized='view',
    tags=['marts', 'fact', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'fact'}
) }}

select
    position_id,
    portfolio_id,
    security_id,
    position_date,
    quantity,
    market_value_usd,
    ticker,
    security_name,
    asset_class,
    sector,
    daily_pnl,
    daily_return_pct
from {{ ref('int_position_returns') }}
