-- Pipeline C: Report Layer
-- report_position_attribution.sql
-- Purpose: Position-level contribution analysis report

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'report'}
) }}

select
    position_id,
    portfolio_id,
    security_id,
    position_date,
    ticker,
    security_name,
    asset_class,
    sector,
    market_value_usd,
    total_portfolio_value,
    position_weight_pct,
    daily_pnl,
    daily_return_pct,
    position_contribution_bps
from {{ ref('int_position_attribution') }}
