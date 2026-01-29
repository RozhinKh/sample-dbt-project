-- Pipeline C: Report Layer
-- report_performance_drivers.sql
-- Purpose: Performance driver analysis and attribution by position and sector

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'report'}
) }}

select
    position_id,
    portfolio_id,
    ticker,
    security_name,
    sector,
    asset_class,
    position_date,
    market_value_usd,
    daily_pnl,
    daily_return_pct,
    position_weight,
    pnl_contribution_bps,
    weighted_return_contribution,
    performance_category,
    pnl_impact_rank,
    cumulative_pnl_contribution
from {{ ref('int_performance_attribution_detailed') }}
