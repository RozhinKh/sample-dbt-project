-- Pipeline C: Report Layer
-- report_sector_analysis.sql
-- Purpose: Sector-level performance and contribution analysis

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'report'}
) }}

select
    portfolio_id,
    position_date,
    sector,
    position_count,
    sector_value,
    total_portfolio_value,
    sector_weight_pct,
    sector_pnl,
    sector_return_pct,
    sector_contribution_to_portfolio,
    sector_pnl_contribution_pct
from {{ ref('int_sector_performance_attribution') }}
