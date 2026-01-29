-- Pipeline C: Report Layer
-- report_portfolio_risk_analysis.sql
-- Purpose: Comprehensive risk analysis and decomposition

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_c'],
    meta={'pipeline': 'c', 'layer': 'marts', 'table_type': 'report'}
) }}

select
    portfolio_id,
    valuation_date,
    nav_usd,
    daily_return_pct,
    volatility_90d_pct,
    avg_daily_return_90d_pct,
    max_daily_return_90d_pct,
    min_daily_return_90d_pct,
    positive_days_90d,
    negative_days_90d,
    win_rate_pct,
    sharpe_ratio_simple,
    return_range_90d,
    risk_classification
from {{ ref('int_portfolio_analysis_advanced') }}
