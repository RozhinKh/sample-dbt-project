-- Pipeline B: Report Layer
-- report_trade_performance.sql
-- Purpose: Comprehensive trade execution and performance analysis

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'marts', 'table_type': 'report'}
) }}

select
    trade_id,
    trade_date,
    portfolio_id,
    security_id,
    ticker,
    quantity,
    execution_price,
    trade_value,
    commission,
    daily_avg_price,
    execution_percentile,
    price_vs_avg_pct,
    execution_quality,
    commission_ratio_bps
from {{ ref('int_trade_execution_analysis') }}
