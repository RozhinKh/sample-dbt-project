-- Pipeline B: Report Layer
-- report_trading_performance.sql
-- Purpose: Trading performance summary by portfolio and security

{{ config(
    materialized='view',
    tags=['marts', 'report', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'marts', 'table_type': 'report'}
) }}

select * from {{ ref('int_trade_summary') }}
