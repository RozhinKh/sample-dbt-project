-- Pipeline B: Fact Layer
-- fact_trades.sql
-- Purpose: Detailed trade fact table with all metrics

{{ config(
    materialized='view',
    tags=['marts', 'fact', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'marts', 'table_type': 'fact'}
) }}

select * from {{ ref('int_trade_metrics') }}
