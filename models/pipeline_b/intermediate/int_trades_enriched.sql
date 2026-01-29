-- Pipeline B: Intermediate Layer
-- int_trades_enriched.sql
-- Purpose: Enrich trades with security and broker details

{{ config(
    materialized='view',
    tags=['intermediate', 'pipeline_b'],
    meta={'pipeline': 'b', 'layer': 'intermediate'}
) }}

with trades as (
    select * from {{ ref('stg_trades') }}
),
securities as (
    select * from {{ ref('stg_securities') }}
),
brokers as (
    select * from {{ ref('stg_brokers') }}
)

select
    t.trade_id,
    t.portfolio_id,
    t.security_id,
    t.broker_id,
    t.trade_date,
    t.trade_type,
    t.quantity,
    t.price,
    t.commission,
    s.ticker,
    s.security_name,
    s.asset_class,
    s.sector,
    b.broker_name
from trades t
inner join securities s on t.security_id = s.security_id
inner join brokers b on t.broker_id = b.broker_id
